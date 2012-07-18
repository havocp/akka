/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2003-2011, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */

package scala.concurrent.impl

import java.util.Collection
import java.util.concurrent.{ Callable, ExecutorService, TimeUnit }
import scala.concurrent.util.Duration
import scala.concurrent.{ Awaitable, BlockContext, ExecutionContext, ExecutionContextExecutor, ExecutionContextExecutorService, CanAwait }
import scala.util.control.NonFatal

/**
 * An ExecutionContext which attempts to keep related Runnable batched on the
 * same thread, which may give better performance by 1) avoiding dispatch
 * through the ExecutionContext's queue and 2) creating a simple
 * "CPU affinity" for a related chain of tasks.
 */
private class BatchingExecutionContext(val delegate: ExecutionContext) extends ExecutionContextExecutor {

  // invariant: if "_tasksLocal.get ne null" then we are inside
  // BatchingRunnable.run; if it is null, we are outside
  private val _tasksLocal = new ThreadLocal[List[Runnable]]()

  // only valid to call if _tasksLocal.get ne null
  private def push(runnable: Runnable): Unit =
    _tasksLocal.set(runnable :: _tasksLocal.get)

  // only valid to call if _tasksLocal.get ne null
  private def nonEmpty(): Boolean =
    _tasksLocal.get.nonEmpty

  // only valid to call if _tasksLocal.get ne null
  private def pop(): Runnable = {
    val tasks = _tasksLocal.get
    _tasksLocal.set(tasks.tail)
    tasks.head
  }

  private class BatchingBlockContext(previous: BlockContext) extends BlockContext {

    override def internalBlockingCall[T](awaitable: Awaitable[T], atMost: Duration): T = {
      // if we know there will be blocking, we don't want to
      // keep tasks queued up because it could deadlock.
      _tasksLocal.get match {
        case null ⇒
        // not inside a BatchingRunnable
        case Nil  ⇒
        // inside a BatchingRunnable, but nothing is queued up
        case list ⇒ {
          // inside a BatchingRunnable and there's a queue;
          // make a new BatchingRunnable and send it to
          // another thread
          _tasksLocal set Nil
          delegate.execute(new BatchingRunnable(list))
        }
      }

      // now delegate the blocking to the previous BC
      previous.internalBlockingCall(awaitable, atMost)
    }
  }

  // ONLY BatchingRunnable should be sent directly
  // to delegate.execute()
  private class BatchingRunnable(val initial: List[Runnable]) extends Runnable {
    // this method runs in the delegate ExecutionContext's thread
    override def run(): Unit = {
      require(_tasksLocal.get eq null)

      val bc = new BatchingBlockContext(BlockContext.current)
      BlockContext.withBlockContext(bc) {
        try {
          _tasksLocal set initial
          while (nonEmpty) {
            val next = pop()
            try next.run() catch { case NonFatal(e) ⇒ reportFailure(e) }
          }
        } finally {
          _tasksLocal.remove()
          require(_tasksLocal.get eq null)
        }
      }
    }
  }

  override def execute(runnable: Runnable): Unit = {
    _tasksLocal.get match {
      case null ⇒
        // outside BatchingRunnable.run: start a new batch
        delegate.execute(new BatchingRunnable(runnable :: Nil))
      case _ ⇒
        // inside BatchingRunnable.run: add to existing batch, existing BatchingRunnable will run it
        push(runnable)
    }
  }

  def reportFailure(t: Throwable): Unit = delegate.reportFailure(t)
}

private[concurrent] object BatchingExecutionContext {
  def apply(delegate: ExecutionContext): ExecutionContextExecutor = delegate match {
    case already: BatchingExecutionContext ⇒ already // avoid double-wrap
    case _                                 ⇒ new BatchingExecutionContext(delegate)
  }
}

private class BatchingExecutorService(override val delegate: ExecutionContext with ExecutorService)
  extends BatchingExecutionContext(delegate) with ExecutionContextExecutorService {
  override def shutdown() { delegate.shutdown() }
  override def shutdownNow() = delegate.shutdownNow()
  override def isShutdown = delegate.isShutdown
  override def isTerminated = delegate.isTerminated
  override def awaitTermination(l: Long, timeUnit: TimeUnit) = delegate.awaitTermination(l, timeUnit)
  override def submit[T](callable: Callable[T]) = delegate.submit(callable)
  override def submit[T](runnable: Runnable, t: T) = delegate.submit(runnable, t)
  override def submit(runnable: Runnable) = delegate.submit(runnable)
  override def invokeAll[T](callables: Collection[_ <: Callable[T]]) = delegate.invokeAll(callables)
  override def invokeAll[T](callables: Collection[_ <: Callable[T]], l: Long, timeUnit: TimeUnit) = delegate.invokeAll(callables, l, timeUnit)
  override def invokeAny[T](callables: Collection[_ <: Callable[T]]) = delegate.invokeAny(callables)
  override def invokeAny[T](callables: Collection[_ <: Callable[T]], l: Long, timeUnit: TimeUnit) = delegate.invokeAny(callables, l, timeUnit)
}

private[concurrent] object BatchingExecutorService {
  def apply(delegate: ExecutorService, reporter: Throwable ⇒ Unit): ExecutionContextExecutorService =
    new BatchingExecutorService(ExecutionContext.fromExecutorService(delegate, reporter))

  def apply(delegate: ExecutorService): ExecutionContextExecutorService = delegate match {
    case already: BatchingExecutorService ⇒
      already
    case ec: ExecutionContext with ExecutorService ⇒
      new BatchingExecutorService(ec)
    case _ ⇒
      new BatchingExecutorService(ExecutionContext.fromExecutorService(delegate, ExecutionContext.defaultReporter))
  }
}
