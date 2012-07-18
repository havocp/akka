/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2003-2011, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */

package scala.concurrent

import java.util.concurrent.{ Executor, ExecutorService }

/** This is just a temporary shim hack, move to ExecutionContext.batching in the actual scala.concurrent */
object AkkaExecutionContext {
  /**
   * Decorate an ExecutionContext with a wrapper context
   * which groups multiple nested `Runnable.run()` calls
   * into a single Runnable passed to the original
   * ExecutionContext. This can be a useful optimization
   * because it bypasses the original context's task
   * queue and keeps related (nested) code on a single
   * thread which may improve CPU affinity. However,
   * if tasks passed to the ExecutionContext are blocking
   * or expensive, this optimization can prevent work-stealing
   * and make performance worse. Also, some ExecutionContext
   * may be fast enough natively that this optimization just
   * adds overhead.
   * The default ExecutionContext.global is already batching
   * or fast enough not to benefit from it; while
   * `fromExecutor` and `fromExecutorService` do NOT add
   * this optimization since they don't know whether the underlying
   * executor will benefit from it.
   * A batching executor can create deadlocks if code does
   * not use `scala.concurrent.blocking` when it should,
   * because tasks created within other tasks will block
   * on the outer task completing.
   */
  def batching(context: ExecutionContext): ExecutionContextExecutor =
    impl.BatchingExecutionContext(context)

  /**
   * Like `scala.concurrent.ExecutionContext.batching` but
   * decorates a `java.util.concurrent.ExecutorService` rather than
   * a `scala.concurrent.ExecutionContext`. The `reporter` parameter
   * is used to implement the `scala.concurrent.ExecutionContext.reportFailure`
   * method.
   */
  def batchingExecutorService(service: ExecutorService, reporter: Throwable ⇒ Unit): ExecutionContextExecutorService =
    impl.BatchingExecutorService(service, reporter)

  /**
   * Like `scala.concurrent.ExecutionContext.batching` but
   * decorates a `java.util.concurrent.ExecutorService`
   * rather than a `scala.concurrent.ExecutionContext`.
   * To implement `scala.concurrent.ExecutionContext.reportFailure`, if
   * the provided `java.util.concurrent.ExecutorService` already implements
   * `scala.concurrent.ExecutionContext` then the result will delegate to it;
   * otherwise, `scala.concurrent.ExecutionContext.defaultReporter` will be used.
   */
  def batchingExecutorService(service: ExecutorService): ExecutionContextExecutorService =
    impl.BatchingExecutorService(service)
}
