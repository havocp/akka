
/**
 *  Copyright (C) 2009-2011 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.dispatch

import java.util.concurrent._
import akka.actor._

/**
 * An object which expires at some point, but is likely
 * to be completed well before that point, so we can often
 * stop tracking the expiration early. Such as a Future.
 * This is done as a trait (rather than a dedicated task
 * object as in ScheduledExecutorService) to minimize
 * overhead.
 */
private[akka] trait Expirable {
  /**
   * Returns the time remaining in nanoseconds. If zero or negative, then
   * the expirable can consider itself expired, and check will never be
   * called again. If positive, the check will be called an undefined number
   * of times (but at least once) during the time remaining.
   *
   * This must run quickly, since we don't update currentTimeNanos
   * as we iterate over a bunch of expirables.
   *
   * This method doubles as notification; if an expirable decides
   * it has <= 0 remaining, it has to self-expire.
   */
  def checkExpired(currentTimeNanos: Long): Long
}

/**
 * A service that tracks and expires expirables.
 * It tries to minimize overhead by bunching
 * up expirables with a nearby expiration time,
 * leaving the expiration time a little imprecise.
 * Trying to avoid adding a bunch of
 * tasks to the Scheduler. This also avoids creating
 * the FutureTask objects the scheduler creates and
 * otherwise avoids the scheduler's overhead.
 * Basically we want to make the common case where
 * a future is quickly completed really fast.
 *
 * ExpirationService doesn't care about errors lower than
 * its resolution. Larger errors are still
 * possible due to OS thread scheduling and OS timer resolution
 * and so forth, but we don't even try for errors below
 * the requested resolution. Too-low resolution will lead
 * to CPU and memory cost.
 */
private[akka] class ExpirationService(resolutionInNanos: Long = TimeUnit.MILLISECONDS.toNanos(50)) {

  // queue of expirables that we batch up to schedule all at once. In
  // particular, in the hopefully common case that many futures are
  // completed within a few milliseconds, we would schedule a single
  // timer for all of them.
  private val batchQueue = new ConcurrentLinkedQueue[Expirable]

  def add(expirable: Expirable) = {
    batchQueue.add(expirable)
    scheduleBatch
  }

  def addMany(expirables: Iterable[Expirable]) = {
    for (e ← expirables)
      batchQueue.add(e) // FIXME addAll faster probably
    scheduleBatch
  }

  private var batchScheduled = false

  private def scheduleBatch {
    // could use AtomicReferenceFieldUpdater perhaps for extra speed?
    // FIXME ideally this is very fast, so we should do something without
    // the explicit synchronized.
    synchronized {
      if (!batchScheduled) {
        Scheduler.scheduleOnce(clearBatch, resolutionInNanos, TimeUnit.NANOSECONDS)
        batchScheduled = true
      }
    }
  }

  private val clearBatch = new Runnable {
    override def run() = {
      synchronized {
        batchScheduled = false
        // after this point another clearBatch could
        // schedule and run, which means two of them
        // might drain the queue at once in theory,
        // but that should be fine even if it happens
        // since it's a concurrent queue.
      }

      var buckets = Map.empty[Long, List[Expirable]]

      var e = batchQueue.poll
      val now = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis) // get this once for the whole batch
      while (e != null) {
        val remaining = e.checkExpired(now)
        if (remaining > 0) {
          // bucket the expirables by rounded-off expiration time, be careful
          // to round up so we don't get a time of 0
          val bucket = (remaining / resolutionInNanos) + resolutionInNanos
          //assert(bucket > 0)
          //assert((bucket % resolutionInNanos) == 0)
          buckets.get(bucket) match {
            case Some(old) ⇒
              buckets += (bucket -> (e :: old))
            case None ⇒
              buckets += (bucket -> (e :: Nil))
          }
        } else {
          // nothing more to do, the expirable is expired.
        }
        e = batchQueue.poll
      }

      for ((remaining, expirables) ← buckets) {
        // for each bucket, re-add the expirables in the bucket.
        // note that this leads to again grouping expirables within
        // another initial completion window, which may put tasks
        // together that were originally in a different window.
        Scheduler.scheduleOnce({ () ⇒
          addMany(expirables)
        }, remaining, TimeUnit.NANOSECONDS)
      }
    }
  }
}
