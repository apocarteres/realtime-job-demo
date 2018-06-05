package org.ruads.rt.executor;

import org.ruads.rt.job.Job;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Integer.compare;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newFixedThreadPool;

public final class ParallelExecutor {
  private final int simultaneousJobs;
  private final PriorityQueue<Job> queue;
  private final Map<Job, Long> startTimeMap;
  private final Map<Integer, Set<AtomicBoolean>> priorityLocks;
  private final Object mutex;
  private final AtomicBoolean shutdown;
  private final Semaphore jobsSemaphore;
  private final ExecutorService pool;
  private final AtomicInteger pending;


  public ParallelExecutor(int simultaneousJobs) {
    this.simultaneousJobs = simultaneousJobs;
    this.queue = new PriorityQueue<>((j1, j2) -> compare(j2.getPriority(), j1.getPriority()));
    this.mutex = new Object();
    this.shutdown = new AtomicBoolean();
    this.pool = newFixedThreadPool(simultaneousJobs);
    this.pending = new AtomicInteger();
    this.jobsSemaphore = new Semaphore(1);
    this.startTimeMap = new HashMap<>();
    this.priorityLocks = new HashMap<>();
  }

  public void waitForJobs() {
    System.out.println("executor is waiting for job done");
    try {
      jobsSemaphore.acquire();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public void shutdown() {
    shutdown.set(true);
    synchronized (mutex) {
      mutex.notifyAll();
    }
    System.out.println("shutting down executor. done");
    pool.shutdown();
    System.out.println("shutting down executor. halt");
  }

  public void submit(Job job) {
    if (shutdown.get()) {
      throw new RuntimeException("can't submit. executor is going to shutdown soon...");
    }
    pending.incrementAndGet();
    synchronized (mutex) {
      startTimeMap.put(job, System.currentTimeMillis());
      if (pending.get() >= simultaneousJobs) {
        Set<Integer> old = new HashSet<>();
        for (Map.Entry<Integer, Set<AtomicBoolean>> entry : priorityLocks.entrySet()) {
          if (entry.getKey() < job.getPriority()) {
            Set<AtomicBoolean> value = entry.getValue();
            Iterator<AtomicBoolean> iterator = value.iterator();
            AtomicBoolean next = iterator.next();
            iterator.remove();
            if (!iterator.hasNext()) {
              old.add(entry.getKey());
            }
            next.set(false);
            break;
          }
        }
        for (Integer integer : old) {
          priorityLocks.remove(integer);
        }
      }
      queue.add(job);
      mutex.notifyAll();
    }
  }

  public void start() {
    System.out.println("starting executor...");
    try {
      this.jobsSemaphore.acquire();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    System.out.println("executor started. awaiting jobs to process");
    for (int i = 0; i < simultaneousJobs; i++) {
      System.out.println(format("spawning worker %d", i));
      pool.submit(() -> {
        try {
          while (!shutdown.get()) {
            Job job;
            long startTime;
            AtomicBoolean lock = new AtomicBoolean(true);
            synchronized (mutex) {
              if (pending.get() == 0) {
                mutex.wait();
              }
              job = queue.poll();
              if (job == null) {
                continue;
              }
              startTime = startTimeMap.get(job);
              Set<AtomicBoolean> locks = priorityLocks.getOrDefault(job.getPriority(), new HashSet<>());
              locks.add(lock);
              if (locks.size() == 1) {
                priorityLocks.put(job.getPriority(), locks);
              }
            }
            Job result = job;
            long duration = 0;
            boolean sla = false;
            while (lock.get() && (result = job.process()) != null) {
              duration = System.currentTimeMillis() - startTime;
              if (duration >= result.getSla()) {
                sla = true;
                break;
              }
            }
            if (!sla) {
              if (result != null) {
                if (!lock.get()) {
                  System.out.println(format("lock was released. returning back job %s to the queue", job.getName()));
                  synchronized (mutex) {
                    queue.add(result);
                  }
                  continue;
                }
              } else {
                System.out.println(format(
                  "%s: job %s is done. took %d ms", Thread.currentThread().getName(), job.getName(), duration
                ));
              }
            } else {
              System.out.println(format(
                "%s: SLA violated for job %s. spent %d instead of %d",
                Thread.currentThread().getName(), job.getName(), duration, job.getSla()
              ));
            }
            synchronized (mutex) {
              startTimeMap.remove(job);
            }
            if (pending.decrementAndGet() == 0) {
              System.out.println("no more pending jobs");
              jobsSemaphore.release(1);
            }
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      });
    }
  }

}
