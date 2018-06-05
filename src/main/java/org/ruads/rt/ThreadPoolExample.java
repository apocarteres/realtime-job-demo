package org.ruads.rt;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Collections.emptySet;
import static java.util.concurrent.Executors.newFixedThreadPool;

//1. implement custom thread pool to support priorities
//2. SLA is not accurate
//3. on-demand cancellation won't work as expected

public final class ThreadPoolExample {

  private static final int PAYLOAD_SIZE = 100_000;
  private static final int SIMULTANEOUS_JOBS = 4;
  private static final int SIMULTANEOUS_REQUESTS = 100;
  private static final int JOB_BUCKET_SIZE = 2048;
  private static final int SLA = 1000;

  public static void main(String[] args) throws ExecutionException, InterruptedException, TimeoutException {
    System.out.println("generating payload...");
    int[] integers = IntStream.range(0, PAYLOAD_SIZE).toArray();
    ExecutorService jobService = newFixedThreadPool(SIMULTANEOUS_JOBS);
    BlockingQueue<Runnable> x = new PriorityBlockingQueue<>();
    ExecutorService requestService = new ThreadPoolExecutor(
      SIMULTANEOUS_REQUESTS,
      SIMULTANEOUS_REQUESTS,
      1, TimeUnit.HOURS,
      x);
    try {
      long startTime = System.currentTimeMillis();
      System.out.println(format("generating %d requests...", SIMULTANEOUS_REQUESTS));
      List<Future> futures = generateRequests(integers, jobService, requestService);
      System.out.println("waiting for responses...");
      for (Future future : futures) {
        future.get();
      }
      long duration = System.currentTimeMillis() - startTime;
      System.out.println(format("all job processed. total time: %d", duration));
    } finally {
      jobService.shutdown();
      requestService.shutdown();
    }
  }

  private static List<Future> generateRequests(int[] integers, ExecutorService jobService, ExecutorService emitService) {
    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < SIMULTANEOUS_REQUESTS; i++) {
      int p = i;
      long startTime = System.currentTimeMillis();
      futures.add(emitService.submit(() -> {
        try {
          String name = format("request_%d", p);
          Set<String> response = executeRequest(name, integers, jobService, SLA, startTime, p);
          long elapsedTime = System.currentTimeMillis() - startTime;
          System.out.println(format("%s: took %d, results size: %d", name, elapsedTime, response.size()));
          validate(response);
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
          throw new RuntimeException(e);
        }
      }));
    }
    return futures;
  }

  private static Set<String> executeRequest(String name, int[] payload, ExecutorService service, int sla, long startTime, int priority) throws ExecutionException, InterruptedException, TimeoutException {
    Set<String> result = new HashSet<>(PAYLOAD_SIZE);
    System.out.println(format("%s: distributing jobs across buckets of size %d", name, JOB_BUCKET_SIZE));
    Set<Future<Set<String>>> futures = new HashSet<>();
    for (int i = 0; i < payload.length; i += JOB_BUCKET_SIZE) {
      futures.add(service.submit(new IntToStr(payload, i, min(JOB_BUCKET_SIZE, payload.length - i), priority)));
      if (futures.size() == SIMULTANEOUS_JOBS) {
        for (Future<Set<String>> future : futures) {
          result.addAll(future.get(sla, TimeUnit.MILLISECONDS));
        }
        futures.clear();
      }
      //if something left in queue
      for (Future<Set<String>> future : futures) {
        result.addAll(future.get(sla, TimeUnit.MILLISECONDS));
      }
      long slaCheck = System.currentTimeMillis() - startTime;
      if (slaCheck > sla) {
        System.out.println(format("WARN: SLA is violated for %s. SLA was %d but job spent %d", name, sla, slaCheck));
        return emptySet();
      }
    }
    return result;
  }

  private static final class IntToStr implements Callable<Set<String>>, Comparable<IntToStr> {
    private final int[] data;
    private final int offset;
    private final int size;
    private final int priority;

    private IntToStr(int[] data, int offset, int size, int priority) {
      this.data = data;
      this.offset = offset;
      this.size = size;
      this.priority = priority;
    }

    @Override
    public Set<String> call() {
      Set<String> result = new HashSet<>();
      for (int i = 0; i < size; i++) {
        result.add(String.valueOf(data[offset + i]));
      }
      return result;
    }

    @Override
    public int compareTo(IntToStr that) {
      return Integer.compare(this.priority, that.priority);
    }
  }

  private static void validate(Set<String> data) {
    if (data.isEmpty()) {
      System.out.println("ERROR: no data");
      return;
    }
    for (int i = 0; i < PAYLOAD_SIZE; i++) {
      if (!data.contains(String.valueOf(i))) {
        throw new RuntimeException(format("validation failed on: %d", i));
      }
    }
  }

}
