package org.ruads.rt;

import org.ruads.rt.executor.ParallelExecutor;
import org.ruads.rt.job.SimpleIntToStr;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import static java.lang.String.format;

@SuppressWarnings("ConstantConditions")
public final class RealTimeSimulator {

  private static final int PAYLOAD_SIZE = 100_000;
  private static final int SIMULTANEOUS_JOBS = 4;
  private static final int SIMULTANEOUS_REQUESTS = 100;
  private static final int SLA = 1000;
  private static final boolean ENABLE_PRIORITIZATION = true;

  // request [5] |- - - - - ...                                                    ... - - -|
  //          request [7]    | - - - -  ...                 ... - - - - - - - - - - -|
  //              request [4]                                                                 |- - - - - - -|
  //                   request [55]       | - - - - - - - |

  public static void main(String[] args) {
    int[] ints = IntStream.range(0, PAYLOAD_SIZE).toArray();
    ParallelExecutor executor = new ParallelExecutor(SIMULTANEOUS_JOBS);
    executor.start();
    System.out.println(format("submitting %d requests", SIMULTANEOUS_REQUESTS));
    List<SimpleIntToStr> jobs = new ArrayList<>();
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < SIMULTANEOUS_REQUESTS; i++) {
      jobs.add(new SimpleIntToStr(format("request_%d", i), SLA, ints, ENABLE_PRIORITIZATION ? i : 0));
      executor.submit(jobs.get(i));
    }
    executor.waitForJobs();
    for (SimpleIntToStr job : jobs) {
      validate(job.getOutput());
    }
    long duration = System.currentTimeMillis() - startTime;
    System.out.println(format("all job processed. total time: %d", duration));
    executor.shutdown();
  }

  private static void validate(Set<String> data) {
    if (data.isEmpty()) {
      return;
    }
    for (int i = 0; i < PAYLOAD_SIZE; i++) {
      if (!data.contains(String.valueOf(i))) {
        System.out.println(format("FATAL: validation failed on: %d", i));
        return;
      }
    }
  }

}
