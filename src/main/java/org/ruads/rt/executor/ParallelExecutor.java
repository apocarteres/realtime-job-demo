package org.ruads.rt.executor;

import org.ruads.rt.job.Job;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public final class ParallelExecutor {
        private static final int MAX_SIMULTANEOUS_JOBS = 5;

        void execute(Queue<Job> queue) throws Exception {
            ExecutorService pool = Executors.newFixedThreadPool(MAX_SIMULTANEOUS_JOBS);
            while (!queue.isEmpty()) {
                Collection<Job> jobs = new ArrayList<>(MAX_SIMULTANEOUS_JOBS);
                Job job;
                while ((job = queue.poll()) != null) {
                    jobs.add(job);
                    if (jobs.size() == MAX_SIMULTANEOUS_JOBS) {
                        break;
                    }
                }
                List<Callable<Job>> callableList = jobs.stream().map(j -> {
                    try {
                        return (Callable<Job>) j::process;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }).collect(Collectors.toList());
                List<Future<Job>> futures = pool.invokeAll(callableList);
                for (Future<Job> future : futures) {
                    Job result = future.get();
                    if (result != null) {
                        queue.add(result);
                    }
                }
            }
            pool.shutdown();
        }
    }
