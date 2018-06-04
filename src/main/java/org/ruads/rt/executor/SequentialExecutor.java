package org.ruads.rt.executor;

import org.ruads.rt.job.Job;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;

import static java.lang.String.format;

public final class SequentialExecutor {
    public void execute(Queue<Job> queue) throws Exception {
        Job job;
        Map<Job, Long> jobStartTime = new HashMap<>();
        while ((job = queue.poll()) != null) {
            Job result = job.process();
            if (result != null) {
                queue.add(job);
            }
            //ensures that enqueued jobs are still relevant, if not
            //ejects them asap.
            ejectExpiredJobs(jobStartTime, queue);
        }
    }

    private static void ejectExpiredJobs(Map<Job, Long> map, Queue<Job> queue) {
        Iterator<Job> iterator = queue.iterator();
        while (iterator.hasNext()) {
            Job job = iterator.next();
            Long startedAt = map.computeIfAbsent(job, j -> System.currentTimeMillis());
            if (System.currentTimeMillis() - startedAt > job.getSla()) {
                System.out.println(format("SLA violation for %s! Ejecting job...", job.getName()));
                map.remove(job);
                iterator.remove();
            }
        }
    }
}

