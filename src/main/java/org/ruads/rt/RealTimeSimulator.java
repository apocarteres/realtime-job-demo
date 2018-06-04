package org.ruads.rt;

import org.ruads.rt.executor.SequentialExecutor;
import org.ruads.rt.job.Downloading;
import org.ruads.rt.job.Job;

import java.util.PriorityQueue;

import static java.lang.Integer.compare;

public final class RealTimeSimulator {

    public static void main(String[] args) throws Exception {
        PriorityQueue<Job> queue = new PriorityQueue<>((j1, j2) -> compare(j2.getPriority(), j1.getPriority()));
        queue.add(new Downloading(100, 1, "http://files.ruads.org/pbs_mixdown_lq.mp3"));
        queue.add(new Downloading(60000, 0, "http://files.ruads.org/fallen_mixdown.mp3"));

        new SequentialExecutor().execute(queue);
//        new ParallelExecutor().execute(queue);
    }
}
