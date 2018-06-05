package org.ruads.rt;

/**
 * Requirements:
 *
 * 1. Support SLA. Enqueued or ongoing job should be immediately canceled when SLA is violated.
 * 2. State reporting. Send back job status to client on state update.
 * 3. Comprehensive logging. Log job metrics in advance (what is running and how much time it took).
 * 4. Fine-grained control over resources. Be sure that app both not stuck and not starving.
 * 5. On-demand cancellation. Client should be able to cancel ongoing or enqueued job immediately.
 * 6. Good parallelism. Utilize allocated CPU's as more as possible.
 * 7. Take control over memory consumption. Make memory consumption predictable, provide guarantee not run out of memory at any point.
 */
public final class App {
}
