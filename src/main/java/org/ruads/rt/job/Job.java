package org.ruads.rt.job;

import java.util.Objects;
import java.util.UUID;

public abstract class Job {
    private final int priority;
    private final int sla;
    private final String name;
    private final String id;

    public Job(String name, int sla, int priority) {
        this.priority = priority;
        this.sla = sla;
        this.name = name;
        this.id = UUID.randomUUID().toString();
    }

    public abstract Job process() throws Exception;

    public final int getPriority() {
        return priority;
    }

    public final int getSla() {
        return sla;
    }

    public final String getName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Job job = (Job) o;
        return Objects.equals(id, job.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
