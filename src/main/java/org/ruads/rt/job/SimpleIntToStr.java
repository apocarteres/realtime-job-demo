package org.ruads.rt.job;

import java.util.HashSet;
import java.util.Set;

import static java.lang.String.format;
import static java.lang.Thread.sleep;

public final class SimpleIntToStr extends Job{

  private final int[] data;
  private final Set<String> output;
  private int p;

  public SimpleIntToStr(String name, int sla, int[] data, int priority) {
    super(name, sla, priority);
    this.data = data;
    this.output = new HashSet<>();
  }

  public Set<String> getOutput() {
    return output;
  }

  @Override
  public Job process() {
    output.add(String.valueOf(data[p]));
    ++p;
    if (p == data.length) {
      return null;
    }
    return this;
  }
}
