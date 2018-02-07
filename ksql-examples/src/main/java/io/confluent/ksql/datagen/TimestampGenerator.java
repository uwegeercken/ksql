package io.confluent.ksql.datagen;

import java.util.Random;

public class TimestampGenerator {
  private long next;
  private long burstCount;
  private long increment;
  private long burst;
  private Random random;

  TimestampGenerator(long start, long increment, long burst) {
    this.next = start;
    this.increment = increment;
    this.burst = burst;
    this.burstCount = 0;
    this.random = new Random();
  }

  public long next() {
    if (burstCount < burst) {
      burstCount += 1;
      return next;
    }
    burstCount = 0;
    next = next + (long)(increment * random.nextDouble());
    return next();
  }
}
