package io.confluent.ksql.datagen;

public class TokenBucket {
  private int capacity;
  private int rate;
  private int level;
  private Object lock;
  private long lastFill;

  public TokenBucket(int capacity, int rate) {
    this.rate = rate;
    this.capacity = capacity;
    this.lock = new Object();
    level = capacity;
    lastFill = System.currentTimeMillis();
  }

  public int take(int quantity) throws InterruptedException {
    while (true) {
      long now = System.currentTimeMillis();
      long nextFill;

      synchronized (this.lock) {
        int elapsed = (int) ((now - lastFill) / 1000);
        if (elapsed > 0) {
          this.level += elapsed * this.rate;
          this.level = Math.min(this.level, this.capacity);
          this.lastFill = this.lastFill + elapsed * 1000;
        }
        if (this.level > 0) {
          quantity = Math.min(quantity, this.level);
          this.level -= quantity;
          return quantity;
        }

        nextFill = this.lastFill + 1000;
      }

      now = System.currentTimeMillis();
      if (now < nextFill) {
        Thread.sleep(nextFill - now);
      }
    }
  }
}
