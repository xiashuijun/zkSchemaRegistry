package com.flurry.registry.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class DelayedRetry {

  private static final Logger LOG = LoggerFactory.getLogger(DelayedRetry.class);
  private final Random random = new Random();
  private final int maxRetries;
  private final int baseSleepTimeMs;
  private final int maxSleepTimeMs;

  public DelayedRetry(int maxRetries, int baseSleepTimeMs, int maxSleepTimeMs) {
    this.maxRetries = maxRetries;
    this.baseSleepTimeMs = baseSleepTimeMs;
    this.maxSleepTimeMs = maxSleepTimeMs;
  }

  public boolean allowRetry(int retryCount) {
    if (retryCount < maxRetries) {
      try {
        Thread.sleep(getSleepTimeMs(retryCount));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
      return true;
    }
    return false;

  }

  protected int getSleepTimeMs(int retryCount) {
    int sleepMs = baseSleepTimeMs * Math.max(1, random.nextInt(1 << (retryCount + 1)));
    if (sleepMs > maxSleepTimeMs) {
      LOG.warn(String.format("Sleep extension too large (%d). Pinning to %d", sleepMs, maxSleepTimeMs));
      sleepMs = maxSleepTimeMs;
    }
    return Math.min(maxSleepTimeMs, sleepMs);
  }


}
