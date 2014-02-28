package com.flurry.registry.factory;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.locks.InterProcessMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public abstract class LockFactory<T> {

  private static final Logger LOG = LoggerFactory.getLogger(LockFactory.class);
  private static final long DEFAULT_TTL = 5000;
  protected final CuratorFramework client;

  protected LockFactory(CuratorFramework client) {
    this.client = client;
  }

  public T executeWithLock() throws Exception {
    InterProcessMutex mutex = new InterProcessMutex(client, getLockPath());
    boolean hasLock = mutex.acquire(getTimeout(), TimeUnit.MILLISECONDS);
    try {
      while (!hasLock) {
        hasLock = mutex.acquire(getTimeout(), TimeUnit.MILLISECONDS);
        LOG.info("Waiting to acquire the schema lock someone else is holding it");
      }
      return runCommand();
    } finally {
      mutex.release();
    }
  }

  public abstract T runCommand();

  public abstract String getLockPath();

  public long getTimeout() {
    return DEFAULT_TTL;
  }


}
