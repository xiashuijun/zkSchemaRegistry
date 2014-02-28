package com.flurry.registry.service;

import com.flurry.registry.domain.ProcessIdentifier;
import com.flurry.registry.domain.SchemaTTL;
import com.flurry.registry.serializer.SchemaTTLSerializer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.leader.LeaderLatch;
import com.netflix.curator.framework.recipes.locks.Reaper;
import com.netflix.curator.utils.EnsurePath;
import com.netflix.curator.utils.ThreadUtils;
import com.netflix.curator.utils.ZKPaths;
import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class SchemaReaper implements Closeable {

  static final int DEFAULT_REAPING_THRESHOLD_MS = (int) TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES);
  private static final DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
  private static final Logger log = Logger.getLogger(SchemaReaper.class);
  private final byte[] defaultDataValue;
  private final LeaderLatch leaderLatch;
  private final Reaper reaper;
  private final AtomicReference<State> state = new AtomicReference<State>(State.LATENT);
  private final CuratorFramework client;
  private final String path;
  private final Reaper.Mode mode;
  private final ScheduledExecutorService executor;
  private final int reapingThresholdMs;
  private volatile ScheduledFuture<?> task;

  private enum State {
    LATENT,
    STARTED,
    CLOSED
  }

  /**
   * @param client the client
   * @param path   path to reap children from
   * @param mode   reaping mode
   */
  public SchemaReaper(CuratorFramework client, String path, Reaper.Mode mode, ProcessIdentifier identifier) {
    this(client, path, mode, newExecutorService(), DEFAULT_REAPING_THRESHOLD_MS, identifier);
  }

  /**
   * @param client             the client
   * @param path               path to reap children from
   * @param reapingThresholdMs threshold in milliseconds that determines that a path can be deleted
   * @param mode               reaping mode
   */
  public SchemaReaper(CuratorFramework client, String path, Reaper.Mode mode, int reapingThresholdMs, ProcessIdentifier identifier) {
    this(client, path, mode, newExecutorService(), reapingThresholdMs, identifier);
  }

  /**
   * @param client             the client
   * @param path               path to reap children from
   * @param executor           executor to use for background tasks
   * @param reapingThresholdMs threshold in milliseconds that determines that a path can be deleted
   * @param mode               reaping mode
   */
  public SchemaReaper(CuratorFramework client, String path, Reaper.Mode mode, ScheduledExecutorService executor, int reapingThresholdMs, ProcessIdentifier identifier) {
    this.client = client;
    this.path = path;
    this.mode = mode;
    this.executor = executor;
    this.reapingThresholdMs = reapingThresholdMs;
    this.reaper = new Reaper(client, executor, reapingThresholdMs);
    this.leaderLatch = new LeaderLatch(client, makeDefaultLeaderLatchPath(client, path), identifier.getId());
    this.defaultDataValue = getDefaultDataContents();
  }

  /**
   * The reaper must be started
   *
   * @throws Exception errors
   */
  public void start() throws Exception {
    Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "Cannot be started more than once");
    this.leaderLatch.start();
    log.info(leaderLatch.getId() + " : is the leader for SchemaReaper");
    task = executor.scheduleWithFixedDelay
            (
                    new Runnable() {
                      @Override
                      public void run() {
                        doWork();
                      }
                    },
                    0,
                    reapingThresholdMs,
                    TimeUnit.MILLISECONDS
            );

    reaper.start();
  }

  @Override
  public void close() throws IOException {
    this.leaderLatch.close();
    if (state.compareAndSet(State.STARTED, State.CLOSED)) {
      Closeables.closeQuietly(reaper);
      task.cancel(true);
    }
  }

  @VisibleForTesting boolean isLeader() {
    return leaderLatch.hasLeadership();
  }

  private static ScheduledExecutorService newExecutorService() {
    return ThreadUtils.newFixedThreadScheduledPool(2, "ChildReaper");
  }

  public void doWork() {
    try {
      this.leaderLatch.await();
      List<String> children = client.getChildren().forPath(path);
      for (String name : children) {
        String thisPath = ZKPaths.makePath(path, name);
        Stat stat = client.checkExists().forPath(thisPath);
        if ((stat != null) && (stat.getNumChildren() == 0)) {
          byte[] bytes = client.getData().forPath(thisPath);
          // Sigh Netflix,
          // the default data used by curator is
          // InetAddress.getLocalHost().getHostAddress().getBytes() for a ZNode
          if (Arrays.equals(bytes, defaultDataValue)) {
            continue;
          }
          SchemaTTL schemaTTL = SchemaTTLSerializer.deSerialize(bytes);
          if (!schemaTTL.isTimestamp()) {
            schemaTTL.setAsTimestamp();
            log.info("Setting expiration for " + thisPath + " to expire: " + dateFormat.format(schemaTTL.getExpirationTime()));
            client.setData().forPath(thisPath, SchemaTTLSerializer.serialize(schemaTTL));
          }
          if (schemaTTL.isExpired()) {
            reaper.addPath(thisPath, mode);
            log.info("Reaping expired path: " + thisPath);
          }
        }
      }
    } catch (Exception e) {
      log.error("Could not get children for path: " + path, e);
    }
  }

  private static String makeDefaultLeaderLatchPath(CuratorFramework client, String path) {
    try {
      new EnsurePath("/tmp/reaperLeader").ensure(client.getZookeeperClient());
      return ZKPaths.makePath("/tmp/reaperLeader", path);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private byte[] getDefaultDataContents()  {
    try {
      return InetAddress.getLocalHost().getHostAddress().getBytes();
    } catch (UnknownHostException e) {
      throw Throwables.propagate(e);
    }
  }
}