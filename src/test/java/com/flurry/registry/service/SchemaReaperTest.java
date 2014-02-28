package com.flurry.registry.service;

import com.flurry.registry.ZKTestUtil;
import com.flurry.registry.domain.ProcessIdentifier;
import com.flurry.registry.domain.RegistrationType;
import com.flurry.registry.domain.SchemaVersion;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.locks.Reaper;
import com.netflix.curator.utils.EnsurePath;
import com.netflix.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.junit.AfterClass;
import org.junit.Test;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class SchemaReaperTest {

  private static final ZKTestUtil testUtil = new ZKTestUtil(3);

  @AfterClass
  public static void tearDown() throws Exception {
    testUtil.stopAll();
  }

  @Test
  public void testReapingHasNoEffectIfTTLIsNotSet() throws Exception {
    CuratorFramework client = testUtil.getRandomClient();
    SchemaVersion version = new SchemaVersion("schemaA");
    String path = ZKPaths.makePath(RegistrationType.SCHEMA.getBasePath(), version.getId());
    client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(ZKPaths.makePath(path, "someProcess"));
    final CountDownLatch latch = new CountDownLatch(1);
    SchemaReaper reaper = new SchemaReaper(client, path, Reaper.Mode.REAP_INDEFINITELY, 1, new ProcessIdentifier("someProcess")) {
      @Override
      public void doWork() {
        super.doWork();
        latch.countDown();
      }
    };
    reaper.start();
    latch.await();
    assertEquals(version.getId(), Iterables.getOnlyElement(client.getChildren().forPath(RegistrationType.SCHEMA.getBasePath()))); // make sure path still exists
  }

  @Test
  public void testDefaultDataForAZNode() throws Exception {
    CuratorFramework client = testUtil.getRandomClient();
    String result = client.create().creatingParentsIfNeeded().forPath("/foo");
    assertTrue(Arrays.equals(client.getData().forPath(result), InetAddress.getLocalHost().getHostAddress().getBytes()));
  }

  @Test
  public void testOnlyOneClientGetsTheReaper() throws Exception {
    CuratorFramework mainClient = testUtil.getRandomClient();
    for (RegistrationType type : RegistrationType.values()) {
      new EnsurePath(type.getBasePath()).ensure(mainClient.getZookeeperClient());
    }

    CountDownLatch globalLatch = new CountDownLatch(1);
    AtomicInteger leaderCount = new AtomicInteger(0);
    CuratorFramework clientOne = testUtil.getRandomClient();
    CuratorFramework clientTwo = testUtil.getRandomClient();
    CuratorFramework clientThree = testUtil.getRandomClient();

    SchemaReaperId reaperOne = createAndStartReaper(clientOne, globalLatch, leaderCount, "one");
    SchemaReaperId reaperTwo = createAndStartReaper(clientTwo, globalLatch, leaderCount, "two");
    SchemaReaperId reaperThree = createAndStartReaper(clientThree, globalLatch, leaderCount, "three");

    globalLatch.await();
    assertEquals(1, leaderCount.get());

    // find the reaper that has won the leader latch
    Map<SchemaReaperId, CuratorFramework> reapers = Maps.newHashMap(ImmutableMap.of(reaperOne, clientOne, reaperTwo, clientTwo, reaperThree, clientThree));

    // kill that reapers connection
    String originalId = killLeaderReaperAndGetId(reapers);
    assertEquals(2, leaderCount.get());

    // we should have a new leader
    String newId = killLeaderReaperAndGetId(reapers);
    assertNotEquals(originalId, newId);
    assertEquals(3, leaderCount.get());

    String lastId = getLeader(reapers).getId();
    // should not equal either of the leaders
    assertNotEquals(lastId, originalId);
    assertNotEquals(lastId, newId);
  }

  private String killLeaderReaperAndGetId(Map<SchemaReaperId, CuratorFramework> reapers) throws Exception {
    SchemaReaperId leaderReaper = getLeader(reapers);
    testUtil.killClientConnection(reapers.get(leaderReaper));
    reapers.remove(leaderReaper);
    leaderReaper.getLeaderElectionLatch().await();
    return leaderReaper.getId();
  }

  private SchemaReaperId getLeader(Map<SchemaReaperId, CuratorFramework> reapers) {
    return Iterables.find(reapers.keySet(), new Predicate<SchemaReaperId>() {
      @Override
      public boolean apply(SchemaReaperId input) {
        return input.getReaper().isLeader();
      }
    });
  }

  private SchemaReaperId createAndStartReaper(CuratorFramework client, final CountDownLatch latch, final AtomicInteger leaderCount, String id) throws Exception {
    final CountDownLatch leaderElectionLatch = new CountDownLatch(1);
    SchemaReaper reaper = new SchemaReaper(client, RegistrationType.SCHEMA.getBasePath(), Reaper.Mode.REAP_INDEFINITELY, Integer.MAX_VALUE, new ProcessIdentifier(id)) {
      @Override
      public void doWork() {
        super.doWork();
        latch.countDown();
        leaderElectionLatch.countDown();
        leaderCount.incrementAndGet();
      }
    };
    startInBackgroundThread(reaper);
    return new SchemaReaperId(id, reaper, leaderElectionLatch);
  }

  private void startInBackgroundThread(final SchemaReaper reaper) {
    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          reaper.start();
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    }).start();
  }

  private class SchemaReaperId {
    private final String id;
    private final SchemaReaper reaper;
    private final CountDownLatch leaderElectionLatch;

    private SchemaReaperId(String id, SchemaReaper reaper, CountDownLatch leaderElectionLatch) {
      this.id = id;
      this.reaper = reaper;
      this.leaderElectionLatch = leaderElectionLatch;
    }

    public String getId() {
      return id;
    }

    public SchemaReaper getReaper() {
      return reaper;
    }

    public CountDownLatch getLeaderElectionLatch() {
      return leaderElectionLatch;
    }
  }

}
