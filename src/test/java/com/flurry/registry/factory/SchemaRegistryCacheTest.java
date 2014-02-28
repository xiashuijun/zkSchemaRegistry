package com.flurry.registry.factory;

import com.flurry.registry.ZKTestUtil;
import com.flurry.registry.domain.*;
import com.flurry.registry.service.SchemaReaper;
import com.flurry.registry.service.SchemaRegistryService;
import com.flurry.registry.service.ZNodeSequenceService;
import com.flurry.registry.util.DelayedRetry;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.locks.Reaper;
import com.sun.tools.javac.util.Pair;
import org.apache.zookeeper.ZKUtil;
import org.junit.*;

import java.util.Iterator;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;

public class SchemaRegistryCacheTest {

  private static final DelayedRetry RETRY = new DelayedRetry(10, 1, 1);

  private static ZKTestUtil testUtil;
  private CuratorFramework client;

  @BeforeClass
  public static void setUpClass() throws Exception {
    testUtil = new ZKTestUtil(3);
  }

  @Before
  public void setUp() throws Exception {
    client = testUtil.getRandomClient();
  }

  @After
  public void tearDown() throws Exception {
    ZKUtil.deleteRecursive(client.getZookeeperClient().getZooKeeper(), RegistrationType.PROCESS.getBasePath());
    ZKUtil.deleteRecursive(client.getZookeeperClient().getZooKeeper(), RegistrationType.SCHEMA.getBasePath());
    ZKUtil.deleteRecursive(client.getZookeeperClient().getZooKeeper(), "/tmp");
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    testUtil.stopAll();
  }

  @Test
  public void testWeDoNotDeleteSchemaRightAway() throws Exception {
    CuratorFramework mainClient = testUtil.getRandomClient();
    SchemaRegistryService service = createServiceAndSync(mainClient);
    SchemaReaper reaper = makeReaper(mainClient);
    try {
      SchemaRegistryCache schemaRegistryCache = new SchemaRegistryCache(mainClient, service, reaper);
      schemaRegistryCache.start(new SchemaVersion("schemaA"), new ProcessIdentifier("a"), 1, RETRY);
      Pair<CuratorFramework, SchemaProcessZNodes> result = registerSchemaProcess(new SchemaVersion("schemaB"), new ProcessIdentifier("b"), 100000);
      testUtil.killClientConnection(result.fst);
      assertEquals(Lists.newArrayList("schemaB", "schemaA"), mainClient.getChildren().forPath(RegistrationType.SCHEMA.getBasePath()));
    } finally {
      reaper.close();
    }
  }

  @Test
  public void testReaperRemovesSchemaAfterExpiration() throws Exception {
    CuratorFramework mainClient = testUtil.getRandomClient();
    SchemaRegistryService service = createServiceAndSync(mainClient);
    SchemaReaper reaper = makeReaper(mainClient);
    try {
      SchemaRegistryCache schemaRegistryCache = new SchemaRegistryCache(mainClient, service, reaper);
      schemaRegistryCache.start(new SchemaVersion("schemaA"), new ProcessIdentifier("a"), 1, RETRY);
      Pair<CuratorFramework, SchemaProcessZNodes> result = registerSchemaProcess(new SchemaVersion("schemaB"), new ProcessIdentifier("b"), 1);
      testUtil.killClientConnection(result.fst);
      Thread.sleep(5);
      assertEquals("schemaA", Iterables.getOnlyElement(mainClient.getChildren().forPath(RegistrationType.SCHEMA.getBasePath())));
    } finally {
      reaper.close();
    }
  }

  @Test
  public void testWeWaitForAllSchemasToReloadCachesBeforeWeStart() throws Exception {
    CuratorFramework someClient = testUtil.getRandomClient();
    SchemaRegistryService someClientsService = createServiceAndSync(someClient);
    SchemaVersion schemaVersion = new SchemaVersion("someClientSchema");
    ProcessIdentifier identifier = new ProcessIdentifier("someIdentifier");
    SchemaProcessZNodes registeredSchema = someClientsService.registerSchema(schemaVersion, identifier, 1);

    CuratorFramework mainClient = testUtil.getRandomClient();
    final CountDownLatch latch = new CountDownLatch(1);
    SchemaReaper reaper = makeReaper(someClient);
    try {
      final SchemaRegistryCache schemaRegistryCache = new SchemaRegistryCache(mainClient, someClientsService, reaper) {
        @Override
        public void start(SchemaVersion schemaVersion, ProcessIdentifier identifier, long ttl, DelayedRetry retry) throws Exception {
          super.start(schemaVersion, identifier, ttl, retry);
          latch.countDown();
        }
      };

      final SchemaVersion schemaTwo = new SchemaVersion("schemaTwo");
      new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            schemaRegistryCache.start(schemaTwo, new ProcessIdentifier("mainProcess"), 1, RETRY);
          } catch (Exception e) {
            throw Throwables.propagate(e);
          }
        }
      }).start();

      // main process hasn't started yet
      assertEquals(1, latch.getCount());
      // assert that the process knows nothing about the new started process

      // register the main process
      someClientsService.mutateMyActiveSchemas(schemaTwo, registeredSchema.getProcessIDZNode(), ModificationType.ADD);
      latch.await();

      TreeSet<SequenceZNode> sequenceService = new ZNodeSequenceService(mainClient).getSortedChildren(RegistrationType.PROCESS.getBasePath());
      assertEquals(2, sequenceService.size());
      Iterator<SequenceZNode> iterator = sequenceService.iterator();
      assertEquals(Sets.newHashSet(schemaVersion, schemaTwo), someClientsService.getSchemaVersions(iterator.next()));
      assertEquals(Sets.newHashSet(schemaVersion, schemaTwo), someClientsService.getSchemaVersions(iterator.next()));
    } finally {
      reaper.close();
    }
  }

  @Test
  public void testWhenProcessDiesWithLastSchemaItIsNotActive() throws Exception {
    CuratorFramework someClient = testUtil.getRandomClient();
    SchemaRegistryService someClientsService = createServiceAndSync(someClient);
    SchemaVersion schemaVersion = new SchemaVersion("someClientSchema");
    ProcessIdentifier identifier = new ProcessIdentifier("someIdentifier");
    SchemaProcessZNodes registeredSchema = someClientsService.registerSchema(schemaVersion, identifier, 1);

    CuratorFramework mainClient = testUtil.getRandomClient();
    final CountDownLatch latch = new CountDownLatch(1);
    SchemaReaper reaper = makeReaper(mainClient);
    try {
      final SchemaRegistryCache schemaRegistryCache = new SchemaRegistryCache(mainClient, someClientsService, reaper) {
        @Override
        public void start(SchemaVersion schemaVersion, ProcessIdentifier identifier, long ttl, DelayedRetry retry) throws Exception {
          super.start(schemaVersion, identifier, ttl, retry);
          latch.countDown();
        }
      };

      final SchemaVersion schemaTwo = new SchemaVersion("schemaTwo");
      new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            schemaRegistryCache.start(schemaTwo, new ProcessIdentifier("mainProcess"), 1, RETRY);
          } catch (Exception e) {
            throw Throwables.propagate(e);
          }
        }
      }).start();

      // main process hasn't started yet
      assertEquals(1, latch.getCount());
      // assert that the process knows nothing about the new started process

      // register the main process
      someClientsService.mutateMyActiveSchemas(schemaTwo, registeredSchema.getProcessIDZNode(), ModificationType.ADD);

      latch.await();
      TreeSet<SequenceZNode> sequenceService = new ZNodeSequenceService(mainClient).getSortedChildren(RegistrationType.PROCESS.getBasePath());
      assertEquals(2, sequenceService.size());
      Iterator<SequenceZNode> iterator = sequenceService.iterator();
      assertEquals(Sets.newHashSet(schemaVersion, schemaTwo), someClientsService.getSchemaVersions(iterator.next()));
      assertEquals(Sets.newHashSet(schemaVersion, schemaTwo), someClientsService.getSchemaVersions(iterator.next()));
    } finally {
      reaper.close();
    }
  }

  private Pair<CuratorFramework, SchemaProcessZNodes> registerSchemaProcess(SchemaVersion version, ProcessIdentifier identifier, long ttl) throws Exception {
    CuratorFramework someClient = testUtil.getRandomClient();
    SchemaRegistryService service = createServiceAndSync(someClient);
    SchemaProcessZNodes result = service.registerSchema(version, identifier, ttl);
    return Pair.of(someClient, result);
  }

  private SchemaReaper makeReaper(CuratorFramework mainClient) {
    return new SchemaReaper(mainClient, RegistrationType.SCHEMA.getBasePath(), Reaper.Mode.REAP_INDEFINITELY, 1, new ProcessIdentifier("bleh"));
  }

  private SchemaRegistryService createServiceAndSync(CuratorFramework mainClient) {
    SchemaRegistryService service = new SchemaRegistryService(mainClient);
    service.sync(RegistrationType.PROCESS);
    service.sync(RegistrationType.SCHEMA);
    return service;
  }
}
