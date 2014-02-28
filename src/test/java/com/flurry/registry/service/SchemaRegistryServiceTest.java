package com.flurry.registry.service;

import com.flurry.registry.ZKTestUtil;
import com.flurry.registry.domain.*;
import com.flurry.registry.serializer.SchemaTTLSerializer;
import com.flurry.registry.serializer.SchemaVersionsSerializer;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.transaction.CuratorTransactionResult;
import com.netflix.curator.utils.ZKPaths;
import com.sun.tools.javac.util.Pair;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZKUtil;
import org.junit.*;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static junit.framework.Assert.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class SchemaRegistryServiceTest {

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
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    testUtil.stopAll();
  }

  @Test
  public void testParsingProcessZNodes() throws Exception {
    ProcessIdentifier identifier = new ProcessIdentifier("1");
    SchemaVersion schemaVersion = new SchemaVersion("onlySchema");
    Set<SchemaVersion> schemas = Sets.newHashSet(schemaVersion);
    String result = client
            .create()
            .creatingParentsIfNeeded()
            .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
            .forPath(ZKPaths.makePath(RegistrationType.PROCESS.getBasePath(), SequenceZNode.makeZNodePrefixPath(identifier)), SchemaVersionsSerializer.serialize(schemas));
    SequenceZNode idZNode = SequenceZNode.parse(result);
    assertEquals(Long.valueOf(0), idZNode.getSequenceIdAsLong());
    assertEquals("1-" + String.format(Locale.ENGLISH, "%010d", 0), idZNode.getNodeName());
    assertEquals(schemas, new SchemaRegistryService(client).getActiveSchemas(schemaVersion));
  }

  @Test
  public void testRegisteringSchema() throws Exception {
    SchemaRegistryService service = new SchemaRegistryService(client);
    SchemaVersion version1 = new SchemaVersion("version1");
    ProcessIdentifier id1 = new ProcessIdentifier("id1");

    SequenceZNode processIDNode = service.registerSchema(version1, id1, 1).getProcessIDZNode();
    ZKTestUtil.printDirectoryTree(client, RegistrationType.SCHEMA.getBasePath());
    ZKTestUtil.printDirectoryTree(client, RegistrationType.PROCESS.getBasePath());
    assertEquals(Long.valueOf(0), processIDNode.getSequenceIdAsLong()); // should be the first process node

    // verify that the RegistrationType.SCHEMA.getBasePath()/version1/id1 exists
    assertEquals(version1.getId(), Iterables.getOnlyElement(client.getChildren().forPath(RegistrationType.SCHEMA.getBasePath())));

    // verify that the RegistrationType.PROCESS.getBasePath()/id1 exits
    assertEquals(processIDNode.getNodeName(), Iterables.getOnlyElement(client.getChildren().forPath(RegistrationType.PROCESS.getBasePath())));

    // verify that it knows about its own schema
    Set<SchemaVersion> schemaVersions = SchemaVersionsSerializer.deSerialize(client.getData().forPath(ZKPaths.makePath(RegistrationType.PROCESS.getBasePath(), processIDNode.getNodeName())));
    assertEquals(version1.getId(), Iterables.getOnlyElement(schemaVersions).getId());
  }

  @Test
  public void testGettingAlreadyActiveProcesses() throws Exception {
    SchemaRegistryService service = new SchemaRegistryService(client);
    SequenceZNode processOne = service.registerSchema(new SchemaVersion("version1"), new ProcessIdentifier("id1"), 1).getProcessIDZNode();
    SequenceZNode processTwo = service.registerSchema(new SchemaVersion("version2"), new ProcessIdentifier("id2"), 1).getProcessIDZNode();
    SequenceZNode processThree = service.registerSchema(new SchemaVersion("version3"), new ProcessIdentifier("id3"), 1).getProcessIDZNode();
    assertEquals(Lists.newArrayList(processOne), Lists.newArrayList(service.getActiveSchemasBeforeInclusive(processOne)));
    assertEquals(Lists.newArrayList(processOne, processTwo), Lists.newArrayList(service.getActiveSchemasBeforeInclusive(processTwo)));
    assertEquals(Lists.newArrayList(processOne, processTwo, processThree), Lists.newArrayList(service.getActiveSchemasBeforeInclusive(processThree)));
  }

  @Test
  public void testHasSchema() throws Exception {
    SchemaRegistryService service = new SchemaRegistryService(client);
    SchemaVersion version1 = new SchemaVersion("version1");
    SchemaVersion version2 = new SchemaVersion("version2");
    SequenceZNode processOne = service.registerSchema(version1, new ProcessIdentifier("id1"), 1).getProcessIDZNode();
    SequenceZNode processTwo = service.registerSchema(version2, new ProcessIdentifier("id2"), 1).getProcessIDZNode();

    assertTrue(service.hasSchema(processOne, version1));
    assertFalse(service.hasSchema(processOne, version2));

    assertTrue(service.hasSchema(processTwo, version1));
    assertTrue(service.hasSchema(processTwo, version2));
  }

  @Test
  public void testWeGetTheLargerValueWhenAddingASchema() throws Exception {
    SchemaRegistryService service = new SchemaRegistryService(client);
    SchemaVersion version = new SchemaVersion("version1");
    ProcessIdentifier id = new ProcessIdentifier("id1");
    ProcessIdentifier id2 = new ProcessIdentifier("id2");
    service.registerSchema(version, id, 1);
    String schemaPath = ZKPaths.makePath(RegistrationType.SCHEMA.getBasePath(), version.getId());
    assertEquals(1, SchemaTTLSerializer.deSerialize(client.getData().forPath(schemaPath)).getTtl());
    service.registerSchema(version, id2, 2);
    assertEquals(2, SchemaTTLSerializer.deSerialize(client.getData().forPath(schemaPath)).getTtl());
    // make sure no timestamp is set
    assertFalse(SchemaTTLSerializer.deSerialize(client.getData().forPath(schemaPath)).isTimestamp());
  }

  @Test
  public void testEphemeralNodesSequencingWith() throws Exception {
    List<Callable<Collection<CuratorTransactionResult>>> list = Lists.newArrayList();
    final int MAX = 100;
    int i = 0;
    int j = 0;
    while (i < MAX) {
      list.add(getCallable(i, j));
      i++;
      if (i % 5 == 0) {
        j++;
      }
    }
    ExecutorService executorService = Executors.newFixedThreadPool(100);
    List<Future<Collection<CuratorTransactionResult>>> result = executorService.invokeAll(list);
    for (Future<Collection<CuratorTransactionResult>> future : result) {
      Iterator<CuratorTransactionResult> resultIterator = future.get().iterator();
      String schemaPath = resultIterator.next().getResultPath();
      String processPath = resultIterator.next().getResultPath();
      String schemaNode = ZKPaths.getPathAndNode(schemaPath).getNode();
      String processNode = ZKPaths.getPathAndNode(processPath).getNode();
      String errorMessage = "Schema path is: " + schemaPath + " but process path is: " + processPath;
      assertEquals(errorMessage, schemaNode, processNode);
    }
  }

  @Test
  public void testMakingASchemaVersionPathAlwaysIncludesYourOwnPath() throws Exception {
    SchemaRegistryService service = new SchemaRegistryService(client);
    int i = 0;
    int j = 100;
    while (i < 100) {
      SchemaVersion mySchemaVersion = new SchemaVersion(String.valueOf(i));
      SchemaProcessZNodes result = service.registerSchema(mySchemaVersion, new ProcessIdentifier(String.valueOf(j)), 1l);
      Set<SchemaVersion> schemaVersions = service.getSchemaVersions(result.getProcessIDZNode());
      assertTrue(schemaVersions.contains(mySchemaVersion));
      i++;
      assertEquals(i, schemaVersions.size());
    }
  }

  @Test(expected = NoSuchElementException.class)
  public void testAmITheFirstProcessForSchema() throws Exception {
    SchemaVersion schemaVersion = new SchemaVersion("schemaA");
    SchemaRegistryService service = new SchemaRegistryService(client);
    String basePath = ZKPaths.makePath(RegistrationType.SCHEMA.getBasePath(), schemaVersion.getId());

    Pair<CuratorFramework, SequenceZNode> first = addProcess(basePath, "id3");
    Pair<CuratorFramework, SequenceZNode> second = addProcess(basePath, "id2");
    Pair<CuratorFramework, SequenceZNode> third = addProcess(basePath, "id1");

    assertEquals(first.snd, service.getEarliestProcessForSchema(schemaVersion));
    testUtil.killClientConnection(first.fst);
    assertEquals(second.snd, service.getEarliestProcessForSchema(schemaVersion));
    testUtil.killClientConnection(second.fst);
    assertEquals(third.snd, service.getEarliestProcessForSchema(schemaVersion));
    testUtil.killClientConnection(third.fst);
    service.getEarliestProcessForSchema(schemaVersion);
  }

  private Callable<Collection<CuratorTransactionResult>> getCallable(final int id, final int schema) {
    return new Callable<Collection<CuratorTransactionResult>>() {

      @Override
      public Collection<CuratorTransactionResult> call() throws Exception {
        String idName = "-id-" + id;
        String schemaId = "-schema-" + schema;
        CuratorFramework randomClient = testUtil.getRandomClient();
        return randomClient
                .inTransaction()
                .create()
                .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                .forPath(ZKPaths.makePath(RegistrationType.SCHEMA.getBasePath(), idName))
                .and()
                .create()
                .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                .forPath(ZKPaths.makePath(RegistrationType.PROCESS.getBasePath(), idName))
                .and()
                .commit();
      }
    };
  }

  private Pair<CuratorFramework, SequenceZNode> addProcess(String schemaPath, String idPrefix) throws Exception {
    CuratorFramework client = testUtil.getRandomClient();
    String path = client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(ZKPaths.makePath(schemaPath, idPrefix + "-"));
    return new Pair<CuratorFramework, SequenceZNode>(client, SequenceZNode.parse(path));
  }

}
