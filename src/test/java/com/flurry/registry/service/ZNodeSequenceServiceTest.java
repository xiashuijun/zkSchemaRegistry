package com.flurry.registry.service;

import com.flurry.registry.ZKTestUtil;
import com.flurry.registry.domain.RegistrationType;
import com.flurry.registry.domain.SequenceZNode;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Locale;

import static org.junit.Assert.assertEquals;

public class ZNodeSequenceServiceTest {

  private final ZKTestUtil testUtil = new ZKTestUtil(1);
  private CuratorFramework client;
  private ZNodeSequenceService service;

  @Before
  public void setUp() throws Exception {
    client = testUtil.getRandomClient();
    service = new ZNodeSequenceService(client);
  }

  @After
  public void tearDown() throws Exception {
    testUtil.stopAll();
  }

  @Test
  public void testGettingSchemasBelow() throws Exception {
    String prefix = "id";
    int numIterations = 10;
    int i = 0;
    while (i < numIterations) {
      String identifier = numIterations - i + "-";
      client
              .create()
              .creatingParentsIfNeeded()
              .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
              .forPath(ZKPaths.makePath(RegistrationType.PROCESS.getBasePath(), prefix + identifier));
      i++;
    }
    assertEquals(10, service.getAllWithLowerSequence(RegistrationType.PROCESS.getBasePath(), makeSequenceZNode(10), true).size());
    assertEquals(10, service.getAllWithLowerSequence(RegistrationType.PROCESS.getBasePath(), makeSequenceZNode(11), true).size());
    assertEquals(6, service.getAllWithLowerSequence(RegistrationType.PROCESS.getBasePath(), makeSequenceZNode(5), true).size());
  }

  private SequenceZNode makeSequenceZNode(long sequenceId) {
    return new SequenceZNode("bleh", String.format(Locale.ENGLISH, "%010d", sequenceId));
  }
}
