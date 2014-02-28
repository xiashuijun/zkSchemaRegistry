package com.flurry.registry.factory;

import com.flurry.registry.ZKTestUtil;
import com.flurry.registry.domain.RegistrationType;
import com.flurry.registry.domain.SchemaTTL;
import com.flurry.registry.domain.SchemaVersion;
import com.flurry.registry.serializer.SchemaTTLSerializer;
import com.google.common.collect.Lists;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.utils.EnsurePath;
import com.netflix.curator.utils.ZKPaths;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;

public class UpdateSchemaTTLLockTest {

  private ZKTestUtil testUtil;
  private CuratorFramework mainClient;

  @Before
  public void setUp() throws Exception {
    testUtil = new ZKTestUtil(1);
    mainClient = testUtil.getRandomClient();
    new EnsurePath(RegistrationType.SCHEMA.getBasePath()).ensure(mainClient.getZookeeperClient());
  }

  @After
  public void tearDown() throws Exception {
    testUtil.stopAll();
  }

  @Test
  public void testLockingPreventsAnotherThreadFromUpdating() throws Exception {
    List<Callable<Boolean>> list = Lists.newArrayList();
    int i = 0;
    while (i < 10) {
      list.add(makeCallable(new UpdateSchemaTTLLock(testUtil.getRandomClient(), new SchemaVersion("a"), new SchemaTTL(i))));
      i++;
    }
    ExecutorService service = Executors.newFixedThreadPool(10);
    List<Future<Boolean>> result = service.invokeAll(list);
    // wait for everything to finish
    for (Future<Boolean> booleanFuture : result) {
      booleanFuture.get();
    }

    String schemaPath = ZKPaths.makePath(RegistrationType.SCHEMA.getBasePath(), "a");
    // the max ttl should be persisted
    assertEquals(9l, SchemaTTLSerializer.deSerialize(mainClient.getData().forPath(schemaPath)).getTtl());

  }

  private Callable<Boolean> makeCallable(final UpdateSchemaTTLLock process) {
    return new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return process.executeWithLock();
      }
    };
  }

}
