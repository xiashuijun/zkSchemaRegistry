package com.flurry.registry;

import com.flurry.registry.domain.RegistrationType;
import com.google.common.base.Throwables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.imps.CuratorFrameworkState;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.curator.test.KillSession;
import com.netflix.curator.test.TestingCluster;
import com.netflix.curator.utils.EnsurePath;
import org.apache.zookeeper.ZKUtil;

import java.io.IOException;

public class ZKTestUtil {

  private final TestingCluster cluster;

  public ZKTestUtil(int numNodes) {
    try {
      this.cluster = new TestingCluster(numNodes);
      cluster.start();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public CuratorFramework getRandomClient() throws Exception {
    CuratorFramework client = CuratorFrameworkFactory.newClient(cluster.getConnectString(), new ExponentialBackoffRetry(1000, 5));
    client.start();
    while (!client.getState().equals(CuratorFrameworkState.STARTED)) {
      Thread.sleep(100);
    }
    new EnsurePath(RegistrationType.PROCESS.getBasePath()).ensure(client.getZookeeperClient());
    new EnsurePath(RegistrationType.SCHEMA.getBasePath()).ensure(client.getZookeeperClient());
    return client;
  }

  public void killClientConnection(CuratorFramework client) throws Exception {
    KillSession.kill(client.getZookeeperClient().getZooKeeper(), cluster.getConnectString());
  }

  public void stopAll() throws IOException {
    cluster.stop();
  }

  public static void printDirectoryTree(CuratorFramework client, String path) throws Exception {
    if (path.endsWith("/")) {
      path = path.substring(0, path.length() - 1);
    }
    if (!path.startsWith("/")) {
      path = "/" + path;
    }
    System.out.println("---- DIRECTORY TREE ----");
    for (String someShit : ZKUtil.listSubTreeBFS(client.getZookeeperClient().getZooKeeper(), path)) {
      System.out.println(someShit);
    }
    System.out.println("------------------------");
  }

}
