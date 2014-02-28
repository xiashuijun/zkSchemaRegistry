package com.flurry.registry.service;

import com.flurry.registry.domain.SequenceZNode;
import com.google.common.collect.Sets;
import com.netflix.curator.framework.CuratorFramework;

import java.util.SortedSet;
import java.util.TreeSet;

public class ZNodeSequenceService {

  private final CuratorFramework client;

  public ZNodeSequenceService(CuratorFramework client) {
    this.client = client;
  }

  public SortedSet<SequenceZNode> getAllWithLowerSequence(String basePath, SequenceZNode sequenceZNode, boolean inclusive) throws Exception {
    return getSortedChildren(basePath).headSet(sequenceZNode, inclusive);
  }

  public TreeSet<SequenceZNode> getSortedChildren(String basePath) throws Exception {
    TreeSet<SequenceZNode> result = Sets.newTreeSet();
    for (String somePath : client.getChildren().forPath(basePath)) {
      result.add(SequenceZNode.parse(somePath));
    }
    return result;
  }

  public SequenceZNode getEarliestSequence(String basePath) throws Exception {
    return getSortedChildren(basePath).first();
  }

}
