package com.flurry.registry.service;

import com.flurry.registry.domain.*;
import com.flurry.registry.factory.UpdateSchemaTTLLock;
import com.flurry.registry.serializer.SchemaVersionsSerializer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.transaction.CuratorTransactionResult;
import com.netflix.curator.utils.EnsurePath;
import com.netflix.curator.utils.ZKPaths;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;

import java.util.*;

public class SchemaRegistryService {

  private static final Logger log = Logger.getLogger(SchemaRegistryService.class);
  private final CuratorFramework client;
  private final ZNodeSequenceService sequenceService;

  public SchemaRegistryService(CuratorFramework client) {
    this.client = client;
    this.sequenceService = new ZNodeSequenceService(client);
  }

  public void sync(RegistrationType type) {
    client.sync(type.getBasePath(), null);
  }

  public boolean doAllNodesBelowSequenceHaveIdenticalSchemas(final SequenceZNode zNode) throws Exception {
    SortedSet<SequenceZNode> activeProcesses = getActiveSchemasBeforeInclusive(zNode);
    Iterator<SequenceZNode> iterator = activeProcesses.iterator();
    SequenceZNode previousProcess = iterator.next();
    while (iterator.hasNext()) {
      SequenceZNode currentProcess = iterator.next();
      if (!getSchemaVersions(previousProcess).equals(getSchemaVersions(currentProcess))) {
        return false;
      }
      previousProcess = currentProcess;
    }
    return true;
  }

  public SequenceZNode getEarliestProcessForSchema(final SchemaVersion schemaVersion) throws Exception {
    return sequenceService.getEarliestSequence(getSchemaVersionPath(schemaVersion));
  }

  public boolean hasSchema(SequenceZNode zNode, SchemaVersion schemaVersion) throws Exception {
    return getSchemaVersions(zNode).contains(schemaVersion);
  }

  public void mutateMyActiveSchemas(SchemaVersion schemaVersion, SequenceZNode myZNode, ModificationType modificationType) throws Exception {
    Set<SchemaVersion> currentlyActiveSchemas = getSchemaVersions(myZNode);
    modificationType.mutate(schemaVersion, currentlyActiveSchemas);
    client.setData().forPath(getProcessVersionPath(myZNode), SchemaVersionsSerializer.serialize(currentlyActiveSchemas));
  }

  public SchemaProcessZNodes registerSchema(SchemaVersion schemaVersion, ProcessIdentifier identifier, long expirationTimeMs) throws Exception {
    log.info("Starting to register schema: " + schemaVersion.getId() + " for process: " + identifier.getId() + " with timeout of: " + expirationTimeMs);
    ensureBasePaths();
    UpdateSchemaTTLLock lock = new UpdateSchemaTTLLock(client, schemaVersion, new SchemaTTL(expirationTimeMs));
    lock.executeWithLock();
    Collection<CuratorTransactionResult> results = registerInTransaction(identifier, schemaVersion);
    Iterator<CuratorTransactionResult> iterator = results.iterator();
    String schemaPath = iterator.next().getResultPath();
    String processPath = iterator.next().getResultPath();
    log.info("Done registering schema");
    return new SchemaProcessZNodes(schemaPath, processPath);
  }

  public Set<SchemaVersion> getSchemaVersions(SequenceZNode zNode) throws Exception {
    String path = ZKPaths.makePath(RegistrationType.PROCESS.getBasePath(), zNode.getNodeName());
    return SchemaVersionsSerializer.deSerialize(client.getData().forPath(path));
  }

  @VisibleForTesting SortedSet<SequenceZNode> getActiveSchemasBeforeInclusive(SequenceZNode zNode) throws Exception {
    return sequenceService.getAllWithLowerSequence(RegistrationType.PROCESS.getBasePath(), zNode, true);
  }

  @VisibleForTesting
  Collection<CuratorTransactionResult> registerInTransaction(ProcessIdentifier identifier, SchemaVersion schemaVersion) throws Exception {
    String ephemeralId = SequenceZNode.makeZNodePrefixPath(identifier);
    return client.inTransaction()
            .create()
            .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
            .forPath(
                    ZKPaths.makePath(getSchemaVersionPath(schemaVersion), ephemeralId)
            )
            .and()
            .create()
            .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
            .forPath(
                    ZKPaths.makePath(RegistrationType.PROCESS.getBasePath(), ephemeralId), SchemaVersionsSerializer.serialize(getActiveSchemas(schemaVersion))
            )
            .and()
            .commit();
  }

  @VisibleForTesting
  HashSet<SchemaVersion> getActiveSchemas(SchemaVersion schemaVersion) throws Exception {
    HashSet<SchemaVersion> result = Sets.newHashSet();
    List<String> strings = client.getChildren().forPath(RegistrationType.SCHEMA.getBasePath());
    for (final String version : strings) {
      result.add(new SchemaVersion(version));
    }
    result.add(schemaVersion);
    return result;
  }

  private String getSchemaVersionPath(SchemaVersion schemaVersion) {
    return ZKPaths.makePath(RegistrationType.SCHEMA.getBasePath(), schemaVersion.getId());
  }

  private String getProcessVersionPath(SequenceZNode sequenceZNode) {
    return ZKPaths.makePath(RegistrationType.PROCESS.getBasePath(), sequenceZNode.getNodeName());
  }

  private void ensureBasePaths() {
    try {
      for (RegistrationType type : RegistrationType.values()) {
        new EnsurePath(type.getBasePath()).ensure(client.getZookeeperClient());
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }


}
