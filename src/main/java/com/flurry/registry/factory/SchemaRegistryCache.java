package com.flurry.registry.factory;

import com.flurry.registry.domain.*;
import com.flurry.registry.exception.SchemasFailedToUpdateException;
import com.flurry.registry.service.SchemaReaper;
import com.flurry.registry.service.SchemaRegistryService;
import com.flurry.registry.util.DelayedRetry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.cache.PathChildrenCache;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheEvent;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;

import java.util.Set;

public class SchemaRegistryCache {

  private static final Logger log = Logger.getLogger(SchemaRegistryCache.class);
  private final Set<SchemaVersion> schemaVersions;
  private final SchemaRegistryService service;
  private final PathChildrenCache cache;
  private final SchemaReaper schemaReaper;
  private SchemaProcessZNodes myZNodes;
  private SchemaVersion mySchemaVersion;

  public SchemaRegistryCache(CuratorFramework client, SchemaRegistryService service, SchemaReaper reaper) {
    this.schemaVersions = Sets.newHashSet();
    this.service = service;
    this.cache = new PathChildrenCache(client, RegistrationType.SCHEMA.getBasePath(), true);
    this.schemaReaper = reaper;
  }

  public void start(SchemaVersion schemaVersion, ProcessIdentifier identifier, long ttl, DelayedRetry retry) throws Exception {
    schemaReaper.start();
    myZNodes = service.registerSchema(schemaVersion, identifier, ttl);
    SequenceZNode processZNode = myZNodes.getProcessIDZNode();
    service.sync(RegistrationType.PROCESS);

    int numAttempts = 0;
    while (!service.doAllNodesBelowSequenceHaveIdenticalSchemas(processZNode)) {
      if (!retry.allowRetry(++numAttempts)) {
        throw new SchemasFailedToUpdateException("Some processes failed to update their schemas in time, cant introduce new schema until all processes have updated");
      }
    }
    addListener();
  }

  public SchemaProcessZNodes getMyZNodes() {
    return myZNodes;
  }

  public Set<SchemaVersion> getActiveSchemas() {
    return schemaVersions;
  }

  public SchemaVersion getMySchemaVersion() {
    return mySchemaVersion;
  }

  @VisibleForTesting void addListener() throws Exception {
    this.cache.getListenable().addListener(new PathChildrenCacheListener() {
      @Override
      public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
        ModificationType modificationEvent = ModificationType.getForEvent(event);
        if (modificationEvent != null) {
          log.info("Have to update cache for event: " + modificationEvent.getEventType().name() + " path: " + event.getData().getPath());
          SchemaVersion schemaVersion = new SchemaVersion(FilenameUtils.getName(event.getData().getPath()));
          modificationEvent.mutate(schemaVersion, schemaVersions);
          service.mutateMyActiveSchemas(schemaVersion, myZNodes.getProcessIDZNode(), modificationEvent);
        }
      }
    });
    this.cache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
  }

}
