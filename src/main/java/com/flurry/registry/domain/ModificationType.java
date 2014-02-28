package com.flurry.registry.domain;

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheEvent;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

public enum ModificationType {

  ADD(PathChildrenCacheEvent.Type.CHILD_ADDED) {
    @Override
    public void mutate(SchemaVersion schemaVersion, Set<SchemaVersion> schemaVersions) {
      schemaVersions.add(schemaVersion);
    }
  },
  REMOVE(PathChildrenCacheEvent.Type.CHILD_REMOVED) {
    @Override
    public void mutate(SchemaVersion schemaVersion, Set<SchemaVersion> schemaVersions) {
      schemaVersions.remove(schemaVersion);
    }
  };

  public static final Map<PathChildrenCacheEvent.Type, ModificationType> MAP = Maps.uniqueIndex(Arrays.asList(ModificationType.values()), new Function<ModificationType, PathChildrenCacheEvent.Type>() {
    @Override
    public PathChildrenCacheEvent.Type apply(ModificationType input) {
      return input.getEventType();
    }
  });

  private final PathChildrenCacheEvent.Type eventType;

  ModificationType(PathChildrenCacheEvent.Type eventType) {
    this.eventType = eventType;
  }

  public PathChildrenCacheEvent.Type getEventType() {
    return eventType;
  }

  public static ModificationType getForEvent(PathChildrenCacheEvent event) {
    return MAP.get(event.getType());
  }

  public abstract void mutate(SchemaVersion schemaVersion, Set<SchemaVersion> schemaVersions);


}
