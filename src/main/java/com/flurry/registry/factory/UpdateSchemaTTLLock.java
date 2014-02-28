package com.flurry.registry.factory;

import com.flurry.registry.domain.RegistrationType;
import com.flurry.registry.domain.SchemaTTL;
import com.flurry.registry.domain.SchemaVersion;
import com.flurry.registry.serializer.SchemaTTLSerializer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.utils.EnsurePath;
import com.netflix.curator.utils.ZKPaths;

public class UpdateSchemaTTLLock extends LockFactory<Boolean> {

  private final SchemaTTL ttl;
  private final String schemaPath;

  public UpdateSchemaTTLLock(CuratorFramework client, SchemaVersion version, SchemaTTL ttl) {
    super(client);
    schemaPath = ZKPaths.makePath(RegistrationType.SCHEMA.getBasePath(), version.getId());
    this.ttl = ttl;
  }

  @Override
  public Boolean runCommand() {
    try {
      new EnsurePath(schemaPath).ensure(client.getZookeeperClient());
      if (overwriteTTL(schemaPath, ttl)) {
        client.setData().forPath(schemaPath, SchemaTTLSerializer.serialize(ttl));
        return true;
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
    return false;
  }

  @VisibleForTesting
  boolean overwriteTTL(String schemaPath, SchemaTTL ttl) throws Exception {
    byte[] data = client.getData().forPath(schemaPath);
    if (data.length > 0) {
      return ttl.getTtl() > SchemaTTLSerializer.deSerialize(data).getTtl();
    } else {
      client.setData().forPath(schemaPath, SchemaTTLSerializer.serialize(ttl));
      return false;
    }

  }

  @Override
  public String getLockPath() {
    return "/lock";
  }
}
