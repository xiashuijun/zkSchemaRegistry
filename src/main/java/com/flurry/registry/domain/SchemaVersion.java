package com.flurry.registry.domain;

import org.apache.log4j.Logger;

import java.io.Serializable;

public class SchemaVersion implements Serializable, IdAware {

  private static final long serialVersionUID = 1l;
  private final String version;

  public SchemaVersion(String version) {
    this.version = version;
  }

  @Override
  public String getId() {
    return version;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    SchemaVersion that = (SchemaVersion) o;

    if (!version.equals(that.version)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    return version.hashCode();
  }

  @Override
  public String toString() {
    return "SchemaVersion{" +
            "version='" + version + '\'' +
            '}';
  }
}
