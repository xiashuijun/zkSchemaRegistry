package com.flurry.registry.domain;

import java.io.Serializable;
import java.util.Date;

public class SchemaTTL implements Serializable {

  private static final long serialVersionUID = 1l;
  private long timestamp;
  private long ttl;

  public SchemaTTL(long ttl) {
    this.timestamp = -1;
    this.ttl = ttl;
  }

  public boolean isTimestamp() {
    return timestamp != -1;
  }

  public void setAsTimestamp() {
    this.timestamp = System.currentTimeMillis() + ttl;
  }

  public Date getExpirationTime() {
    return new Date(timestamp);
  }

  public boolean isExpired() {
    return isTimestamp() && System.currentTimeMillis() > this.timestamp;
  }

  public long getTtl() {
    return ttl;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    SchemaTTL schemaTTL = (SchemaTTL) o;

    if (timestamp != schemaTTL.timestamp) return false;
    if (ttl != schemaTTL.ttl) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = (int) (timestamp ^ (timestamp >>> 32));
    result = 31 * result + (int) (ttl ^ (ttl >>> 32));
    return result;
  }
}
