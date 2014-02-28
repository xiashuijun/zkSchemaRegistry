package com.flurry.registry.domain;

public class ProcessIdentifier implements IdAware {

  private final String id;

  public ProcessIdentifier(String id) {
    this.id = id;
  }

  public String getId() {
    return id;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ProcessIdentifier that = (ProcessIdentifier) o;

    if (!id.equals(that.id)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }

  @Override
  public String toString() {
    return "ProcessIdentifier{" +
            "id='" + id + '\'' +
            '}';
  }
}
