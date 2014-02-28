package com.flurry.registry.domain;

public enum RegistrationType {
  SCHEMA("schema"),
  PROCESS("process");

  private final String basePath;

  RegistrationType(String basePath) {
    this.basePath = basePath;
  }

  public String getBasePath() {
    return "/" + basePath;
  }

}
