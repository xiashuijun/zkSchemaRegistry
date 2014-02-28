package com.flurry.registry.domain;

public class SchemaProcessZNodes {

  private final SequenceZNode schemaZNode;
  private final SequenceZNode processIDZNode;

  public SchemaProcessZNodes(String schemaPath, String processPath) {
    this(SequenceZNode.parse(schemaPath), SequenceZNode.parse(processPath));

  }
  public SchemaProcessZNodes(SequenceZNode schemaZNode, SequenceZNode processIDZNode) {
    this.schemaZNode = schemaZNode;
    this.processIDZNode = processIDZNode;
  }

  public SequenceZNode getSchemaZNode() {
    return schemaZNode;
  }

  public SequenceZNode getProcessIDZNode() {
    return processIDZNode;
  }
}
