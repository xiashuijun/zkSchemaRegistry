package com.flurry.registry.domain;

import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;

public class SequenceZNode implements Comparable<SequenceZNode> {

  private static final Logger log = Logger.getLogger(SequenceZNode.class);
  public static final String SEPARATOR = "-";
  private final String id;
  private final String sequenceId;
  private final Long numericSequenceId;

  public SequenceZNode(String id, String sequenceId) {
    this.id = id;
    this.sequenceId = sequenceId;
    this.numericSequenceId = Long.parseLong(sequenceId);
  }

  public static SequenceZNode parse(String somePath) {
    log.info("Parsing sequence znode: " + somePath);
    String[] result = FilenameUtils.getName(somePath).split(SEPARATOR);
    return new SequenceZNode(result[0], result[1]);
  }

  public String getId() {
    return id;
  }

  public Long getSequenceIdAsLong() {
    return numericSequenceId;
  }

  public String getNodeName() {
    return this.id + SEPARATOR + this.sequenceId;
  }

  public static String makeZNodePrefixPath(IdAware identifier) {
    return identifier.getId() + SEPARATOR;
  }

  @Override
  public int compareTo(SequenceZNode that) {
    return this.getSequenceIdAsLong().compareTo(that.getSequenceIdAsLong());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    SequenceZNode that = (SequenceZNode) o;

    if (!id.equals(that.id)) return false;
    if (!numericSequenceId.equals(that.numericSequenceId)) return false;
    if (!sequenceId.equals(that.sequenceId)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = id.hashCode();
    result = 31 * result + sequenceId.hashCode();
    result = 31 * result + numericSequenceId.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return id + "-" + getSequenceIdAsLong();
  }
}
