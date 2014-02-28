package com.flurry.registry.serializer;

import com.flurry.registry.domain.SchemaVersion;
import com.google.common.base.Throwables;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.util.HashSet;
import java.util.Set;

public class SchemaVersionsSerializer {

  public static byte[] serialize(Set<SchemaVersion> schemaVersionSet) {
    if (schemaVersionSet == null) {
      schemaVersionSet = new HashSet<SchemaVersion>();
    }
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutput out = null;
    try {
      out = new ObjectOutputStream(baos);
      out.writeObject(schemaVersionSet);
      return baos.toByteArray();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    } finally {
      try {
        if (out != null) {
          out.close();
        }
      } catch (IOException ex) {
        // ignore close exception
      }
      IOUtils.closeQuietly(baos);
    }
  }

  public static Set<SchemaVersion> deSerialize(byte[] bytes) {
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    ObjectInput in = null;
    try {
      in = new ObjectInputStream(bais);
      return (Set<SchemaVersion>) in.readObject();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    } finally {
      IOUtils.closeQuietly(bais);
      try {
        if (in != null) {
          in.close();
        }
      } catch (IOException ex) {
        // ignore close exception
      }
    }
  }



}
