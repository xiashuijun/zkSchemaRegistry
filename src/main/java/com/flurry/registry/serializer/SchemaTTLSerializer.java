package com.flurry.registry.serializer;

import com.flurry.registry.domain.SchemaTTL;
import com.google.common.base.Throwables;
import org.apache.commons.io.IOUtils;

import java.io.*;

public class SchemaTTLSerializer {

  public static byte[] serialize(SchemaTTL schemaTTL) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutput out = null;
    try {
      out = new ObjectOutputStream(baos);
      out.writeObject(schemaTTL);
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

  public static SchemaTTL deSerialize(byte[] bytes) {
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    ObjectInput in = null;
    try {
      in = new ObjectInputStream(bais);
      return (SchemaTTL) in.readObject();
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
