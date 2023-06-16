package com.bettercloud.bigtable.orm;

import java.util.Objects;
import org.apache.hadoop.hbase.util.Bytes;

public class StringKey<T extends Entity> implements Key<T> {

  private final String keyString;

  @SuppressWarnings("WeakerAccess") // Used by generated Entities
  public StringKey(final String keyString) {
    this.keyString = keyString;
  }

  @Override
  public byte[] toBytes() {
    return Bytes.toBytes(keyString);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final StringKey<?> stringKey = (StringKey<?>) o;

    return Objects.equals(keyString, stringKey.keyString);
  }

  @Override
  public int hashCode() {
    return Objects.hash(keyString);
  }

  @Override
  public String toString() {
    return keyString;
  }

  @Override
  public int compareTo(Key otherKey) {
    return Bytes.compareTo(this.toBytes(), otherKey.toBytes());
  }
}
