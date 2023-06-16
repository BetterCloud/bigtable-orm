package com.bettercloud.bigtable.orm;

import java.util.Arrays;
import org.apache.hadoop.hbase.util.Bytes;

public class RawKey<T extends Entity> implements Key<T> {

  private final byte[] key;

  public RawKey(byte[] key) {
    this.key = Arrays.copyOf(key, key.length);
  }

  @Override
  public byte[] toBytes() {
    return key;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    RawKey<?> rawKey = (RawKey<?>) o;
    return Arrays.equals(key, rawKey.key);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(key);
  }

  @Override
  public int compareTo(Key otherKey) {
    return Bytes.compareTo(this.toBytes(), otherKey.toBytes());
  }
}
