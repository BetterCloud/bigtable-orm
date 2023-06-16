package com.bettercloud.bigtable.orm;

public interface Key<T extends Entity> extends Comparable<Key> {

  byte[] toBytes();
}
