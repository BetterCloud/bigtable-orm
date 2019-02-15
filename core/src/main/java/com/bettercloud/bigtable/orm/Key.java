package com.bettercloud.bigtable.orm;

public interface Key<T extends Entity> {

    byte[] toBytes();
}
