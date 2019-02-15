package com.bettercloud.bigtable.orm;

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
    public String toString() {
        return keyString;
    }
}
