package com.bettercloud.bigtable.orm;

public interface KeyBuilder<T extends Entity> {

    Key<T> build();
}
