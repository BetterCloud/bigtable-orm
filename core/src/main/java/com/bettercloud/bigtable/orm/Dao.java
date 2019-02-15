package com.bettercloud.bigtable.orm;

import java.io.IOException;
import java.util.Optional;

public interface Dao<T extends Entity> {

    <K extends Key<T>> Optional<T> get(final K key) throws IOException;

    <K extends Key<T>> void save(final K key, final T entity) throws IOException;

    <K extends Key<T>> void delete(final K key) throws IOException;
}
