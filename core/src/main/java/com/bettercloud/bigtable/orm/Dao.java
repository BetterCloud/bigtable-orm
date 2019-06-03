package com.bettercloud.bigtable.orm;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface Dao<T extends Entity> {

    @Deprecated
    <K extends Key<T>> Optional<T> get(final K key) throws IOException;

    <K extends Key<T>> Map<K, T> getAll(final Set<K> keys) throws IOException;

    <K extends Key<T>> List<T> scan(final K startKey,
                                    final boolean startKeyInclusive,
                                    final K endKey,
                                    final boolean endKeyInclusive,
                                    final int numRows) throws IOException;
    @Deprecated
    <K extends Key<T>> T save(final K key, final T entity) throws IOException;

    <K extends Key<T>> Map<K, T> saveAll(final Map<K, T> entities) throws IOException;

    @Deprecated
    <K extends Key<T>> void delete(final K key) throws IOException;

    <K extends Key<T>> void deleteAll(final Set<K> keys) throws IOException;
}
