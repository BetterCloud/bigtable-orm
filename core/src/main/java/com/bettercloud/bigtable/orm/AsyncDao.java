package com.bettercloud.bigtable.orm;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.CompletableFuture;

public interface AsyncDao<T extends Entity> {

    <K extends Key<T>> CompletableFuture<T> get(final K key);

    <K extends Key<T>> Map<K, CompletableFuture<T>> get(Set<K> keys);

    <K extends Key<T>> CompletableFuture<Map<K, T>> getAll(final Set<K> keys);

    <K extends Key<T>> CompletableFuture<SortedMap<Key<T>, T>> scan(final K startKey,
                                                                    final boolean startKeyInclusive,
                                                                    final K endKey,
                                                                    final boolean endKeyInclusive,
                                                                    final int numRows);

    <K extends Key<T>> CompletableFuture<SortedMap<Key<T>, T>> scan(final K startKey,
                                                                    final boolean startKeyInclusive,
                                                                    final K endKey,
                                                                    final boolean endKeyInclusive,
                                                                    final int numRows,
                                                                    final String constant);

    <K extends Key<T>> CompletableFuture<T> save(final K key, final T entity) throws IOException;

    <K extends Key<T>> Map<K, CompletableFuture<T>> save(Map<K, T> entities) throws IOException;

    <K extends Key<T>> CompletableFuture<Map<K, T>> saveAll(final Map<K, T> entities) throws IOException;

    <K extends Key<T>> CompletableFuture<Void> delete(final K key);

    <K extends Key<T>> List<CompletableFuture<Void>> delete(Set<K> keys);

    <K extends Key<T>> CompletableFuture<Void> deleteAll(final Set<K> keys);
}
