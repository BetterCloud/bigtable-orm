package com.bettercloud.bigtable.orm;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

class BigTableEntityDao<T extends Entity> extends AbstractDao<T> implements Dao<T> {

  private final Table table;

  BigTableEntityDao(
      final Table table,
      final Iterable<? extends Column> columns,
      final Supplier<T> entityFactory,
      final Function<T, EntityConfiguration.EntityDelegate<T>> delegateFactory,
      final ObjectMapper objectMapper) {
    super(columns, entityFactory, delegateFactory, objectMapper);
    this.table = table;
  }

  BigTableEntityDao(
      final Table table,
      final Iterable<? extends Column> columns,
      final Supplier<T> entityFactory,
      final Function<T, EntityConfiguration.EntityDelegate<T>> delegateFunction) {
    this(table, columns, entityFactory, delegateFunction, new ObjectMapper());
  }

  /**
   * If the key does not exist, then an empty {@link Optional} is returned.
   *
   * <p>If the key exists, then an {@link Optional} containing an object of type {@link T} is
   * returned.
   *
   * <p>The returned object will contain data for each configured column only if the corresponding
   * column actually contained data. If data is found, then it is deserialized using the configured
   * {@link com.fasterxml.jackson.core.type.TypeReference} for that column. If no data is found,
   * then the corresponding value for that column is set to null.
   *
   * @param key The key of the row to retrieve
   * @param <K> The type of key used to retrieve the row
   * @return An Optional containing the specified row, if it exists, otherwise an empty Optional
   * @throws IOException when an error occurs while communicating with BigTable
   * @throws NullPointerException when the provided key is null
   */
  @Override
  @Deprecated
  public <K extends Key<T>> Optional<T> get(final K key) throws IOException {
    Objects.requireNonNull(key);

    final Map<K, T> results = getAll(Collections.singleton(key));

    return Optional.ofNullable(results.get(key));
  }

  /**
   * If a specified key exists, then the resulting Map will contain the key and a value of type
   * {@link T}.
   *
   * <p>If a specified key does not exist, then the resulting Map will not contain the key at all.
   *
   * <p>This means that {@link Map#containsKey(Object)} will return <b>false</b> for rows that do
   * not exist.
   *
   * <p>The returned objects will contain data for each configured column only if the corresponding
   * column actually contained data. If data is found, then it is deserialized using the configured
   * {@link com.fasterxml.jackson.core.type.TypeReference} for that column. If no data is found,
   * then the corresponding value for that column is set to null.
   *
   * @param keys The keys of the rows to retrieve
   * @param <K> The type of the keys used to retrieve the rows
   * @return A Map containing pairs of keys and their corresponding nullable values
   * @throws IOException when an error occurs while communicating with BigTable
   * @throws NullPointerException when the provided Set of keys is null
   */
  @Override
  public <K extends Key<T>> Map<K, T> getAll(final Set<K> keys) throws IOException {
    Objects.requireNonNull(keys);

    final List<K> keyList = new ArrayList<>(keys);

    final List<Get> gets = keysToGets(keys);

    final Result[] results = table.get(gets);

    final Map<K, Result> resultsByKey =
        IntStream.range(0, gets.size())
            .boxed()
            .collect(Collectors.toMap(keyList::get, i -> results[i]));

    final Map<K, T> entitiesByKey = new HashMap<>();

    for (final Map.Entry<K, Result> entry : resultsByKey.entrySet()) {
      final Result result = entry.getValue();

      if (!result.isEmpty()) {
        final T entity = convertToEntity(result);
        entitiesByKey.put(entry.getKey(), entity);
      }
    }

    return Collections.unmodifiableMap(entitiesByKey);
  }

  /**
   * Utility method for running scan without a provided constant.
   *
   * <p>Runs a paging table scan from the provided starting key to the provided ending key, and
   * returns a list of paired Key/Value entities in the order returned from BigTable.
   *
   * <p>Use the last returned entity to construct a new starting key for subsequent paging requests
   * until no values are returned.
   *
   * @param startKey key to start scanning from (does not have to have an existing record at the
   *     location)
   * @param startKeyInclusive whether to include result from startKey
   * @param endKey key to end scanning on (does not have to have an existing record at the location)
   * @param endKeyInclusive whether to include result from endKey
   * @param numRows max number of entries to return
   * @return A list of entities in the order that they are stored in BigTable
   * @throws IOException when an error occurs while communicating with BigTable
   */
  @Override
  public <K extends Key<T>> SortedMap<Key<T>, T> scan(
      final K startKey,
      final boolean startKeyInclusive,
      final K endKey,
      final boolean endKeyInclusive,
      final int numRows)
      throws IOException {
    return scan(startKey, startKeyInclusive, endKey, endKeyInclusive, numRows, null);
  }

  /**
   * Runs a paging table scan from the provided starting key to the provided ending key, and returns
   * a list of paired Key/Value entities in the order returned from BigTable.
   *
   * <p>Use the last returned entity to construct a new starting key for subsequent paging requests
   * until no values are returned.
   *
   * @param startKey key to start scanning from (does not have to have an existing record at the
   *     location)
   * @param startKeyInclusive whether to include result from startKey
   * @param endKey key to end scanning on (does not have to have an existing record at the location)
   * @param endKeyInclusive whether to include result from endKey
   * @param numRows max number of entries to return
   * @param constant optional field to be used to be included, should be the constant provided to
   *     KeyComponent if it exists
   * @return A list of entities in the order that they are stored in BigTable
   * @throws IOException when an error occurs while communicating with BigTable
   */
  @Override
  public <K extends Key<T>> SortedMap<Key<T>, T> scan(
      final K startKey,
      final boolean startKeyInclusive,
      final K endKey,
      final boolean endKeyInclusive,
      final int numRows,
      @Nullable final String constant)
      throws IOException {
    Objects.requireNonNull(startKey);
    Objects.requireNonNull(endKey);

    final Scan scan =
        keysToScan(startKey, startKeyInclusive, endKey, endKeyInclusive, numRows, constant);

    final ResultScanner scanner = table.getScanner(scan);
    final SortedMap<Key<T>, T> results = new TreeMap<>();

    Result result;
    while ((result = scanner.next()) != null) {
      if (!result.isEmpty()) {
        final T entity = convertToEntity(result);
        results.put(new RawKey<T>(result.getRow()), entity);
      }
    }

    return Collections.unmodifiableSortedMap(results);
  }

  /**
   * It appears to be possible to only update specific columns instead of writing the entire Entity
   * in every pass.
   *
   * <p>Low-hanging fruit for storing hashes of column contents within the Entity to reduce
   * redundant writes.
   *
   * <p>It is possible to put null values into all columns, while allowing the key to continue to
   * exist.
   *
   * @param key The key of the row to persist
   * @param entity The entity which should be persisted
   * @param <K> The type of key used to persist the row
   * @return An entity representing the value that was actually persisted, including any updated
   *     timestamps
   * @throws IOException when an error occurs while communicating with BigTable
   * @throws NullPointerException when the provided key or value is null
   */
  @Override
  @Deprecated
  public <K extends Key<T>> T save(final K key, final T entity) throws IOException {
    Objects.requireNonNull(key);
    Objects.requireNonNull(entity);

    final Map<K, T> results = saveAll(Collections.singletonMap(key, entity));

    return results.get(key);
  }

  /**
   * Persists the provided keys and their corresponding values.
   *
   * <p>The Map returned contains all of the provided keys, and the actual values that were
   * persisted, including any updated timestamps for "versioned" columns.
   *
   * <p>It appears to be possible to only update specific columns instead of writing the entire
   * Entity in every pass.
   *
   * <p>Low-hanging fruit for storing hashes of column contents within the Entity to reduce
   * redundant writes.
   *
   * <p>It is possible to put null values into all columns, while allowing the key to continue to
   * exist.
   *
   * @param entities A Map containing the the keys and their corresponding values to persist
   * @param <K> The type of key used to persist the rows
   * @return A Map containing the keys and the values that were actually persisted, including any
   *     updated timestamps
   * @throws IOException when an error occurs while communicating with BigTable
   * @throws NullPointerException when the provided Map is null, or any of its keys or values is
   *     null
   */
  @Override
  public <K extends Key<T>> Map<K, T> saveAll(final Map<K, T> entities) throws IOException {
    Objects.requireNonNull(entities);

    final PutResultDto<K, T> putResults = entitiesToPuts(entities);
    final Map<K, T> results = putResults.getKeyValueMap();
    final List<Put> puts = putResults.getPuts();

    table.put(puts);

    return Collections.unmodifiableMap(results);
  }

  /**
   * {@link Delete#addColumn(byte[], byte[])} deletes the <i>latest</i> value by timestamp in the
   * column, while {@link Delete#addColumns(byte[], byte[])} deletes <i>all</i> values in the
   * column.
   *
   * <p>A future release may implement multiple versions per column, but for now we're only working
   * with one version, and thus want to make sure this column is empty for the given row key (delete
   * <i>all</i> versions).
   *
   * @param key The key of the row to delete
   * @param <K> The type of key used to delete the row
   * @throws IOException when an error occurs while communicating with BigTable
   * @throws NullPointerException when the provided key is null
   */
  @Override
  @Deprecated
  public <K extends Key<T>> void delete(final K key) throws IOException {
    Objects.requireNonNull(key);

    deleteAll(Collections.singleton(key));
  }

  /**
   * {@link Delete#addColumn(byte[], byte[])} deletes the <i>latest</i> value by timestamp in the
   * column, while {@link Delete#addColumns(byte[], byte[])} deletes <i>all</i> values in the
   * column.
   *
   * <p>A future release may implement multiple versions per column, but for now we're only working
   * with one version, and thus want to make sure this column is empty for the given row key (delete
   * <i>all</i> versions).
   *
   * @param keys The keys of the rows to delete
   * @param <K> The type of key used to delete the rows
   * @throws IOException when an error occurs while communicating with BigTable
   * @throws NullPointerException when the provided Set is null
   */
  @Override
  public <K extends Key<T>> void deleteAll(final Set<K> keys) throws IOException {
    Objects.requireNonNull(keys);

    final List<Delete> deletes = keysToDeletes(keys);

    table.delete(deletes);
  }
}
