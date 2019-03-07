package com.bettercloud.bigtable.orm;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

class BigTableEntityDao<T extends Entity> implements Dao<T> {

    private final Table table;
    private final Iterable<Column> columns;
    private final Supplier<T> entityFactory;
    private final Function<T, EntityConfiguration.EntityDelegate<T>> delegateFactory;
    private final ObjectMapper objectMapper;

    BigTableEntityDao(final Table table,
                      final Iterable<Column> columns,
                      final Supplier<T> entityFactory,
                      final Function<T, EntityConfiguration.EntityDelegate<T>> delegateFactory,
                      final ObjectMapper objectMapper) {
        this.table = table;
        this.columns = columns;
        this.entityFactory = entityFactory;
        this.delegateFactory = delegateFactory;
        this.objectMapper = objectMapper;
    }

    BigTableEntityDao(final Table table,
                      final Iterable<Column> columns,
                      final Supplier<T> entityFactory,
                      final Function<T, EntityConfiguration.EntityDelegate<T>> delegateFunction) {
        this(table, columns, entityFactory, delegateFunction, new ObjectMapper());
    }

    /**
     * If the key does not exist, then an empty {@link Optional} is returned.
     *
     * If the key exists, then an {@link Optional} containing an object of type {@link T} is returned.
     *
     * The returned object will contain data for each configured column only if the corresponding column actually
     * contained data. If data is found, then it is deserialized using the configured
     * {@link com.fasterxml.jackson.core.type.TypeReference} for that column. If no data is found, then the
     * corresponding value for that column is set to null.
     *
     * @param key The key of the row to retrieve
     * @param <K> The type of key used to retrieve the row
     * @return An Optional containing the specified row, if it exists, otherwise an empty Optional
     * @throws IOException when an error occurs while communicating with BigTable
     */
    @Override
    public <K extends Key<T>> Optional<T> get(final K key) throws IOException {
        Objects.requireNonNull(key);

        final T entity;

        final Get get = new Get(key.toBytes());

        for (final Column column : columns) {
            get.addColumn(Bytes.toBytes(column.getFamily()), Bytes.toBytes(column.getQualifier()));
        }

        final Result result = table.get(get);

        if (!result.isEmpty()) {
            entity = entityFactory.get();

            final EntityConfiguration.EntityDelegate<T> delegate = delegateFactory.apply(entity);

            for (final Column column : columns) {
                final byte[] family = Bytes.toBytes(column.getFamily());
                final byte[] qualifier = Bytes.toBytes(column.getQualifier());

                final Cell cell = result.getColumnLatestCell(family, qualifier);

                final Object value;

                if (cell != null) {
                    final byte[] bytes = cell.getValueArray();

                    if (bytes.length > 0) {
                        value = objectMapper.readValue(bytes, column.getTypeReference());
                    } else {
                        value = null;
                    }
                } else {
                    value = null;
                }

                delegate.setColumnValue(column, value);

                if (column.isVersioned()) {
                    final Long timestamp = Optional.ofNullable(cell)
                            .map(Cell::getTimestamp)
                            .orElse(null);

                    delegate.setColumnTimestamp(column, timestamp);
                }
            }
        } else {
            entity = null;
        }

        return Optional.ofNullable(entity);
    }

    /**
     * It appears to be possible to only update specific columns instead of writing the entire Entity in every pass.
     *
     * Low-hanging fruit for storing hashes of column contents within the Entity to reduce redundant writes.
     *
     * It is possible to put null values into all columns, while allowing the key to continue to exist.
     *
     * @param key The key of the row to persist
     * @param entity The entity which should be persisted
     * @param <K> The type of key used to persist the row
     * @return An entity representing the value that was actually persisted, including any updated timestamps
     * @throws IOException when an error occurs while communicating with BigTable
     */
    @Override
    public <K extends Key<T>> T save(final K key, final T entity) throws IOException {
        Objects.requireNonNull(key);
        Objects.requireNonNull(entity);

        final T result = entityFactory.get();

        final Put put = new Put(key.toBytes());

        final EntityConfiguration.EntityDelegate<T> sourceDelegate = delegateFactory.apply(entity);
        final EntityConfiguration.EntityDelegate<T> resultDelegate = delegateFactory.apply(result);

        for (final Column column : columns) {
            final Object value = sourceDelegate.getColumnValue(column);
            resultDelegate.setColumnValue(column, value);

            final byte[] bytes;

            if (value != null) {
                bytes = objectMapper.writeValueAsBytes(value);
            } else {
                bytes = null;
            }

            if (column.isVersioned()) {
                final long timestamp = Optional.ofNullable(sourceDelegate.getColumnTimestamp(column))
                        .orElseGet(() -> Instant.now().toEpochMilli());

                put.addColumn(Bytes.toBytes(column.getFamily()), Bytes.toBytes(column.getQualifier()), timestamp, bytes);

                resultDelegate.setColumnTimestamp(column, timestamp);
            } else {
                put.addColumn(Bytes.toBytes(column.getFamily()), Bytes.toBytes(column.getQualifier()), bytes);
            }
        }

        table.put(put);

        return result;
    }

    /**
     * {@link Delete#addColumn(byte[], byte[])} deletes the <i>latest</i> value by timestamp in the column, while
     * {@link Delete#addColumns(byte[], byte[])} deletes <i>all</i> values in the column.
     *
     * A future release may implement multiple versions per column, but for now we're only working with one version,
     * and thus want to make sure this column is empty for the given row key (delete <i>all</i> versions).
     *
     * @param key The key of the row to delete
     * @param <K> The type of key used to delete the row
     * @throws IOException when an error occurs while communicating with BigTable
     */
    @Override
    public <K extends Key<T>> void delete(final K key) throws IOException {
        Objects.requireNonNull(key);

        final Delete delete = new Delete(key.toBytes());

        for (final Column column : columns) {
            delete.addColumns(Bytes.toBytes(column.getFamily()), Bytes.toBytes(column.getQualifier()));
        }

        table.delete(delete);
    }
}
