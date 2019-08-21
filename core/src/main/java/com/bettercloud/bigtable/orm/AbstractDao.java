package com.bettercloud.bigtable.orm;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import javax.annotation.Nullable;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

abstract class AbstractDao<T extends Entity> {

    private final Iterable<? extends Column> columns;
    private final Supplier<T> entityFactory;
    private final Function<T, EntityConfiguration.EntityDelegate<T>> delegateFactory;
    private final ObjectMapper objectMapper;

    AbstractDao(Iterable<? extends Column> columns,
                Supplier<T> entityFactory,
                Function<T, EntityConfiguration.EntityDelegate<T>> delegateFactory,
                ObjectMapper objectMapper) {
        this.columns = columns;
        this.entityFactory = entityFactory;
        this.delegateFactory = delegateFactory;
        this.objectMapper = objectMapper;
    }

    T convertToEntity(final Result result) throws IOException {
        final T entity = entityFactory.get();

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

        return entity;
    }

    <K extends Key<T>> Get keysToGets(final K key) {
        return keysToGets(Collections.singletonList(key)).get(0);
    }

    <K extends Key<T>> List<Get> keysToGets(final Collection<K> keys) {
        return keys.stream()
                .map(Key::toBytes)
                .map(key -> {
                    final Get get = new Get(key);
                    for (final Column column : columns) {
                        get.addColumn(Bytes.toBytes(column.getFamily()), Bytes.toBytes(column.getQualifier()));
                    }
                    return get;
                }).collect(Collectors.toList());
    }

    <K extends Key<T>> Scan keysToScan(final K startKey, final boolean startKeyInclusive,
                                       final K endKey, final boolean endKeyInclusive,
                                       final int numRows, @Nullable final String constant) {
        List<Filter> filters = new ArrayList<>();
        filters.add(new PageFilter(numRows));
        if (Objects.nonNull(constant) && !"".equals(constant)) {
            filters.add(new RowFilter(CompareOperator.EQUAL, new BinaryComparator(constant.getBytes())));
        }
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);

        final Scan scan = new Scan();
        scan.setFilter(filterList);
        scan.withStartRow(startKey.toBytes(), startKeyInclusive);
        scan.withStopRow(endKey.toBytes(), endKeyInclusive);

        return scan;
    }

    <K extends Key<T>> PutTuple<K, T> entitiesToPuts(final K key, T entity) throws IOException {
        return entitiesToPuts(Collections.singletonMap(key, entity)).putTuples.get(0);
    }

    <K extends Key<T>> PutResultDto<K, T> entitiesToPuts(final Map<K, T> entities) throws IOException {
        final List<PutTuple<K, T>> putResults = new ArrayList<>();

        for (final Map.Entry<K, T> entry : entities.entrySet()) {
            final K key = Objects.requireNonNull(entry.getKey());
            final T entity = Objects.requireNonNull(entry.getValue());

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

            putResults.add(new PutTuple<>(key, result, put));
        }

        return new PutResultDto<>(putResults);
    }

    <K extends Key<T>> Delete keysToDeletes(final K key) {
        return keysToDeletes(Collections.singletonList(key)).get(0);
    }

    <K extends Key<T>> List<Delete> keysToDeletes(final Collection<K> keys) {
        return keys.stream()
                .map(key -> {
                    final Delete delete = new Delete(key.toBytes());

                    for (final Column column : columns) {
                        delete.addColumns(Bytes.toBytes(column.getFamily()), Bytes.toBytes(column.getQualifier()));
                    }

                    return delete;
                })
                .collect(Collectors.toList());
    }

    static class PutResultDto<K, T> {
        private final List<PutTuple<K, T>> putTuples;

        PutResultDto(List<PutTuple<K, T>> putTuples) {
            this.putTuples = putTuples;
        }

        List<Put> getPuts() {
            return putTuples.stream().map(PutTuple::getPut)
                    .collect(Collectors.collectingAndThen(Collectors.toList(), Collections::unmodifiableList));
        }

        Map<K, T> getKeyValueMap() {
            return putTuples.stream()
                    .collect(Collectors.collectingAndThen(Collectors.toMap(PutTuple::getKey, PutTuple::getResult), Collections::unmodifiableMap));
        }

        List<K> getKeys() {
            return putTuples.stream().map(PutTuple::getKey)
                    .collect(Collectors.collectingAndThen(Collectors.toList(), Collections::unmodifiableList));
        }

        List<T> getResults() {
            return putTuples.stream().map(PutTuple::getResult)
                    .collect(Collectors.collectingAndThen(Collectors.toList(), Collections::unmodifiableList));
        }
    }

    static class PutTuple<K, T> {
        private final K key;
        private final T result;
        private final Put put;

        private PutTuple(final K key, final T result, final Put put) {
            this.key = key;
            this.result = result;
            this.put = put;
        }

        K getKey() {
            return key;
        }

        T getResult() {
            return result;
        }

        Put getPut() {
            return put;
        }
    }
}
