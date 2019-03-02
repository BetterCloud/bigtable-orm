package com.bettercloud.bigtable.orm;

import java.util.function.Supplier;

public interface EntityConfiguration<T extends Entity> {

    String getDefaultTableName();

    Iterable<Column> getColumns();

    Supplier<T> getEntityFactory();

    EntityDelegate<T> getDelegateForEntity(final T entity);

    interface EntityDelegate<T extends Entity> {

        Object getColumnValue(final Column column);

        void setColumnValue(final Column column, final Object value);

        Long getColumnTimestamp(final Column column);

        void setColumnTimestamp(final Column column, final long timestamp);
    }
}
