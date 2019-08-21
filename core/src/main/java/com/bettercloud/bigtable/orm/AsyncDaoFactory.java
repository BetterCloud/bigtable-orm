package com.bettercloud.bigtable.orm;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.BigtableAsyncConnection;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

public class AsyncDaoFactory {

    private final AsyncConnection asyncConnection;

    @SuppressWarnings("WeakerAccess") // Public API
    public AsyncDaoFactory(final String projectId, final String instanceId) throws IOException {
        this(new BigtableAsyncConnection(BigtableConfiguration.configure(projectId, instanceId)));
    }

    public AsyncDaoFactory(AsyncConnection asyncConnection) {
        this.asyncConnection = asyncConnection;
    }

    @SuppressWarnings("WeakerAccess") // Public API
    public <T extends Entity> AsyncDao<T> daoFor(final Class<T> entityType, final Options options) {
        Objects.requireNonNull(entityType);

        final EntityConfiguration<T> entityConfiguration = EntityRegistry.getConfigurationForType(entityType);

        return daoFor(entityConfiguration, options);
    }

    @SuppressWarnings("WeakerAccess") // Public API
    public <T extends Entity> AsyncDao<T> daoFor(final Class<T> entityType) {
        return daoFor(entityType, null);
    }

    @SuppressWarnings("WeakerAccess") // Public API
    public <T extends Entity> AsyncDao<T> daoFor(final EntityConfiguration<T> entityConfiguration, final Options options) {
        Objects.requireNonNull(entityConfiguration);

        final String resolvedTableName = Optional.ofNullable(options)
                .map(Options::getTableName)
                .orElseGet(entityConfiguration::getDefaultTableName);

        final TableName hbaseTableName = TableName.valueOf(resolvedTableName);

        final AsyncTable table = asyncConnection.getTable(hbaseTableName);
        final Iterable<? extends Column> columns = entityConfiguration.getColumns();
        final Supplier<T> entityFactory = entityConfiguration.getEntityFactory();
        final Function<T, EntityConfiguration.EntityDelegate<T>> delegateFactory = entityConfiguration::getDelegateForEntity;

        return new BigTableEntityAsyncDao<>(table, columns, entityFactory, delegateFactory);
    }

    @SuppressWarnings("WeakerAccess") // Public API
    public static OptionsBuilder optionsBuilder() {
        return new OptionsBuilder();
    }

    @SuppressWarnings("WeakerAccess") // Public API
    public static class Options {

        private final String tableName;

        private Options(final String tableName) {
            this.tableName = tableName;
        }

        private String getTableName() {
            return tableName;
        }
    }

    @SuppressWarnings("WeakerAccess") // Public API
    public static class OptionsBuilder {

        private String tableName;

        private OptionsBuilder() {
            // Only accessible via AsyncDaoFactory.optionsBuilder()
        }

        public OptionsBuilder setTableName(final String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Options build() {
            return new Options(tableName);
        }
    }
}
