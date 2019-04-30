package com.bettercloud.bigtable.orm;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

public class DaoFactory {

    private final Connection connection;

    public DaoFactory(final String projectId, final String instanceId) {
        this(BigtableConfiguration.connect(projectId, instanceId));
    }

    @SuppressWarnings("WeakerAccess") // Public API
    public DaoFactory(final Connection connection) {
        this.connection = connection;
    }

    @SuppressWarnings("WeakerAccess") // Public API
    public <T extends Entity> Dao<T> daoFor(final Class<T> entityType, final Options options) throws IOException {
        Objects.requireNonNull(entityType);

        final EntityConfiguration<T> entityConfiguration = EntityRegistry.getConfigurationForType(entityType);

        return daoFor(entityConfiguration, options);
    }

    @SuppressWarnings("WeakerAccess") // Public API
    public <T extends Entity> Dao<T> daoFor(final Class<T> entityType) throws IOException {
        return daoFor(entityType, null);
    }

    @SuppressWarnings("WeakerAccess") // Public API
    public <T extends Entity> Dao<T> daoFor(final EntityConfiguration<T> entityConfiguration, final Options options) throws IOException {
        Objects.requireNonNull(entityConfiguration);

        final String resolvedTableName = Optional.ofNullable(options)
                                                 .map(Options::getTableName)
                                                 .orElseGet(entityConfiguration::getDefaultTableName);

        final TableName hbaseTableName = TableName.valueOf(resolvedTableName);

        final Table table = connection.getTable(hbaseTableName);
        final Iterable<? extends Column> columns = entityConfiguration.getColumns();
        final Supplier<T> entityFactory = entityConfiguration.getEntityFactory();
        final Function<T, EntityConfiguration.EntityDelegate<T>> delegateFactory = entityConfiguration::getDelegateForEntity;

        return new BigTableEntityDao<>(table, columns, entityFactory, delegateFactory);
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
            // Only accessible via DaoFactory.optionsBuilder()
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
