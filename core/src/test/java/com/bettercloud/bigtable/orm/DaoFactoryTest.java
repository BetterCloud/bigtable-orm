package com.bettercloud.bigtable.orm;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.io.IOException;
import java.util.function.Supplier;

import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class DaoFactoryTest {

    private static final String TABLE_NAME = "table_name";

    @Mock
    private Connection connection;

    private DaoFactory daoFactory;

    @Before
    public void setup() {
        initMocks(this);

        daoFactory = new DaoFactory(connection);
    }

    @Test(expected = NullPointerException.class)
    public void testDaoForNullEntityTypeThrowsNullPointerException() throws IOException {
        daoFactory.daoFor(null);
    }

    @Test(expected = IllegalStateException.class)
    public void testDaoForUnregisteredEntityTypeThrowsIllegalStateException() throws IOException {
        daoFactory.daoFor(UnregisteredEntity.class);
    }

    @Test
    public void testDaoForRegisteredEntityTypeReturnsDaoForEntity() throws IOException {
        final Table table = mock(Table.class);
        when(connection.getTable(TableName.valueOf(TABLE_NAME))).thenReturn(table);

        final Dao<RegisteredEntity> registeredEntityDao = daoFactory.daoFor(RegisteredEntity.class);

        assertNotNull(registeredEntityDao);

        verify(connection).getTable(eq(TableName.valueOf(TABLE_NAME)));
    }

    @Test
    public void testDaoForRegisteredEntityTypeWithNullOptionsReturnsDaoForEntityUsingDefaultTableName() throws IOException {
        final DaoFactory.Options options = null;

        final Table table = mock(Table.class);
        when(connection.getTable(TableName.valueOf(TABLE_NAME))).thenReturn(table);

        final Dao<RegisteredEntity> registeredEntityDao = daoFactory.daoFor(RegisteredEntity.class, options);

        assertNotNull(registeredEntityDao);

        verify(connection).getTable(eq(TableName.valueOf(TABLE_NAME)));
    }

    @Test
    public void testDaoForRegisteredEntityTypeWithNullTableNameReturnsDaoForEntityUsingDefaultTableName() throws IOException {
        final String tableName = null;

        final DaoFactory.Options options = DaoFactory.optionsBuilder()
                .setTableName(tableName)
                .build();

        final Table table = mock(Table.class);
        when(connection.getTable(TableName.valueOf(TABLE_NAME))).thenReturn(table);

        final Dao<RegisteredEntity> registeredEntityDao = daoFactory.daoFor(RegisteredEntity.class, options);

        assertNotNull(registeredEntityDao);

        verify(connection).getTable(eq(TableName.valueOf(TABLE_NAME)));
    }

    @Test
    public void testDaoForRegisteredEntityTypeWithCustomTableNameReturnsDaoForEntityUsingDefinedTableName() throws IOException {
        final String tableName = "a_different_table_name";

        final DaoFactory.Options options = DaoFactory.optionsBuilder()
                .setTableName(tableName)
                .build();

        final Table table = mock(Table.class);
        when(connection.getTable(TableName.valueOf(tableName))).thenReturn(table);

        final Dao<RegisteredEntity> registeredEntityDao = daoFactory.daoFor(RegisteredEntity.class, options);

        assertNotNull(registeredEntityDao);

        verify(connection).getTable(eq(TableName.valueOf(tableName)));
    }

    private static class UnregisteredEntity implements Entity {
        // Nothing to see here
    }

    private static class RegisteredEntity extends RegisterableEntity {

        static {
            register(TestConfiguration.INSTANCE, RegisteredEntity.class);
        }

        private static class TestConfiguration implements EntityConfiguration<RegisteredEntity> {

            private static final EntityConfiguration<RegisteredEntity> INSTANCE = new TestConfiguration();

            @Override
            public String getDefaultTableName() {
                return TABLE_NAME;
            }

            @Override
            public Iterable<Column> getColumns() {
                return null;
            }

            @Override
            public Supplier<RegisteredEntity> getEntityFactory() {
                return null;
            }

            @Override
            public EntityDelegate<RegisteredEntity> getDelegateForEntity(final RegisteredEntity entity) {
                return null;
            }
        }
    }
}
