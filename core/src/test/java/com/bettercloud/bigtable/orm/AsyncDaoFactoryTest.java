package com.bettercloud.bigtable.orm;

import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AdvancedScanResultConsumer;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class AsyncDaoFactoryTest extends AbstractDaoFactoryTest {

  @Mock private AsyncConnection connection;

  @Mock private AsyncTable<AdvancedScanResultConsumer> table;

  private AsyncDaoFactory asyncDaoFactory;

  @Before
  public void setup() {
    initMocks(this);

    asyncDaoFactory = new AsyncDaoFactory(connection);
  }

  @Test(expected = NullPointerException.class)
  public void testDaoForNullEntityTypeThrowsNullPointerException() {
    asyncDaoFactory.daoFor(null);
  }

  @Test(expected = IllegalStateException.class)
  public void testDaoForUnregisteredEntityTypeThrowsIllegalStateException() {
    asyncDaoFactory.daoFor(UnregisteredEntity.class);
  }

  @Test
  public void testDaoForRegisteredEntityTypeReturnsDaoForEntity() {
    when(connection.getTable(TableName.valueOf(TABLE_NAME))).thenReturn(table);

    final AsyncDao<RegisteredEntity> registeredEntityDao =
        asyncDaoFactory.daoFor(RegisteredEntity.class);

    assertNotNull(registeredEntityDao);

    verify(connection).getTable(eq(TableName.valueOf(TABLE_NAME)));
  }

  @Test
  public void
      testDaoForRegisteredEntityTypeWithNullOptionsReturnsDaoForEntityUsingDefaultTableName() {
    final AsyncDaoFactory.Options options = null;

    when(connection.getTable(TableName.valueOf(TABLE_NAME))).thenReturn(table);

    final AsyncDao<RegisteredEntity> registeredEntityDao =
        asyncDaoFactory.daoFor(RegisteredEntity.class, options);

    assertNotNull(registeredEntityDao);

    verify(connection).getTable(eq(TableName.valueOf(TABLE_NAME)));
  }

  @Test
  public void
      testDaoForRegisteredEntityTypeWithNullTableNameReturnsDaoForEntityUsingDefaultTableName() {
    final String tableName = null;

    final AsyncDaoFactory.Options options =
        AsyncDaoFactory.optionsBuilder().setTableName(tableName).build();

    when(connection.getTable(TableName.valueOf(TABLE_NAME))).thenReturn(table);

    final AsyncDao<RegisteredEntity> registeredEntityDao =
        asyncDaoFactory.daoFor(RegisteredEntity.class, options);

    assertNotNull(registeredEntityDao);

    verify(connection).getTable(eq(TableName.valueOf(TABLE_NAME)));
  }

  @Test
  public void
      testDaoForRegisteredEntityTypeWithCustomTableNameReturnsDaoForEntityUsingDefinedTableName() {
    final String tableName = "a_different_table_name";

    final AsyncDaoFactory.Options options =
        AsyncDaoFactory.optionsBuilder().setTableName(tableName).build();

    when(connection.getTable(TableName.valueOf(tableName))).thenReturn(table);

    final AsyncDao<RegisteredEntity> registeredEntityDao =
        asyncDaoFactory.daoFor(RegisteredEntity.class, options);

    assertNotNull(registeredEntityDao);

    verify(connection).getTable(eq(TableName.valueOf(tableName)));
  }

  @Test
  public void testDaoForUnRegisteredEntityTypeReturnsDaoForEntityConfiguration() {
    when(connection.getTable(TableName.valueOf(TABLE_NAME))).thenReturn(table);

    final AsyncDao<UnregisteredEntity> unregisteredEntityDao =
        asyncDaoFactory.daoFor(UnregisteredEntity.TestConfiguration.INSTANCE, null);

    assertNotNull(unregisteredEntityDao);

    verify(connection).getTable(eq(TableName.valueOf(TABLE_NAME)));
  }
}
