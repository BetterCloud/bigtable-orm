package com.bettercloud.bigtable.orm;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.bettercloud.bigtable.orm.annotations.Column;
import com.bettercloud.bigtable.orm.annotations.Entity;
import com.bettercloud.bigtable.orm.annotations.KeyComponent;
import com.bettercloud.bigtable.orm.annotations.Table;
import java.util.stream.StreamSupport;
import org.junit.Test;

public class GeneratedEntityDelegateTest {

  @Test
  public void testEntityDelegateEntityConfigurationContainsFunctionalEntityDelegate() {
    final EntityConfiguration<EntityDelegateEntity> entityConfiguration =
        EntityRegistry.getConfigurationForType(EntityDelegateEntity.class);

    assertNotNull(entityConfiguration);

    final EntityDelegateEntity entity = new EntityDelegateEntity();

    final EntityConfiguration.EntityDelegate<EntityDelegateEntity> delegate =
        entityConfiguration.getDelegateForEntity(entity);

    final Iterable<? extends com.bettercloud.bigtable.orm.Column> columns =
        entityConfiguration.getColumns();

    final com.bettercloud.bigtable.orm.Column stringValueColumn =
        StreamSupport.stream(columns.spliterator(), false)
            .filter(
                column ->
                    EntityDelegateTableConfiguration.EntityDelegateEntity.COLUMN_FAMILY_1.equals(
                        column.getFamily()))
            .filter(
                column ->
                    EntityDelegateTableConfiguration.EntityDelegateEntity.COLUMN_QUALIFIER_1.equals(
                        column.getQualifier()))
            .filter(column -> String.class.equals(column.getTypeReference().getType()))
            .filter(column -> !column.isVersioned())
            .findFirst()
            .orElseThrow(IllegalStateException::new);

    assertNull(entity.getStringValue());
    assertNull(delegate.getColumnValue(stringValueColumn));

    final String stringValue = "hello";
    delegate.setColumnValue(stringValueColumn, stringValue);

    assertEquals(stringValue, entity.getStringValue());
    assertEquals(stringValue, delegate.getColumnValue(stringValueColumn));

    final com.bettercloud.bigtable.orm.Column booleanValueColumn =
        StreamSupport.stream(columns.spliterator(), false)
            .filter(
                column ->
                    EntityDelegateTableConfiguration.EntityDelegateEntity.COLUMN_FAMILY_2.equals(
                        column.getFamily()))
            .filter(
                column ->
                    EntityDelegateTableConfiguration.EntityDelegateEntity.COLUMN_QUALIFIER_2.equals(
                        column.getQualifier()))
            .filter(column -> Boolean.class.equals(column.getTypeReference().getType()))
            .filter(column -> !column.isVersioned())
            .findFirst()
            .orElseThrow(IllegalStateException::new);

    assertNull(entity.getBooleanValue());
    assertNull(delegate.getColumnValue(booleanValueColumn));

    final Boolean booleanValue = false;
    delegate.setColumnValue(booleanValueColumn, booleanValue);

    assertEquals(booleanValue, entity.getBooleanValue());
    assertEquals(booleanValue, delegate.getColumnValue(booleanValueColumn));
  }

  @Test(expected = ClassCastException.class)
  public void testEntityDelegateThrowsClassCastExceptionWhenSettingColumnValueToIncorrectType() {
    final EntityConfiguration<EntityDelegateEntity> entityConfiguration =
        EntityRegistry.getConfigurationForType(EntityDelegateEntity.class);

    assertNotNull(entityConfiguration);

    final EntityDelegateEntity entity = new EntityDelegateEntity();

    final EntityConfiguration.EntityDelegate<EntityDelegateEntity> delegate =
        entityConfiguration.getDelegateForEntity(entity);

    final Iterable<? extends com.bettercloud.bigtable.orm.Column> columns =
        entityConfiguration.getColumns();

    final com.bettercloud.bigtable.orm.Column stringValueColumn =
        StreamSupport.stream(columns.spliterator(), false)
            .filter(
                column ->
                    EntityDelegateTableConfiguration.EntityDelegateEntity.COLUMN_FAMILY_1.equals(
                        column.getFamily()))
            .filter(
                column ->
                    EntityDelegateTableConfiguration.EntityDelegateEntity.COLUMN_QUALIFIER_1.equals(
                        column.getQualifier()))
            .filter(column -> String.class.equals(column.getTypeReference().getType()))
            .filter(column -> !column.isVersioned())
            .findFirst()
            .orElseThrow(IllegalStateException::new);

    assertNull(entity.getStringValue());
    assertNull(delegate.getColumnValue(stringValueColumn));

    final Integer wrongType = 7;
    delegate.setColumnValue(stringValueColumn, wrongType);
  }

  @Test(expected = IllegalArgumentException.class)
  public void
      testEntityDelegateThrowsIllegalArgumentExceptionWhenSettingTimestampForUnversionedColumn() {
    final EntityConfiguration<EntityDelegateEntity> entityConfiguration =
        EntityRegistry.getConfigurationForType(EntityDelegateEntity.class);

    assertNotNull(entityConfiguration);

    final EntityDelegateEntity entity = new EntityDelegateEntity();

    final EntityConfiguration.EntityDelegate<EntityDelegateEntity> delegate =
        entityConfiguration.getDelegateForEntity(entity);

    final Iterable<? extends com.bettercloud.bigtable.orm.Column> columns =
        entityConfiguration.getColumns();

    final com.bettercloud.bigtable.orm.Column stringValueColumn =
        StreamSupport.stream(columns.spliterator(), false)
            .filter(
                column ->
                    EntityDelegateTableConfiguration.EntityDelegateEntity.COLUMN_FAMILY_1.equals(
                        column.getFamily()))
            .filter(
                column ->
                    EntityDelegateTableConfiguration.EntityDelegateEntity.COLUMN_QUALIFIER_1.equals(
                        column.getQualifier()))
            .filter(column -> String.class.equals(column.getTypeReference().getType()))
            .filter(column -> !column.isVersioned())
            .findFirst()
            .orElseThrow(IllegalStateException::new);

    delegate.setColumnTimestamp(stringValueColumn, 1234L);
  }

  @Test(expected = IllegalArgumentException.class)
  public void
      testEntityDelegateThrowsIllegalArgumentExceptionWhenGettingTimestampForUnversionedColumn() {
    final EntityConfiguration<EntityDelegateEntity> entityConfiguration =
        EntityRegistry.getConfigurationForType(EntityDelegateEntity.class);

    assertNotNull(entityConfiguration);

    final EntityDelegateEntity entity = new EntityDelegateEntity();

    final EntityConfiguration.EntityDelegate<EntityDelegateEntity> delegate =
        entityConfiguration.getDelegateForEntity(entity);

    final Iterable<? extends com.bettercloud.bigtable.orm.Column> columns =
        entityConfiguration.getColumns();

    final com.bettercloud.bigtable.orm.Column stringValueColumn =
        StreamSupport.stream(columns.spliterator(), false)
            .filter(
                column ->
                    EntityDelegateTableConfiguration.EntityDelegateEntity.COLUMN_FAMILY_1.equals(
                        column.getFamily()))
            .filter(
                column ->
                    EntityDelegateTableConfiguration.EntityDelegateEntity.COLUMN_QUALIFIER_1.equals(
                        column.getQualifier()))
            .filter(column -> String.class.equals(column.getTypeReference().getType()))
            .filter(column -> !column.isVersioned())
            .findFirst()
            .orElseThrow(IllegalStateException::new);

    delegate.getColumnTimestamp(stringValueColumn);
  }

  @Test
  public void testEntityDelegateSetsTimestampOnEntityForVersionedColumn() {
    final EntityConfiguration<EntityDelegateEntity> entityConfiguration =
        EntityRegistry.getConfigurationForType(EntityDelegateEntity.class);

    assertNotNull(entityConfiguration);

    final EntityDelegateEntity entity = new EntityDelegateEntity();

    final EntityConfiguration.EntityDelegate<EntityDelegateEntity> delegate =
        entityConfiguration.getDelegateForEntity(entity);

    final Iterable<? extends com.bettercloud.bigtable.orm.Column> columns =
        entityConfiguration.getColumns();

    final com.bettercloud.bigtable.orm.Column intValueColumn =
        StreamSupport.stream(columns.spliterator(), false)
            .filter(
                column ->
                    EntityDelegateTableConfiguration.EntityDelegateEntity.COLUMN_FAMILY_3.equals(
                        column.getFamily()))
            .filter(
                column ->
                    EntityDelegateTableConfiguration.EntityDelegateEntity.COLUMN_QUALIFIER_3.equals(
                        column.getQualifier()))
            .filter(column -> Integer.class.equals(column.getTypeReference().getType()))
            .filter(com.bettercloud.bigtable.orm.Column::isVersioned)
            .findFirst()
            .orElseThrow(IllegalStateException::new);

    assertNull(entity.getIntValueTimestamp());
    assertNull(delegate.getColumnTimestamp(intValueColumn));

    final long timestamp = 1234L;
    delegate.setColumnTimestamp(intValueColumn, timestamp);

    assertEquals(timestamp, (long) entity.getIntValueTimestamp());
    assertEquals(timestamp, (long) delegate.getColumnTimestamp(intValueColumn));
  }

  @Table("entity_delegate_table")
  private class EntityDelegateTableConfiguration {

    @Entity(keyComponents = {@KeyComponent(constant = "constant")})
    private class EntityDelegateEntity {

      private static final String COLUMN_FAMILY_1 = "cf-1";
      private static final String COLUMN_QUALIFIER_1 = "cq-1";

      private static final String COLUMN_FAMILY_2 = "cf-2";
      private static final String COLUMN_QUALIFIER_2 = "cq-2";

      private static final String COLUMN_FAMILY_3 = "cf-3";
      private static final String COLUMN_QUALIFIER_3 = "cq-3";

      @Column(family = COLUMN_FAMILY_1, qualifier = COLUMN_QUALIFIER_1)
      private String stringValue;

      @Column(family = COLUMN_FAMILY_2, qualifier = COLUMN_QUALIFIER_2)
      private Boolean booleanValue;

      @Column(family = COLUMN_FAMILY_3, qualifier = COLUMN_QUALIFIER_3, versioned = true)
      private int intValue;
    }
  }
}
