package com.bettercloud.bigtable.orm;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.bettercloud.bigtable.orm.annotations.Column;
import com.bettercloud.bigtable.orm.annotations.Entity;
import com.bettercloud.bigtable.orm.annotations.KeyComponent;
import com.bettercloud.bigtable.orm.annotations.Table;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.junit.Test;

public class GeneratedColumnTest {

  @Test
  public void testSingleColumnEntityConfigurationContainsOneColumn() {
    final EntityConfiguration<SingleColumnEntity> entityConfiguration =
        EntityRegistry.getConfigurationForType(SingleColumnEntity.class);

    assertNotNull(entityConfiguration);

    final Iterable<? extends com.bettercloud.bigtable.orm.Column> result =
        entityConfiguration.getColumns();

    assertNotNull(result);
    assertEquals(1, StreamSupport.stream(result.spliterator(), false).count());
  }

  @Test
  public void
      testSingleColumnEntityConfigurationContainsColumnWithMatchingFamilyAndQualifierAndTypeReferenceAndVersioning() {
    final EntityConfiguration<SingleColumnEntity> entityConfiguration =
        EntityRegistry.getConfigurationForType(SingleColumnEntity.class);

    assertNotNull(entityConfiguration);

    final Iterable<? extends com.bettercloud.bigtable.orm.Column> columns =
        entityConfiguration.getColumns();

    final List<com.bettercloud.bigtable.orm.Column> results =
        StreamSupport.stream(columns.spliterator(), false)
            .filter(
                column ->
                    EntityConfigurationTableConfiguration.SingleColumnEntity.COLUMN_FAMILY.equals(
                        column.getFamily()))
            .filter(
                column ->
                    EntityConfigurationTableConfiguration.SingleColumnEntity.COLUMN_QUALIFIER
                        .equals(column.getQualifier()))
            .filter(column -> String.class.equals(column.getTypeReference().getType()))
            .filter(column -> !column.isVersioned())
            .collect(Collectors.toList());

    assertNotNull(results);
    assertEquals(1, results.size());
  }

  @Test
  public void
      testInferredColumnQualifierEntityConfigurationContainsColumnWithDefinedFamilyAndTypeReferenceAndVersioningWithFieldNameAsQualifier() {
    final EntityConfiguration<InferredColumnQualifierEntity> entityConfiguration =
        EntityRegistry.getConfigurationForType(InferredColumnQualifierEntity.class);

    assertNotNull(entityConfiguration);

    final Iterable<? extends com.bettercloud.bigtable.orm.Column> columns =
        entityConfiguration.getColumns();

    final List<com.bettercloud.bigtable.orm.Column> results =
        StreamSupport.stream(columns.spliterator(), false)
            .filter(
                column ->
                    EntityConfigurationTableConfiguration.InferredColumnQualifierEntity
                        .COLUMN_FAMILY
                        .equals(column.getFamily()))
            .filter(column -> "column".equals(column.getQualifier()))
            .filter(column -> String.class.equals(column.getTypeReference().getType()))
            .filter(column -> !column.isVersioned())
            .collect(Collectors.toList());

    assertNotNull(results);
    assertEquals(1, results.size());
  }

  @Test
  public void
      testMultiColumnEntityConfigurationContainsColumnWithMatchingFamilyAndQualifierAndTypeReferenceAndVersioningForAllFields() {
    final EntityConfiguration<MultiColumnEntity> entityConfiguration =
        EntityRegistry.getConfigurationForType(MultiColumnEntity.class);

    assertNotNull(entityConfiguration);

    final Iterable<? extends com.bettercloud.bigtable.orm.Column> columns =
        entityConfiguration.getColumns();

    final List<com.bettercloud.bigtable.orm.Column> results1 =
        StreamSupport.stream(columns.spliterator(), false)
            .filter(
                column ->
                    EntityConfigurationTableConfiguration.MultiColumnEntity.COLUMN_FAMILY_1.equals(
                        column.getFamily()))
            .filter(
                column ->
                    EntityConfigurationTableConfiguration.MultiColumnEntity.COLUMN_QUALIFIER_1
                        .equals(column.getQualifier()))
            .filter(column -> String.class.equals(column.getTypeReference().getType()))
            .filter(column -> !column.isVersioned())
            .collect(Collectors.toList());

    assertNotNull(results1);
    assertEquals(1, results1.size());

    final List<com.bettercloud.bigtable.orm.Column> results2 =
        StreamSupport.stream(columns.spliterator(), false)
            .filter(
                column ->
                    EntityConfigurationTableConfiguration.MultiColumnEntity.COLUMN_FAMILY_2.equals(
                        column.getFamily()))
            .filter(
                column ->
                    EntityConfigurationTableConfiguration.MultiColumnEntity.COLUMN_QUALIFIER_2
                        .equals(column.getQualifier()))
            .filter(column -> Integer.class.equals(column.getTypeReference().getType()))
            .filter(column -> !column.isVersioned())
            .collect(Collectors.toList());

    assertNotNull(results2);
    assertEquals(1, results2.size());
  }

  @Test
  public void
      testPrimitiveColumnEntityConfigurationContainsColumnWithDefinedFamilyAndQualifierAndVersioningWithBoxedTypeReference() {
    final EntityConfiguration<PrimitiveColumnEntity> entityConfiguration =
        EntityRegistry.getConfigurationForType(PrimitiveColumnEntity.class);

    assertNotNull(entityConfiguration);

    final Iterable<? extends com.bettercloud.bigtable.orm.Column> columns =
        entityConfiguration.getColumns();

    final List<com.bettercloud.bigtable.orm.Column> results =
        StreamSupport.stream(columns.spliterator(), false)
            .filter(
                column ->
                    EntityConfigurationTableConfiguration.PrimitiveColumnEntity.COLUMN_FAMILY
                        .equals(column.getFamily()))
            .filter(
                column ->
                    EntityConfigurationTableConfiguration.PrimitiveColumnEntity.COLUMN_QUALIFIER
                        .equals(column.getQualifier()))
            .filter(column -> Boolean.class.equals(column.getTypeReference().getType()))
            .filter(column -> !column.isVersioned())
            .collect(Collectors.toList());

    assertNotNull(results);
    assertEquals(1, results.size());
  }

  @Test
  public void
      testVersionedColumnEntityConfigurationContainsColumnWithDefinedFamilyAndQualifierAndVersioning() {
    final EntityConfiguration<VersionedColumnEntity> entityConfiguration =
        EntityRegistry.getConfigurationForType(VersionedColumnEntity.class);

    assertNotNull(entityConfiguration);

    final Iterable<? extends com.bettercloud.bigtable.orm.Column> columns =
        entityConfiguration.getColumns();

    final List<com.bettercloud.bigtable.orm.Column> results =
        StreamSupport.stream(columns.spliterator(), false)
            .filter(
                column ->
                    EntityConfigurationTableConfiguration.VersionedColumnEntity.COLUMN_FAMILY
                        .equals(column.getFamily()))
            .filter(
                column ->
                    EntityConfigurationTableConfiguration.VersionedColumnEntity.COLUMN_QUALIFIER
                        .equals(column.getQualifier()))
            .filter(column -> Boolean.class.equals(column.getTypeReference().getType()))
            .filter(com.bettercloud.bigtable.orm.Column::isVersioned)
            .collect(Collectors.toList());

    assertNotNull(results);
    assertEquals(1, results.size());
  }

  @Test
  public void
      testMultiVersionedColumnEntityConfigurationContainsColumnsWithDefinedFamilyAndQualifierAndVersioning() {
    final EntityConfiguration<MultiVersionedColumnEntity> entityConfiguration =
        EntityRegistry.getConfigurationForType(MultiVersionedColumnEntity.class);

    assertNotNull(entityConfiguration);

    final Iterable<? extends com.bettercloud.bigtable.orm.Column> columns =
        entityConfiguration.getColumns();

    final List<com.bettercloud.bigtable.orm.Column> results1 =
        StreamSupport.stream(columns.spliterator(), false)
            .filter(
                column ->
                    EntityConfigurationTableConfiguration.MultiVersionedColumnEntity.COLUMN_FAMILY_1
                        .equals(column.getFamily()))
            .filter(
                column ->
                    EntityConfigurationTableConfiguration.MultiVersionedColumnEntity
                        .COLUMN_QUALIFIER_1
                        .equals(column.getQualifier()))
            .filter(column -> String.class.equals(column.getTypeReference().getType()))
            .filter(com.bettercloud.bigtable.orm.Column::isVersioned)
            .collect(Collectors.toList());

    assertNotNull(results1);
    assertEquals(1, results1.size());

    final List<com.bettercloud.bigtable.orm.Column> results2 =
        StreamSupport.stream(columns.spliterator(), false)
            .filter(
                column ->
                    EntityConfigurationTableConfiguration.MultiVersionedColumnEntity.COLUMN_FAMILY_2
                        .equals(column.getFamily()))
            .filter(
                column ->
                    EntityConfigurationTableConfiguration.MultiVersionedColumnEntity
                        .COLUMN_QUALIFIER_2
                        .equals(column.getQualifier()))
            .filter(column -> Integer.class.equals(column.getTypeReference().getType()))
            .filter(com.bettercloud.bigtable.orm.Column::isVersioned)
            .collect(Collectors.toList());

    assertNotNull(results2);
    assertEquals(1, results2.size());
  }

  @Table("column_table")
  private class EntityConfigurationTableConfiguration {

    @Entity(keyComponents = {@KeyComponent(constant = "constant")})
    private class SingleColumnEntity {

      private static final String COLUMN_FAMILY = "my_column_family";
      private static final String COLUMN_QUALIFIER = "my_column_qualifier";

      @Column(family = COLUMN_FAMILY, qualifier = COLUMN_QUALIFIER)
      private String column;
    }

    @Entity(keyComponents = {@KeyComponent(constant = "constant")})
    private class InferredColumnQualifierEntity {

      private static final String COLUMN_FAMILY = "my_column_family";

      @Column(family = COLUMN_FAMILY)
      private String column;
    }

    @Entity(keyComponents = {@KeyComponent(constant = "constant")})
    private class MultiColumnEntity {

      private static final String COLUMN_FAMILY_1 = "family_1";
      private static final String COLUMN_QUALIFIER_1 = "qualifier_1";

      private static final String COLUMN_FAMILY_2 = "family_2";
      private static final String COLUMN_QUALIFIER_2 = "qualifier_2";

      @Column(family = COLUMN_FAMILY_1, qualifier = COLUMN_QUALIFIER_1)
      private String column1;

      @Column(family = COLUMN_FAMILY_2, qualifier = COLUMN_QUALIFIER_2)
      private Integer column2;
    }

    @Entity(keyComponents = {@KeyComponent(constant = "constant")})
    private class PrimitiveColumnEntity {

      private static final String COLUMN_FAMILY = "my_column_family";
      private static final String COLUMN_QUALIFIER = "my_column_qualifier";

      @Column(family = COLUMN_FAMILY, qualifier = COLUMN_QUALIFIER)
      private boolean column;
    }

    @Entity(keyComponents = {@KeyComponent(constant = "constant")})
    private class VersionedColumnEntity {

      private static final String COLUMN_FAMILY = "my_column_family";
      private static final String COLUMN_QUALIFIER = "my_column_qualifier";

      @Column(family = COLUMN_FAMILY, qualifier = COLUMN_QUALIFIER, versioned = true)
      private boolean column;
    }

    @Entity(keyComponents = {@KeyComponent(constant = "constant")})
    private class MultiVersionedColumnEntity {

      private static final String COLUMN_FAMILY_1 = "family_1";
      private static final String COLUMN_QUALIFIER_1 = "qualifier_1";

      private static final String COLUMN_FAMILY_2 = "family_2";
      private static final String COLUMN_QUALIFIER_2 = "qualifier_2";

      @Column(family = COLUMN_FAMILY_1, qualifier = COLUMN_QUALIFIER_1, versioned = true)
      private String column1;

      @Column(family = COLUMN_FAMILY_2, qualifier = COLUMN_QUALIFIER_2, versioned = true)
      private Integer column2;
    }
  }
}
