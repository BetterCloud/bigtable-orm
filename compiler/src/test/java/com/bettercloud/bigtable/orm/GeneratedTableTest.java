package com.bettercloud.bigtable.orm;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.bettercloud.bigtable.orm.annotations.Column;
import com.bettercloud.bigtable.orm.annotations.Entity;
import com.bettercloud.bigtable.orm.annotations.KeyComponent;
import com.bettercloud.bigtable.orm.annotations.Table;
import org.junit.Test;

public class GeneratedTableTest {

  @Test
  public void testTableEntityConfigurationContainsTableName() {
    final EntityConfiguration<TableEntity> entityConfiguration =
        EntityRegistry.getConfigurationForType(TableEntity.class);

    assertNotNull(entityConfiguration);

    assertEquals(TableConfiguration.TABLE_NAME, entityConfiguration.getDefaultTableName());
  }

  @Table(TableConfiguration.TABLE_NAME)
  private class TableConfiguration {

    private static final String TABLE_NAME = "table";

    @Entity(keyComponents = {@KeyComponent(constant = "constant")})
    private class TableEntity {

      @Column(family = "family")
      private String value;
    }
  }
}
