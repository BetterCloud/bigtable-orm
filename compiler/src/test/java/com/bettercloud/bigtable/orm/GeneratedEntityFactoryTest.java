package com.bettercloud.bigtable.orm;

import static org.junit.Assert.assertNotNull;

import com.bettercloud.bigtable.orm.annotations.Column;
import com.bettercloud.bigtable.orm.annotations.Entity;
import com.bettercloud.bigtable.orm.annotations.KeyComponent;
import com.bettercloud.bigtable.orm.annotations.Table;
import java.util.function.Supplier;
import org.junit.Test;

public class GeneratedEntityFactoryTest {

  @Test
  public void testEntityFactoryEntityConfigurationContainsEntityFactory() {
    final EntityConfiguration<EntityFactoryEntity> entityConfiguration =
        EntityRegistry.getConfigurationForType(EntityFactoryEntity.class);

    assertNotNull(entityConfiguration);

    final Supplier<EntityFactoryEntity> entityFactory = entityConfiguration.getEntityFactory();

    assertNotNull(entityFactory);

    final EntityFactoryEntity result = entityFactory.get();

    assertNotNull(result);
  }

  @Table("entity_factory_table")
  private class EntityFactoryTableConfiguration {

    @Entity(keyComponents = {@KeyComponent(constant = "constant")})
    private class EntityFactoryEntity {

      @Column(family = "family")
      private String stringValue;
    }
  }
}
