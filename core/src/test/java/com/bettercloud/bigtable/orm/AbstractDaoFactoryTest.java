package com.bettercloud.bigtable.orm;

import java.util.function.Supplier;

abstract class AbstractDaoFactoryTest {

  static final String TABLE_NAME = "table_name";

  static class UnregisteredEntity implements Entity {

    static class TestConfiguration implements EntityConfiguration<UnregisteredEntity> {

      static final EntityConfiguration<UnregisteredEntity> INSTANCE =
          new UnregisteredEntity.TestConfiguration();

      @Override
      public String getDefaultTableName() {
        return TABLE_NAME;
      }

      @Override
      public Iterable<Column> getColumns() {
        return null;
      }

      @Override
      public Supplier<UnregisteredEntity> getEntityFactory() {
        return null;
      }

      @Override
      public EntityDelegate<UnregisteredEntity> getDelegateForEntity(
          final UnregisteredEntity entity) {
        return null;
      }
    }
  }

  static class RegisteredEntity extends RegisterableEntity {

    static {
      register(TestConfiguration.INSTANCE, RegisteredEntity.class);
    }

    static class TestConfiguration implements EntityConfiguration<RegisteredEntity> {

      static final EntityConfiguration<RegisteredEntity> INSTANCE = new TestConfiguration();

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
