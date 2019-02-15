package com.bettercloud.bigtable.orm;

public abstract class RegisterableEntity implements Entity {

    @SuppressWarnings("WeakerAccess") // Used by generated Entities
    protected static <T extends Entity> void register(final EntityConfiguration<T> entityConfiguration,
                                                      final Class<T> type) {
        EntityRegistry.register(entityConfiguration, type);
    }
}
