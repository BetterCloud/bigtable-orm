package com.bettercloud.bigtable.orm;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class EntityRegistry {

  private static final Map<Class<?>, EntityConfiguration<?>> ENTITY_CONFIGURATIONS =
      new ConcurrentHashMap<>();

  static <T extends Entity> void register(
      final EntityConfiguration<T> entityConfiguration, final Class<T> type) {
    ENTITY_CONFIGURATIONS.put(type, entityConfiguration);
  }

  @SuppressWarnings("unchecked") // Compile-time type checks happen via register()
  static <T extends Entity> EntityConfiguration<T> getConfigurationForType(final Class<T> type) {
    try {
      // Force class initialization, required for static block evaluation
      Class.forName(type.getName());
    } catch (final ClassNotFoundException e) {
      // This should never happen, but can if people are dumb
      throw new IllegalArgumentException("Class not found", e);
    }

    if (ENTITY_CONFIGURATIONS.containsKey(type)) {
      return (EntityConfiguration<T>) ENTITY_CONFIGURATIONS.get(type);
    } else {
      throw new IllegalStateException("Could not retrieve configuration for type " + type);
    }
  }
}
