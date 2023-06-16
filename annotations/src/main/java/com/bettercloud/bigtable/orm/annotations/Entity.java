package com.bettercloud.bigtable.orm.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.SOURCE)
public @interface Entity {

  /**
   * @return The delimiter used to join the {@link #keyComponents()} for all entites of the
   *     annotated type.
   */
  String keyDelimiter() default "::";

  /**
   * An array of {@link KeyComponent}s that will be used to generate a KeyBuilder that always
   * generates a suitable key for this entity.
   *
   * <p>At least one {@link KeyComponent} must be defined.
   *
   * <p>It is up to the programmer to define key formats that will not collide with one another.
   * This annotation is constructed for easy readability for code reviews.
   *
   * @return An array of key components
   */
  KeyComponent[] keyComponents();
}
