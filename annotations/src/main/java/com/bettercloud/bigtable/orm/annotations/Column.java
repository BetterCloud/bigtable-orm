package com.bettercloud.bigtable.orm.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.SOURCE)
public @interface Column {

  /**
   * Required.
   *
   * @return The column family
   */
  String family();

  /**
   * When undefined, then the name of the annotated field is used.
   *
   * @return The column qualifier
   */
  String qualifier() default "";

  /**
   * When true, then additional getters and setters will be generated for maintaining the column's
   * version.
   *
   * @return Whether the column should support versioning
   */
  boolean versioned() default false;
}
