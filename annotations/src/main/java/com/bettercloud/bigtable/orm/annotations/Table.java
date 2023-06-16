package com.bettercloud.bigtable.orm.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.SOURCE)
public @interface Table {

  /**
   * The default name of the table in which all entities defined under the annotated type will
   * reside.
   *
   * <p>Required.
   *
   * @return The table name
   */
  String value();
}
