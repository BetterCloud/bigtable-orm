package com.bettercloud.bigtable.orm.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE_PARAMETER)
@Retention(RetentionPolicy.SOURCE)
public @interface KeyComponent {

    /**
     * The name of the key component, used to generate the method name for the corresponding step in the KeyBuilder.
     *
     * Must be defined if {@link #constant()} is not set.
     *
     * Ignored when {@link #constant()} is set.
     *
     * @return The key component name
     */
    String name() default "";

    /**
     * The type of the key component, used as the method parameter for the corresponding step in the KeyBuilder.
     *
     * Ignored when {@link #constant()} is set.
     *
     * @return The key component type
     */
    Class<?> type() default String.class;

    /**
     * Used to define a key component that should never vary (such as an entity type).
     *
     * Must be defined if {@link #name()} is not set.
     *
     * When defined, then {@link #name()} and {@link #type()} are ignored.
     *
     * @return A constant that should be used in the position of this key component
     */
    String constant() default "";
}
