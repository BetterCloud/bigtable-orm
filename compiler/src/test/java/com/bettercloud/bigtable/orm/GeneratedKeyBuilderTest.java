package com.bettercloud.bigtable.orm;

import com.bettercloud.bigtable.orm.annotations.Column;
import com.bettercloud.bigtable.orm.annotations.Entity;
import com.bettercloud.bigtable.orm.annotations.KeyComponent;
import com.bettercloud.bigtable.orm.annotations.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class GeneratedKeyBuilderTest {

    @Test
    public void testConstantKeyEntityKeyBuilderContainsNoCustomizationSteps() {
        final Key<ConstantKeyEntity> key = ConstantKeyEntity.keyBuilder().build();

        assertNotNull(key);

        assertEquals(KeyBuilderTableConfiguration.ConstantKeyEntity.CONSTANT_KEY_COMPONENT, key.toString());
        assertArrayEquals(Bytes.toBytes(KeyBuilderTableConfiguration.ConstantKeyEntity.CONSTANT_KEY_COMPONENT), key.toBytes());
    }

    @Test(expected = NullPointerException.class)
    public void testSingleKeyComponentEntityKeyBuilderWithNullValueThrowsNullPointerException() {
        SingleKeyComponentEntity.keyBuilder()
                .myValue(null)
                .build();
    }

    @Test
    public void testSingleKeyComponentEntityKeyBuilderBuildsKeyContainingDefinedValue() {
        final String value = "my key";

        final Key<SingleKeyComponentEntity> key = SingleKeyComponentEntity.keyBuilder()
                .myValue(value)
                .build();

        assertNotNull(key);

        assertEquals(value, key.toString());
        assertArrayEquals(Bytes.toBytes(value), key.toBytes());
    }

    @Test
    public void testMultiKeyComponentEntityKeyBuilderBuildsKeyJoiningAllPartsWithDefaultDelimiter() {
        final String first = "hello";
        final String second = "world";

        final Key<MultiKeyComponentEntity> key = MultiKeyComponentEntity.keyBuilder()
                .first(first)
                .second(second)
                .build();

        assertNotNull(key);

        final String expected = String.join("::", first, second);

        assertEquals(expected, key.toString());
        assertArrayEquals(Bytes.toBytes(expected), key.toBytes());
    }

    @Test
    public void testCustomKeyDelimiterEntityKeyBuilderBuildsKeyJoiningAllPartsWithCustomDelimiter() {
        final String first = "hello";
        final String second = "world";

        final Key<CustomKeyDelimiterEntity> key = CustomKeyDelimiterEntity.keyBuilder()
                .first(first)
                .second(second)
                .build();

        assertNotNull(key);

        final String expected = String.join(KeyBuilderTableConfiguration.CustomKeyDelimiterEntity.CUSTOM_DELIMITER, first, second);

        assertEquals(expected, key.toString());
        assertArrayEquals(Bytes.toBytes(expected), key.toBytes());
    }

    @Test
    public void testMultiKeyComponentEntityWithConstantKeyBuilderBuildsKeyJoiningAllParts() {
        final String first = "first";
        final String third = "third";

        final Key<MultiKeyComponentEntityWithConstant> key = MultiKeyComponentEntityWithConstant.keyBuilder()
                .first(first)
                .third(third)
                .build();

        assertNotNull(key);

        final String expected = String.join("::", first, KeyBuilderTableConfiguration.MultiKeyComponentEntityWithConstant.CONSTANT_KEY_COMPONENT, third);

        assertEquals(expected, key.toString());
        assertArrayEquals(Bytes.toBytes(expected), key.toBytes());
    }

    @Test
    public void testCustomKeyComponentTypeEntityKeyBuilderBuildsKeyFromCustomType() {
        final UUID custom = UUID.randomUUID();

        final Key<CustomKeyComponentTypeEntity> key = CustomKeyComponentTypeEntity.keyBuilder()
                .custom(custom)
                .build();

        assertEquals(custom.toString(), key.toString());
        assertArrayEquals(Bytes.toBytes(custom.toString()), key.toBytes());
    }

    @Table("key_builder_table")
    private class KeyBuilderTableConfiguration {

        @Entity(keyComponents = {
                @KeyComponent(constant = ConstantKeyEntity.CONSTANT_KEY_COMPONENT)
        })
        private class ConstantKeyEntity {

            private static final String CONSTANT_KEY_COMPONENT = "my_constant";

            @Column(family = "family")
            private String value;
        }

        @Entity(keyComponents = {
                @KeyComponent(name = "myValue")
        })
        private class SingleKeyComponentEntity {

            @Column(family = "family")
            private String value;
        }

        @Entity(keyComponents = {
                @KeyComponent(name = "first"),
                @KeyComponent(name = "second")
        })
        private class MultiKeyComponentEntity {

            @Column(family = "family")
            private String value;
        }

        @Entity(keyDelimiter = CustomKeyDelimiterEntity.CUSTOM_DELIMITER, keyComponents = {
                @KeyComponent(name = "first"),
                @KeyComponent(name = "second")
        })
        private class CustomKeyDelimiterEntity {

            private static final String CUSTOM_DELIMITER = "|-|";

            @Column(family = "family")
            private String value;
        }

        @Entity(keyComponents = {
                @KeyComponent(name = "first"),
                @KeyComponent(constant = MultiKeyComponentEntityWithConstant.CONSTANT_KEY_COMPONENT),
                @KeyComponent(name = "third")
        })
        private class MultiKeyComponentEntityWithConstant {

            private static final String CONSTANT_KEY_COMPONENT = "second";

            @Column(family = "family")
            private String value;
        }

        @Entity(keyComponents = {
                @KeyComponent(name = "custom", type = UUID.class)
        })
        private class CustomKeyComponentTypeEntity {

            @Column(family = "family")
            private String value;
        }
    }
}
