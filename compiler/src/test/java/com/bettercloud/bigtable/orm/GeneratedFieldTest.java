package com.bettercloud.bigtable.orm;

import com.bettercloud.bigtable.orm.annotations.Column;
import com.bettercloud.bigtable.orm.annotations.Entity;
import com.bettercloud.bigtable.orm.annotations.KeyComponent;
import com.bettercloud.bigtable.orm.annotations.Table;
import org.junit.Test;

import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class GeneratedFieldTest {

    @Test
    public void testStringFieldGetterReturnsValueSetBySetter() {
        final String stringValue = "hello world";

        final StringFieldEntity entity = new StringFieldEntity();
        entity.setStringField(stringValue);

        final String result = entity.getStringField();

        assertEquals(stringValue, result);
    }

    @Test
    public void testBooleanFieldGetterReturnsValueSetBySetter() {
        final Boolean booleanValue = true;

        final BooleanFieldEntity entity = new BooleanFieldEntity();
        entity.setBooleanField(booleanValue);

        final Boolean result = entity.getBooleanField();

        assertEquals(booleanValue, result);
    }

    @Test
    public void testPrimitiveIntBecomesNullableBoxedValue() {
        final PrimitiveIntEntity entity = new PrimitiveIntEntity();
        entity.setIntField(5);

        assertEquals(5, (int) entity.getIntField());

        entity.setIntField(null);

        assertNull(entity.getIntField());
    }

    @Test
    public void testNestedObjectGetterReturnsValueSetBySetter() {
        final NestedObject nestedObject = new NestedObject();
        nestedObject.setNestedValue("hello");

        final NestedObjectEntity entity = new NestedObjectEntity();
        entity.setNestedObject(nestedObject);

        final NestedObject result = entity.getNestedObject();

        assertEquals(nestedObject, result);
    }

    @Test
    public void testVersionedFieldGetterReturnsValueSetBySetter() {
        final VersionedFieldEntity entity = new VersionedFieldEntity();
        entity.setIntField(10, 1234L);

        assertEquals(10, (int) entity.getIntField());

        entity.setIntField(null, 4321L);

        assertNull(entity.getIntField());

        entity.setIntField(5);

        assertEquals(5, (int) entity.getIntField());

        entity.setIntField(null);

        assertNull(entity.getIntField());
    }

    @Table("field_table")
    private class FieldTableConfiguration {

        @Entity(keyComponents = {
                @KeyComponent(constant = "constant")
        })
        private class StringFieldEntity {

            @Column(family = "family")
            private String stringField;
        }

        @Entity(keyComponents = {
                @KeyComponent(constant = "constant")
        })
        private class BooleanFieldEntity {

            @Column(family = "family")
            private Boolean booleanField;
        }

        @Entity(keyComponents = {
                @KeyComponent(constant = "constant")
        })
        private class PrimitiveIntEntity {

            @Column(family = "family")
            private int intField;
        }

        @Entity(keyComponents = {
                @KeyComponent(constant = "constant")
        })
        private class NestedObjectEntity {

            @Column(family = "family")
            private NestedObject nestedObject;
        }

        @Entity(keyComponents = {
                @KeyComponent(constant = "constant")
        })
        private class VersionedFieldEntity {

            @Column(family = "family", versioned = true)
            private int intField;
        }
    }

    public static class NestedObject {

        private String nestedValue;

        String getNestedValue() {
            return nestedValue;
        }

        void setNestedValue(final String nestedValue) {
            this.nestedValue = nestedValue;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final NestedObject that = (NestedObject) o;

            return Objects.equals(nestedValue, that.nestedValue);
        }

        @Override
        public int hashCode() {
            return Objects.hash(nestedValue);
        }
    }
}
