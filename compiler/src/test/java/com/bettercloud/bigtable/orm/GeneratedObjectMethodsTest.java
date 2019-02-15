package com.bettercloud.bigtable.orm;

import com.bettercloud.bigtable.orm.annotations.Column;
import com.bettercloud.bigtable.orm.annotations.Entity;
import com.bettercloud.bigtable.orm.annotations.KeyComponent;
import com.bettercloud.bigtable.orm.annotations.Table;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class GeneratedObjectMethodsTest {

    @Test
    public void testSingleObjectEntityEquals() {
        final SingleObjectEntity a = new SingleObjectEntity();
        a.setStringField("some value");

        final SingleObjectEntity b = new SingleObjectEntity();
        b.setStringField("some value");

        assertEquals(a, b);

        b.setStringField("something else");

        assertNotEquals(a, b);

        b.setStringField(null);

        assertNotEquals(a, b);
    }

    @Test
    public void testSingleObjectEntityHashCode() {
        final SingleObjectEntity a = new SingleObjectEntity();
        a.setStringField("some value");

        final SingleObjectEntity b = new SingleObjectEntity();
        b.setStringField("some value");

        assertEquals(a.hashCode(), b.hashCode());

        b.setStringField("something else");

        assertNotEquals(a.hashCode(), b.hashCode());

        b.setStringField(null);

        assertNotEquals(a.hashCode(), b.hashCode());
    }

    @Test
    public void testSingleObjectEntityToString() {
        final SingleObjectEntity a = new SingleObjectEntity();
        a.setStringField("some value");

        assertEquals("SingleObjectEntity{stringField='some value'}", a.toString());

        a.setStringField("something else");

        assertEquals("SingleObjectEntity{stringField='something else'}", a.toString());

        a.setStringField(null);

        assertEquals("SingleObjectEntity{stringField='null'}", a.toString());
    }

    @Test
    public void testMultiObjectEntityEquals() {
        final MultiObjectEntity a = new MultiObjectEntity();
        a.setStringField("some value");
        a.setBooleanField(true);

        final MultiObjectEntity b = new MultiObjectEntity();
        b.setStringField("some value");
        b.setBooleanField(true);

        assertEquals(a, b);

        b.setStringField("something else");

        assertNotEquals(a, b);

        b.setStringField("some value");
        b.setBooleanField(false);

        assertNotEquals(a, b);
    }

    @Test
    public void testMultiObjectEntityHashCode() {
        final MultiObjectEntity a = new MultiObjectEntity();
        a.setStringField("some value");
        a.setBooleanField(true);

        final MultiObjectEntity b = new MultiObjectEntity();
        b.setStringField("some value");
        b.setBooleanField(true);

        assertEquals(a.hashCode(), b.hashCode());

        b.setStringField("something else");

        assertNotEquals(a.hashCode(), b.hashCode());

        b.setStringField("some value");
        b.setBooleanField(false);

        assertNotEquals(a.hashCode(), b.hashCode());
    }

    @Test
    public void testMultiObjectEntityToString() {
        final MultiObjectEntity a = new MultiObjectEntity();
        a.setStringField("some value");
        a.setBooleanField(true);

        assertEquals("MultiObjectEntity{stringField='some value', booleanField=true}", a.toString());

        a.setStringField("something else");

        assertEquals("MultiObjectEntity{stringField='something else', booleanField=true}", a.toString());

        a.setStringField("some value");
        a.setBooleanField(false);

        assertEquals("MultiObjectEntity{stringField='some value', booleanField=false}", a.toString());
    }

    @Test
    public void testSingleArrayEntityEquals() {
        final SingleArrayEntity a = new SingleArrayEntity();
        a.setStringArrayField(new String[]{ "one", "two", "three" });

        final SingleArrayEntity b = new SingleArrayEntity();
        b.setStringArrayField(new String[]{ "one", "two", "three" });

        assertEquals(a, b);

        b.setStringArrayField(new String[]{"four"});

        assertNotEquals(a, b);

        b.setStringArrayField(null);

        assertNotEquals(a, b);
    }

    @Test
    public void testSingleArrayEntityHashCode() {
        final SingleArrayEntity a = new SingleArrayEntity();
        a.setStringArrayField(new String[]{ "one", "two", "three" });

        final SingleArrayEntity b = new SingleArrayEntity();
        b.setStringArrayField(new String[]{ "one", "two", "three" });

        assertEquals(a.hashCode(), b.hashCode());

        b.setStringArrayField(new String[]{"four"});

        assertNotEquals(a.hashCode(), b.hashCode());

        b.setStringArrayField(null);

        assertNotEquals(a.hashCode(), b.hashCode());
    }

    @Test
    public void testSingleArrayEntityToString() {
        final SingleArrayEntity a = new SingleArrayEntity();
        a.setStringArrayField(new String[]{ "one", "two", "three" });

        assertEquals("SingleArrayEntity{stringArrayField=" + Arrays.toString(a.getStringArrayField()) + "}",
                a.toString());

        a.setStringArrayField(new String[]{"four"});

        assertEquals("SingleArrayEntity{stringArrayField=" + Arrays.toString(a.getStringArrayField()) + "}",
                a.toString());

        a.setStringArrayField(null);

        assertEquals("SingleArrayEntity{stringArrayField=" + Arrays.toString(a.getStringArrayField()) + "}",
                a.toString());
    }

    @Test
    public void testMultiArrayEntityEquals() {
        final MultiArrayEntity a = new MultiArrayEntity();
        a.setStringArrayField(new String[]{ "one", "two", "three" });
        a.setBooleanArrayField(new Boolean[]{ true, false });

        final MultiArrayEntity b = new MultiArrayEntity();
        b.setStringArrayField(new String[]{ "one", "two", "three" });
        b.setBooleanArrayField(new Boolean[]{ true, false });

        assertEquals(a, b);

        b.setStringArrayField(new String[]{"four"});

        assertNotEquals(a, b);

        b.setStringArrayField(new String[]{ "one", "two", "three" });
        b.setBooleanArrayField(new Boolean[]{ false });

        assertNotEquals(a, b);
    }

    @Test
    public void testMultiArrayEntityHashCode() {
        final MultiArrayEntity a = new MultiArrayEntity();
        a.setStringArrayField(new String[]{ "one", "two", "three" });
        a.setBooleanArrayField(new Boolean[]{ true, false });

        final MultiArrayEntity b = new MultiArrayEntity();
        b.setStringArrayField(new String[]{ "one", "two", "three" });
        b.setBooleanArrayField(new Boolean[]{ true, false });

        assertEquals(a.hashCode(), b.hashCode());

        b.setStringArrayField(new String[]{"four"});

        assertNotEquals(a.hashCode(), b.hashCode());

        b.setStringArrayField(new String[]{ "one", "two", "three" });
        b.setBooleanArrayField(new Boolean[]{ false });

        assertNotEquals(a.hashCode(), b.hashCode());
    }

    @Test
    public void testMultiArrayEntityToString() {
        final MultiArrayEntity a = new MultiArrayEntity();
        a.setStringArrayField(new String[]{ "one", "two", "three" });
        a.setBooleanArrayField(new Boolean[]{ true, false });

        assertEquals("MultiArrayEntity{stringArrayField=" + Arrays.toString(a.getStringArrayField())
                + ", booleanArrayField=" + Arrays.toString(a.getBooleanArrayField()) + "}", a.toString());

        a.setStringArrayField(new String[]{"four"});

        assertEquals("MultiArrayEntity{stringArrayField=" + Arrays.toString(a.getStringArrayField())
                + ", booleanArrayField=" + Arrays.toString(a.getBooleanArrayField()) + "}", a.toString());

        a.setStringArrayField(new String[]{ "one", "two", "three" });
        a.setBooleanArrayField(new Boolean[]{ false });

        assertEquals("MultiArrayEntity{stringArrayField=" + Arrays.toString(a.getStringArrayField())
                + ", booleanArrayField=" + Arrays.toString(a.getBooleanArrayField()) + "}", a.toString());
    }

    @Test
    public void testObjectArrayComboEntityEquals() {
        final ObjectArrayComboEntity a = new ObjectArrayComboEntity();
        a.setStringField("hello");
        a.setBooleanArrayField(new Boolean[]{ false, false });

        final ObjectArrayComboEntity b = new ObjectArrayComboEntity();
        b.setStringField("hello");
        b.setBooleanArrayField(new Boolean[]{ false, false });

        assertEquals(a, b);

        b.setStringField("world");

        assertNotEquals(a, b);

        b.setStringField("hello");
        b.setBooleanArrayField(new Boolean[]{ true, true, true });

        assertNotEquals(a, b);
    }

    @Test
    public void testObjectArrayComboEntityHashCode() {
        final ObjectArrayComboEntity a = new ObjectArrayComboEntity();
        a.setStringField("hello");
        a.setBooleanArrayField(new Boolean[]{ false, false });

        final ObjectArrayComboEntity b = new ObjectArrayComboEntity();
        b.setStringField("hello");
        b.setBooleanArrayField(new Boolean[]{ false, false });

        assertEquals(a.hashCode(), b.hashCode());

        b.setStringField("world");

        assertNotEquals(a.hashCode(), b.hashCode());

        b.setStringField("hello");
        b.setBooleanArrayField(new Boolean[]{ true, true, true });

        assertNotEquals(a.hashCode(), b.hashCode());
    }

    @Test
    public void testObjectArrayComboEntityToString() {
        final ObjectArrayComboEntity a = new ObjectArrayComboEntity();
        a.setStringField("hello");
        a.setBooleanArrayField(new Boolean[]{ false, false });

        assertEquals("ObjectArrayComboEntity{stringField='hello', booleanArrayField="
                + Arrays.toString(a.getBooleanArrayField()) + "}", a.toString());

        a.setStringField("world");

        assertEquals("ObjectArrayComboEntity{stringField='world', booleanArrayField="
                + Arrays.toString(a.getBooleanArrayField()) + "}", a.toString());

        a.setStringField("hello");
        a.setBooleanArrayField(new Boolean[]{ true, true, true });

        assertEquals("ObjectArrayComboEntity{stringField='hello', booleanArrayField="
                + Arrays.toString(a.getBooleanArrayField()) + "}", a.toString());
    }

    @Table("equals_table")
    private class EqualsTableConfiguration {

        @Entity(keyComponents = {
                @KeyComponent(constant = "constant")
        })
        private class SingleObjectEntity {

            @Column(family = "family")
            private String stringField;
        }

        @Entity(keyComponents = {
                @KeyComponent(constant = "constant")
        })
        private class MultiObjectEntity {

            @Column(family = "family")
            private String stringField;

            @Column(family = "family")
            private Boolean booleanField;
        }

        @Entity(keyComponents = {
                @KeyComponent(constant = "constant")
        })
        private class SingleArrayEntity {

            @Column(family = "family")
            private String[] stringArrayField;
        }

        @Entity(keyComponents = {
                @KeyComponent(constant = "constant")
        })
        private class MultiArrayEntity {

            @Column(family = "family")
            private String[] stringArrayField;

            @Column(family = "family")
            private Boolean[] booleanArrayField;
        }

        @Entity(keyComponents = {
                @KeyComponent(constant = "constant")
        })
        private class ObjectArrayComboEntity {

            @Column(family = "family")
            private String stringField;

            @Column(family = "family")
            private Boolean[] booleanArrayField;
        }
    }
}
