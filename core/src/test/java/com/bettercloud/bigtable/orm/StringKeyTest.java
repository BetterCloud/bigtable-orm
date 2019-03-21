package com.bettercloud.bigtable.orm;

import org.junit.Test;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class StringKeyTest {

    @Test
    public void testToBytesReturnsByteValueOfKey() {
        final String keyString = "my key";

        final Key<Entity> key = new StringKey<>(keyString);

        assertArrayEquals(keyString.getBytes(Charset.forName("UTF-8")), key.toBytes());
    }

    @Test
    public void testEqualsReturnsFalseWhenKeysAreNotEqual() {
        final Key<Entity> a = new StringKey<>("hello");
        final Key<Entity> b = new StringKey<>("world");

        assertNotEquals(a, b);
    }

    @Test
    public void testEqualsReturnsTrueWhenKeysAreEqual() {
        final Key<Entity> a = new StringKey<>("goodbye");
        final Key<Entity> b = new StringKey<>("goodbye");

        assertEquals(a, b);
    }

    @Test
    public void testHashCodeReturnsDifferentValuesWhenKeysAreNotEqual() {
        final Key<Entity> a = new StringKey<>("hello");
        final Key<Entity> b = new StringKey<>("world");

        assertNotEquals(a.hashCode(), b.hashCode());
    }

    @Test
    public void testHashCodeReturnsEqualValuesWhenKeysAreEqual() {
        final Key<Entity> a = new StringKey<>("goodbye");
        final Key<Entity> b = new StringKey<>("goodbye");

        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    public void testToStringReturnsStringValueOfKey() {
        final String keyString = "my key";

        final Key<Entity> key = new StringKey<>(keyString);

        assertEquals(keyString, key.toString());
    }

    @Test
    public void testHashMapReturnsValueAtKey() {
        final String keyString = "my key";

        final Key<Entity> putKey = new StringKey<>(keyString);

        final Map<Key<Entity>, String> map = new HashMap<>();

        final String value = "some value";

        map.put(putKey, value);

        final Key<Entity> getKey = new StringKey<>(keyString);

        final String result = map.get(getKey);

        assertEquals(value, result);
    }
}
