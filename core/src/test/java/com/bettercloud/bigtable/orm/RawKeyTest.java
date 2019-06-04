package com.bettercloud.bigtable.orm;

import org.junit.Test;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class RawKeyTest {

    @Test
    public void testToBytesReturnsByteValueOfKey() {
        final byte[] rawKey = new byte[] {
                1, 2, 3
        };

        final Key<Entity> key = new RawKey<>(rawKey);

        assertArrayEquals(rawKey, key.toBytes());
    }

    @Test
    public void testKeyIsCopiedOnConstruction() {
        final byte[] rawKey = new byte[] {
                1, 2, 3
        };

        final Key<Entity> key = new RawKey<>(rawKey);

        rawKey[2] = 4;

        assertNotEquals(rawKey, key.toBytes());
    }

    @Test
    public void testEqualsReturnsTrueWhenKeysAreEqual() {
        final Key<Entity> a = new RawKey<>("goodbye".getBytes());
        final Key<Entity> b = new RawKey<>("goodbye".getBytes());

        assertEquals(a, b);
    }

    @Test
    public void testHashCodeReturnsDifferentValuesWhenKeysAreNotEqual() {
        final Key<Entity> a = new RawKey<>("hello".getBytes());
        final Key<Entity> b = new RawKey<>("world".getBytes());

        assertNotEquals(a.hashCode(), b.hashCode());
    }

    @Test
    public void testHashCodeReturnsEqualValuesWhenKeysAreEqual() {
        final Key<Entity> a = new RawKey<>("goodbye".getBytes());
        final Key<Entity> b = new RawKey<>("goodbye".getBytes());

        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    public void testCompareToReturnsZeroValueWhenByteValueOfKeyIsEqualToOther() {
        final Key<Entity> a = new RawKey<>("hello".getBytes()); // [104][101][108][108][111]
        final Key<Entity> b = new RawKey<>("hello".getBytes()); // [104][101][108][108][111]

        assertEquals(0, a.compareTo(b));
    }

    @Test
    public void testCompareToReturnsPositiveValueWhenByteValueOfKeyIsGreaterThanOther() {
        final Key<Entity> a = new RawKey<>("hello".getBytes()); // [104][101][108][108][111]
        final Key<Entity> b = new RawKey<>("goodbye".getBytes()); // [103][111][111][100][98][121][101]

        // Note: even though goodbye is longer 'g' [103] comes before/is less than 'h' [104]
        assertTrue(0 < a.compareTo(b)); // hello is greater than goodbye
    }

    @Test
    public void testCompareToReturnsNegativeValueWhenByteValueOfKeyIsLessThanOther() {
        final Key<Entity> a = new RawKey<>("hello".getBytes()); // [104][101][108][108][111]
        final Key<Entity> b = new RawKey<>("hello there".getBytes()); // [104][101][108][108][111][32][116][104][101][114][101]

        // Note: on equal byte arrays the longer is greater
        assertTrue(0 > a.compareTo(b)); // hello is less than hello there
    }

    @Test
    public void testHashMapReturnsValueAtKey() {
        final byte[] key = "key".getBytes();

        final Key<Entity> putKey = new RawKey<>(key);

        final Map<Key<Entity>, String> map = new HashMap<>();

        final String value = "some value";

        map.put(putKey, value);

        final Key<Entity> getKey = new RawKey<>(key);

        final String result = map.get(getKey);

        assertEquals(value, result);
    }
}
