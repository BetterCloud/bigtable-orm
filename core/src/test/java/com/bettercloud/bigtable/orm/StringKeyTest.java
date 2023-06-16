package com.bettercloud.bigtable.orm;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

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
  public void testCompareToReturnsZeroValueWhenByteValueOfKeyIsEqualToOther() {
    final Key<Entity> a = new StringKey<>("hello"); // [104][101][108][108][111]
    final Key<Entity> b = new StringKey<>("hello"); // [104][101][108][108][111]

    assertEquals(0, a.compareTo(b));
  }

  @Test
  public void testCompareToReturnsPositiveValueWhenByteValueOfKeyIsGreaterThanOther() {
    final Key<Entity> a = new StringKey<>("hello"); // [104][101][108][108][111]
    final Key<Entity> b = new StringKey<>("goodbye"); // [103][111][111][100][98][121][101]

    // Note: even though goodbye is longer 'g' [103] comes before/is less than 'h' [104]
    assertTrue(0 < a.compareTo(b)); // hello is greater than goodbye
  }

  @Test
  public void testCompareToReturnsNegativeValueWhenByteValueOfKeyIsLessThanOther() {
    final Key<Entity> a = new StringKey<>("hello"); // [104][101][108][108][111]
    final Key<Entity> b =
        new StringKey<>("hello there"); // [104][101][108][108][111][32][116][104][101][114][101]

    // Note: on equal byte arrays the longer is greater
    assertTrue(0 > a.compareTo(b)); // hello is less than hello there
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
