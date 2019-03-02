package com.bettercloud.bigtable.orm;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class BigTableEntityDaoTest {

    @Mock
    private Table table;

    private final List<Column> columns = Arrays.asList(TestColumns.values());

    private final Supplier<TestEntity> entityFactory = TestEntity::new;

    private final Function<TestEntity, EntityConfiguration.EntityDelegate<TestEntity>> delegateFactory = TestDelegate::new;

    private final List<Column> versionedColumns = Arrays.asList(TestVersionedColumns.values());

    private final Supplier<TestVersionedEntity> versionedEntityFactory = TestVersionedEntity::new;

    private final Function<TestVersionedEntity, EntityConfiguration.EntityDelegate<TestVersionedEntity>> versionedDelegateFactory = TestVersionedDelegate::new;

    @Mock
    private ObjectMapper objectMapper;

    private ObjectMapper liveObjectMapper = new ObjectMapper();

    private Dao<TestEntity> testEntityDao;

    private Dao<TestVersionedEntity> testVersionedEntityDao;

    @Before
    public void setup() {
        initMocks(this);

        testEntityDao = new BigTableEntityDao<>(table, columns, entityFactory, delegateFactory, objectMapper);
        testVersionedEntityDao = new BigTableEntityDao<>(table, versionedColumns, versionedEntityFactory,
                versionedDelegateFactory, objectMapper);
    }

    @Test(expected = NullPointerException.class)
    public void testGetWithNullKeyThrowsNullPointerException() throws IOException {
        testEntityDao.get(null);
    }

    @Test
    public void testGetRetrievesAllColumns() throws IOException {
        final String stringValue = "some string";

        final Boolean booleanValue = true;

        final TestNestedObject nestedObject = new TestNestedObject();
        nestedObject.setSomeValue(3);

        final Result result = mock(Result.class);
        when(result.isEmpty()).thenReturn(false);

        final Cell stringValueCell = mock(Cell.class);

        final byte[] stringValueBytes = new byte[] {
                1, 2, 3
        };

        when(stringValueCell.getValueArray()).thenReturn(stringValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.STRING_VALUE.getQualifier()))).thenReturn(stringValueCell);

        when(objectMapper.readValue(stringValueBytes, TestColumns.STRING_VALUE.getTypeReference()))
                .thenReturn(stringValue);

        final Cell booleanValueCell = mock(Cell.class);

        final byte[] booleanValueBytes = new byte[] {
                0
        };

        when(booleanValueCell.getValueArray()).thenReturn(booleanValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getQualifier()))).thenReturn(booleanValueCell);

        when(objectMapper.readValue(booleanValueBytes, TestColumns.BOOLEAN_VALUE.getTypeReference()))
                .thenReturn(booleanValue);

        final Cell nestedObjectCell = mock(Cell.class);

        final byte[] nestedObjectBytes = new byte[] {
                5, 4, 3, 2, 1, 0
        };

        when(nestedObjectCell.getValueArray()).thenReturn(nestedObjectBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.NESTED_OBJECT.getFamily()),
                Bytes.toBytes(TestColumns.NESTED_OBJECT.getQualifier()))).thenReturn(nestedObjectCell);

        when(objectMapper.readValue(nestedObjectBytes, TestColumns.NESTED_OBJECT.getTypeReference()))
                .thenReturn(nestedObject);

        when(table.get(any(Get.class))).thenReturn(result);

        final Optional<TestEntity> retrievedEntity = testEntityDao.get(new StringKey<>("key"));

        assertTrue(retrievedEntity.isPresent());

        final TestEntity entity = retrievedEntity.get();
        assertEquals(stringValue, entity.getStringValue());
        assertEquals(booleanValue, entity.getBooleanValue());
        assertEquals(nestedObject, entity.getNestedObject());
    }

    @Test
    public void testGetRetrievesNullValues() throws IOException {
        final String stringValue = "some string";

        final Boolean booleanValue = true;

        final TestNestedObject nestedObject = null;

        final Result result = mock(Result.class);
        when(result.isEmpty()).thenReturn(false);

        final Cell stringValueCell = mock(Cell.class);

        final byte[] stringValueBytes = new byte[] {
                1, 2, 3
        };

        when(stringValueCell.getValueArray()).thenReturn(stringValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.STRING_VALUE.getQualifier()))).thenReturn(stringValueCell);

        when(objectMapper.readValue(stringValueBytes, TestColumns.STRING_VALUE.getTypeReference()))
                .thenReturn(stringValue);

        final Cell booleanValueCell = mock(Cell.class);

        final byte[] booleanValueBytes = new byte[] {
                0
        };

        when(booleanValueCell.getValueArray()).thenReturn(booleanValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getQualifier()))).thenReturn(booleanValueCell);

        when(objectMapper.readValue(booleanValueBytes, TestColumns.BOOLEAN_VALUE.getTypeReference()))
                .thenReturn(booleanValue);

        final Cell nestedObjectCell = mock(Cell.class);

        final byte[] nestedObjectBytes = new byte[0];

        when(nestedObjectCell.getValueArray()).thenReturn(nestedObjectBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.NESTED_OBJECT.getFamily()),
                Bytes.toBytes(TestColumns.NESTED_OBJECT.getQualifier()))).thenReturn(nestedObjectCell);

        when(objectMapper.readValue(nestedObjectBytes, TestColumns.NESTED_OBJECT.getTypeReference()))
                .thenReturn(nestedObject);

        when(table.get(any(Get.class))).thenReturn(result);

        final Optional<TestEntity> retrievedEntity = testEntityDao.get(new StringKey<>("key"));

        assertTrue(retrievedEntity.isPresent());

        final TestEntity entity = retrievedEntity.get();
        assertNull(entity.getNestedObject());
    }

    @Test
    public void testGetRetrievesVersionedValuesAndTimestamps() throws IOException {
        final String stringValue = "some string";

        final Boolean booleanValue = true;
        final long booleanValueTimestamp = 1234L;

        final Result result = mock(Result.class);
        when(result.isEmpty()).thenReturn(false);

        final Cell stringValueCell = mock(Cell.class);

        final byte[] stringValueBytes = new byte[] {
                1, 2, 3
        };

        when(stringValueCell.getValueArray()).thenReturn(stringValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestVersionedColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestVersionedColumns.STRING_VALUE.getQualifier()))).thenReturn(stringValueCell);

        when(objectMapper.readValue(stringValueBytes, TestVersionedColumns.STRING_VALUE.getTypeReference()))
                .thenReturn(stringValue);

        final Cell booleanValueCell = mock(Cell.class);

        final byte[] booleanValueBytes = new byte[] {
                0
        };

        when(booleanValueCell.getValueArray()).thenReturn(booleanValueBytes);
        when(booleanValueCell.getTimestamp()).thenReturn(booleanValueTimestamp);

        when(result.getColumnLatestCell(Bytes.toBytes(TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getQualifier()))).thenReturn(booleanValueCell);

        when(objectMapper.readValue(booleanValueBytes, TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getTypeReference()))
                .thenReturn(booleanValue);

        when(table.get(any(Get.class))).thenReturn(result);

        final Optional<TestVersionedEntity> retrievedEntity = testVersionedEntityDao.get(new StringKey<>("key"));

        assertTrue(retrievedEntity.isPresent());

        final TestVersionedEntity entity = retrievedEntity.get();
        assertEquals(stringValue, entity.getStringValue());
        assertEquals(booleanValue, entity.getVersionedBooleanValue());
        assertEquals(booleanValueTimestamp, (long) entity.getVersionedBooleanValueTimestamp());
    }

    @Test
    public void testGetWithLiveObjectMapper() throws IOException {
        testEntityDao = new BigTableEntityDao<>(table, columns, entityFactory, delegateFactory);

        final String stringValue = "some string";

        final Boolean booleanValue = true;

        final TestNestedObject nestedObject = new TestNestedObject();
        nestedObject.setSomeValue(3);

        final Result result = mock(Result.class);
        when(result.isEmpty()).thenReturn(false);

        final Cell stringValueCell = mock(Cell.class);

        final byte[] stringValueBytes = liveObjectMapper.writeValueAsBytes(stringValue);

        when(stringValueCell.getValueArray()).thenReturn(stringValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.STRING_VALUE.getQualifier()))).thenReturn(stringValueCell);

        final Cell booleanValueCell = mock(Cell.class);

        final byte[] booleanValueBytes = liveObjectMapper.writeValueAsBytes(booleanValue);

        when(booleanValueCell.getValueArray()).thenReturn(booleanValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getQualifier()))).thenReturn(booleanValueCell);

        final Cell nestedObjectCell = mock(Cell.class);

        final byte[] nestedObjectBytes = liveObjectMapper.writeValueAsBytes(nestedObject);

        when(nestedObjectCell.getValueArray()).thenReturn(nestedObjectBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.NESTED_OBJECT.getFamily()),
                Bytes.toBytes(TestColumns.NESTED_OBJECT.getQualifier()))).thenReturn(nestedObjectCell);

        when(table.get(any(Get.class))).thenReturn(result);

        final Optional<TestEntity> retrievedEntity = testEntityDao.get(new StringKey<>("key"));

        assertTrue(retrievedEntity.isPresent());

        final TestEntity entity = retrievedEntity.get();
        assertEquals(stringValue, entity.getStringValue());
        assertEquals(booleanValue, entity.getBooleanValue());
        assertEquals(nestedObject, entity.getNestedObject());
    }

    @Test
    public void testGetRetrievesNullStringValueWithLiveObjectMapper() throws IOException {
        testEntityDao = new BigTableEntityDao<>(table, columns, entityFactory, delegateFactory);

        final String stringValue = null;

        final Boolean booleanValue = true;

        final TestNestedObject nestedObject = new TestNestedObject();
        nestedObject.setSomeValue(3);

        final Result result = mock(Result.class);
        when(result.isEmpty()).thenReturn(false);

        final Cell stringValueCell = mock(Cell.class);

        final byte[] stringValueBytes = new byte[0];

        when(stringValueCell.getValueArray()).thenReturn(stringValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.STRING_VALUE.getQualifier()))).thenReturn(stringValueCell);

        final Cell booleanValueCell = mock(Cell.class);

        final byte[] booleanValueBytes = liveObjectMapper.writeValueAsBytes(booleanValue);

        when(booleanValueCell.getValueArray()).thenReturn(booleanValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getQualifier()))).thenReturn(booleanValueCell);

        final Cell nestedObjectCell = mock(Cell.class);

        final byte[] nestedObjectBytes = liveObjectMapper.writeValueAsBytes(nestedObject);

        when(nestedObjectCell.getValueArray()).thenReturn(nestedObjectBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.NESTED_OBJECT.getFamily()),
                Bytes.toBytes(TestColumns.NESTED_OBJECT.getQualifier()))).thenReturn(nestedObjectCell);

        when(table.get(any(Get.class))).thenReturn(result);

        final Optional<TestEntity> retrievedEntity = testEntityDao.get(new StringKey<>("key"));

        assertTrue(retrievedEntity.isPresent());

        final TestEntity entity = retrievedEntity.get();
        assertEquals(stringValue, entity.getStringValue());
        assertEquals(booleanValue, entity.getBooleanValue());
        assertEquals(nestedObject, entity.getNestedObject());
    }

    @Test
    public void testGetRetrievesNullBooleanValueWithLiveObjectMapper() throws IOException {
        testEntityDao = new BigTableEntityDao<>(table, columns, entityFactory, delegateFactory);

        final String stringValue = "some string";

        final Boolean booleanValue = null;

        final TestNestedObject nestedObject = new TestNestedObject();
        nestedObject.setSomeValue(3);

        final Result result = mock(Result.class);
        when(result.isEmpty()).thenReturn(false);

        final Cell stringValueCell = mock(Cell.class);

        final byte[] stringValueBytes = liveObjectMapper.writeValueAsBytes(stringValue);

        when(stringValueCell.getValueArray()).thenReturn(stringValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.STRING_VALUE.getQualifier()))).thenReturn(stringValueCell);

        final Cell booleanValueCell = mock(Cell.class);

        final byte[] booleanValueBytes = new byte[0];

        when(booleanValueCell.getValueArray()).thenReturn(booleanValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getQualifier()))).thenReturn(booleanValueCell);

        final Cell nestedObjectCell = mock(Cell.class);

        final byte[] nestedObjectBytes = liveObjectMapper.writeValueAsBytes(nestedObject);

        when(nestedObjectCell.getValueArray()).thenReturn(nestedObjectBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.NESTED_OBJECT.getFamily()),
                Bytes.toBytes(TestColumns.NESTED_OBJECT.getQualifier()))).thenReturn(nestedObjectCell);

        when(table.get(any(Get.class))).thenReturn(result);

        final Optional<TestEntity> retrievedEntity = testEntityDao.get(new StringKey<>("key"));

        assertTrue(retrievedEntity.isPresent());

        final TestEntity entity = retrievedEntity.get();
        assertEquals(stringValue, entity.getStringValue());
        assertEquals(booleanValue, entity.getBooleanValue());
        assertEquals(nestedObject, entity.getNestedObject());
    }

    @Test
    public void testGetRetrievesNullNestedObjectWithLiveObjectMapper() throws IOException {
        testEntityDao = new BigTableEntityDao<>(table, columns, entityFactory, delegateFactory);

        final String stringValue = "some string";

        final Boolean booleanValue = true;

        final TestNestedObject nestedObject = null;

        final Result result = mock(Result.class);
        when(result.isEmpty()).thenReturn(false);

        final Cell stringValueCell = mock(Cell.class);

        final byte[] stringValueBytes = liveObjectMapper.writeValueAsBytes(stringValue);

        when(stringValueCell.getValueArray()).thenReturn(stringValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.STRING_VALUE.getQualifier()))).thenReturn(stringValueCell);

        final Cell booleanValueCell = mock(Cell.class);

        final byte[] booleanValueBytes = liveObjectMapper.writeValueAsBytes(booleanValue);

        when(booleanValueCell.getValueArray()).thenReturn(booleanValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getQualifier()))).thenReturn(booleanValueCell);

        final Cell nestedObjectCell = mock(Cell.class);

        final byte[] nestedObjectBytes = new byte[0];

        when(nestedObjectCell.getValueArray()).thenReturn(nestedObjectBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.NESTED_OBJECT.getFamily()),
                Bytes.toBytes(TestColumns.NESTED_OBJECT.getQualifier()))).thenReturn(nestedObjectCell);

        when(table.get(any(Get.class))).thenReturn(result);

        final Optional<TestEntity> retrievedEntity = testEntityDao.get(new StringKey<>("key"));

        assertTrue(retrievedEntity.isPresent());

        final TestEntity entity = retrievedEntity.get();
        assertEquals(stringValue, entity.getStringValue());
        assertEquals(booleanValue, entity.getBooleanValue());
        assertEquals(nestedObject, entity.getNestedObject());
    }

    @Test
    public void testGetReturnsEmptyOptionalWhenResultIsEmpty() throws IOException {
        final Result result = mock(Result.class);
        when(result.isEmpty()).thenReturn(true);

        when(table.get(any(Get.class))).thenReturn(result);

        final Optional<TestEntity> retrievedEntity = testEntityDao.get(new StringKey<>("key"));

        assertFalse(retrievedEntity.isPresent());
    }

    @Test(expected = NullPointerException.class)
    public void testSaveWithNullKeyThrowsNullPointerException() throws IOException {
        final TestEntity testEntity = new TestEntity();
        testEntity.setStringValue("test");

        testEntityDao.save(null, testEntity);
    }

    @Test(expected = NullPointerException.class)
    public void testSaveWithNullEntityThrowsNullPointerException() throws IOException {
        final Key<TestEntity> key = new StringKey<>("key");

        testEntityDao.save(key, null);
    }

    @Test
    public void testSavePutsAllColumns() throws IOException {
        final String stringValue = "some string";

        final Boolean booleanValue = true;

        final TestNestedObject nestedObject = new TestNestedObject();
        nestedObject.setSomeValue(3);

        final TestEntity testEntity = new TestEntity();
        testEntity.setStringValue(stringValue);
        testEntity.setBooleanValue(booleanValue);
        testEntity.setNestedObject(nestedObject);

        final byte[] stringValueBytes = {
                1, 2, 3
        };

        when(objectMapper.writeValueAsBytes(stringValue)).thenReturn(stringValueBytes);

        final byte[] booleanValueBytes = {
                0
        };

        when(objectMapper.writeValueAsBytes(booleanValue)).thenReturn(booleanValueBytes);

        final byte[] nestedObjectBytes = {
                5, 4, 3, 2, 1, 0
        };

        when(objectMapper.writeValueAsBytes(nestedObject)).thenReturn(nestedObjectBytes);

        final TestEntity result = testEntityDao.save(new StringKey<>("key"), testEntity);

        assertNotNull(result);

        assertEquals(stringValue, result.getStringValue());
        assertEquals(booleanValue, result.getBooleanValue());
        assertEquals(nestedObject, result.getNestedObject());

        final ArgumentCaptor<Put> putArgumentCaptor = ArgumentCaptor.forClass(Put.class);

        verify(table).put(putArgumentCaptor.capture());

        final Put put = putArgumentCaptor.getValue();
        assertNotNull(put);

        assertTrue(put.has(Bytes.toBytes(TestColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.STRING_VALUE.getQualifier()), stringValueBytes));
        assertTrue(put.has(Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getQualifier()), booleanValueBytes));
        assertTrue(put.has(Bytes.toBytes(TestColumns.NESTED_OBJECT.getFamily()),
                Bytes.toBytes(TestColumns.NESTED_OBJECT.getQualifier()), nestedObjectBytes));
    }

    @Test
    public void testSavePutsAllColumnsWithLiveObjectMapper() throws IOException {
        testEntityDao = new BigTableEntityDao<>(table, columns, entityFactory, delegateFactory);

        final String stringValue = "some string";

        final Boolean booleanValue = true;

        final TestNestedObject nestedObject = new TestNestedObject();
        nestedObject.setSomeValue(3);

        final TestEntity testEntity = new TestEntity();
        testEntity.setStringValue(stringValue);
        testEntity.setBooleanValue(booleanValue);
        testEntity.setNestedObject(nestedObject);

        final TestEntity result = testEntityDao.save(new StringKey<>("key"), testEntity);

        assertNotNull(result);

        assertEquals(stringValue, result.getStringValue());
        assertEquals(booleanValue, result.getBooleanValue());
        assertEquals(nestedObject, result.getNestedObject());

        final ArgumentCaptor<Put> putArgumentCaptor = ArgumentCaptor.forClass(Put.class);

        verify(table).put(putArgumentCaptor.capture());

        final Put put = putArgumentCaptor.getValue();
        assertNotNull(put);

        final byte[] expectedStringValueBytes = liveObjectMapper.writeValueAsBytes(stringValue);

        assertTrue(put.has(Bytes.toBytes(TestColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.STRING_VALUE.getQualifier()), expectedStringValueBytes));

        final byte[] expectedBooleanValueBytes = liveObjectMapper.writeValueAsBytes(booleanValue);

        assertTrue(put.has(Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getQualifier()), expectedBooleanValueBytes));

        final byte[] expectedNestedObjectBytes = liveObjectMapper.writeValueAsBytes(nestedObject);

        assertTrue(put.has(Bytes.toBytes(TestColumns.NESTED_OBJECT.getFamily()),
                Bytes.toBytes(TestColumns.NESTED_OBJECT.getQualifier()), expectedNestedObjectBytes));
    }

    @Test
    public void testSavePutsNullStringValueWithLiveObjectMapper() throws IOException {
        testEntityDao = new BigTableEntityDao<>(table, columns, entityFactory, delegateFactory);

        final String stringValue = null;

        final Boolean booleanValue = true;

        final TestNestedObject nestedObject = new TestNestedObject();
        nestedObject.setSomeValue(3);

        final TestEntity testEntity = new TestEntity();
        testEntity.setStringValue(stringValue);
        testEntity.setBooleanValue(booleanValue);
        testEntity.setNestedObject(nestedObject);

        final TestEntity result = testEntityDao.save(new StringKey<>("key"), testEntity);

        assertNotNull(result);

        assertEquals(stringValue, result.getStringValue());
        assertEquals(booleanValue, result.getBooleanValue());
        assertEquals(nestedObject, result.getNestedObject());

        final ArgumentCaptor<Put> putArgumentCaptor = ArgumentCaptor.forClass(Put.class);

        verify(table).put(putArgumentCaptor.capture());

        final Put put = putArgumentCaptor.getValue();
        assertNotNull(put);

        final byte[] expectedStringValueBytes = new byte[0];

        assertTrue(put.has(Bytes.toBytes(TestColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.STRING_VALUE.getQualifier()), expectedStringValueBytes));

        final byte[] expectedBooleanValueBytes = liveObjectMapper.writeValueAsBytes(booleanValue);

        assertTrue(put.has(Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getQualifier()), expectedBooleanValueBytes));

        final byte[] expectedNestedObjectBytes = liveObjectMapper.writeValueAsBytes(nestedObject);

        assertTrue(put.has(Bytes.toBytes(TestColumns.NESTED_OBJECT.getFamily()),
                Bytes.toBytes(TestColumns.NESTED_OBJECT.getQualifier()), expectedNestedObjectBytes));
    }

    @Test
    public void testSavePutsNullBooleanValueWithLiveObjectMapper() throws IOException {
        testEntityDao = new BigTableEntityDao<>(table, columns, entityFactory, delegateFactory);

        final String stringValue = "some string";

        final Boolean booleanValue = null;

        final TestNestedObject nestedObject = new TestNestedObject();
        nestedObject.setSomeValue(3);

        final TestEntity testEntity = new TestEntity();
        testEntity.setStringValue(stringValue);
        testEntity.setBooleanValue(booleanValue);
        testEntity.setNestedObject(nestedObject);

        final TestEntity result = testEntityDao.save(new StringKey<>("key"), testEntity);

        assertNotNull(result);

        assertEquals(stringValue, result.getStringValue());
        assertEquals(booleanValue, result.getBooleanValue());
        assertEquals(nestedObject, result.getNestedObject());

        final ArgumentCaptor<Put> putArgumentCaptor = ArgumentCaptor.forClass(Put.class);

        verify(table).put(putArgumentCaptor.capture());

        final Put put = putArgumentCaptor.getValue();
        assertNotNull(put);

        final byte[] expectedStringValueBytes = liveObjectMapper.writeValueAsBytes(stringValue);

        assertTrue(put.has(Bytes.toBytes(TestColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.STRING_VALUE.getQualifier()), expectedStringValueBytes));

        final byte[] expectedBooleanValueBytes = new byte[0];

        assertTrue(put.has(Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getQualifier()), expectedBooleanValueBytes));

        final byte[] expectedNestedObjectBytes = liveObjectMapper.writeValueAsBytes(nestedObject);

        assertTrue(put.has(Bytes.toBytes(TestColumns.NESTED_OBJECT.getFamily()),
                Bytes.toBytes(TestColumns.NESTED_OBJECT.getQualifier()), expectedNestedObjectBytes));
    }

    @Test
    public void testSaveWithPutsNullNestedObjectWithLiveObjectMapper() throws IOException {
        testEntityDao = new BigTableEntityDao<>(table, columns, entityFactory, delegateFactory);

        final String stringValue = "some string";

        final Boolean booleanValue = true;

        final TestNestedObject nestedObject = null;

        final TestEntity testEntity = new TestEntity();
        testEntity.setStringValue(stringValue);
        testEntity.setBooleanValue(booleanValue);
        testEntity.setNestedObject(nestedObject);

        final TestEntity result = testEntityDao.save(new StringKey<>("key"), testEntity);

        assertNotNull(result);

        assertEquals(stringValue, result.getStringValue());
        assertEquals(booleanValue, result.getBooleanValue());
        assertEquals(nestedObject, result.getNestedObject());

        final ArgumentCaptor<Put> putArgumentCaptor = ArgumentCaptor.forClass(Put.class);

        verify(table).put(putArgumentCaptor.capture());

        final Put put = putArgumentCaptor.getValue();
        assertNotNull(put);

        final byte[] expectedStringValueBytes = liveObjectMapper.writeValueAsBytes(stringValue);

        assertTrue(put.has(Bytes.toBytes(TestColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.STRING_VALUE.getQualifier()), expectedStringValueBytes));

        final byte[] expectedBooleanValueBytes = liveObjectMapper.writeValueAsBytes(booleanValue);

        assertTrue(put.has(Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getQualifier()), expectedBooleanValueBytes));

        final byte[] expectedNestedObjectBytes = new byte[0];

        assertTrue(put.has(Bytes.toBytes(TestColumns.NESTED_OBJECT.getFamily()),
                Bytes.toBytes(TestColumns.NESTED_OBJECT.getQualifier()), expectedNestedObjectBytes));
    }

    @Test
    public void testSavePutsNullValues() throws IOException {
        final String stringValue = "some string";

        final Boolean booleanValue = true;

        final TestNestedObject nestedObject = null;

        final TestEntity testEntity = new TestEntity();
        testEntity.setStringValue(stringValue);
        testEntity.setBooleanValue(booleanValue);
        testEntity.setNestedObject(nestedObject);

        final byte[] stringValueBytes = {
                1, 2, 3
        };

        when(objectMapper.writeValueAsBytes(stringValue)).thenReturn(stringValueBytes);

        final byte[] booleanValueBytes = {
                0
        };

        when(objectMapper.writeValueAsBytes(booleanValue)).thenReturn(booleanValueBytes);

        final byte[] nestedObjectBytes = new byte[0];

        final TestEntity result = testEntityDao.save(new StringKey<>("key"), testEntity);

        assertNotNull(result);

        assertEquals(stringValue, result.getStringValue());
        assertEquals(booleanValue, result.getBooleanValue());
        assertEquals(nestedObject, result.getNestedObject());

        final ArgumentCaptor<Put> putArgumentCaptor = ArgumentCaptor.forClass(Put.class);

        verify(table).put(putArgumentCaptor.capture());

        final Put put = putArgumentCaptor.getValue();
        assertNotNull(put);

        assertTrue(put.has(Bytes.toBytes(TestColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.STRING_VALUE.getQualifier()), stringValueBytes));
        assertTrue(put.has(Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getQualifier()), booleanValueBytes));
        assertTrue(put.has(Bytes.toBytes(TestColumns.NESTED_OBJECT.getFamily()),
                Bytes.toBytes(TestColumns.NESTED_OBJECT.getQualifier()), nestedObjectBytes));
    }

    @Test
    public void testSavePutsVersionedColumnsWithDefinedTimestamp() throws IOException {
        final String stringValue = "some string";

        final Boolean booleanValue = true;
        final long booleanValueTimestamp = 1234L;

        final TestVersionedEntity testVersionedEntity = new TestVersionedEntity();
        testVersionedEntity.setStringValue(stringValue);
        testVersionedEntity.setVersionedBooleanValue(booleanValue, booleanValueTimestamp);

        final byte[] stringValueBytes = {
                1, 2, 3
        };

        when(objectMapper.writeValueAsBytes(stringValue)).thenReturn(stringValueBytes);

        final byte[] booleanValueBytes = {
                0
        };

        when(objectMapper.writeValueAsBytes(booleanValue)).thenReturn(booleanValueBytes);

        final TestVersionedEntity result = testVersionedEntityDao.save(new StringKey<>("key"), testVersionedEntity);

        assertNotNull(result);

        assertEquals(stringValue, result.getStringValue());
        assertEquals(booleanValue, result.getVersionedBooleanValue());
        assertEquals(booleanValueTimestamp, (long) result.getVersionedBooleanValueTimestamp());

        final ArgumentCaptor<Put> putArgumentCaptor = ArgumentCaptor.forClass(Put.class);

        verify(table).put(putArgumentCaptor.capture());

        final Put put = putArgumentCaptor.getValue();
        assertNotNull(put);

        assertTrue(put.has(Bytes.toBytes(TestVersionedColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestVersionedColumns.STRING_VALUE.getQualifier()), stringValueBytes));
        assertTrue(put.has(Bytes.toBytes(TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getQualifier()), booleanValueBytes));
    }

    @Test
    public void testSavePutsVersionedColumnsWithUndefinedTimestamp() throws IOException {
        final String stringValue = "some string";

        final Boolean booleanValue = true;

        final TestVersionedEntity testVersionedEntity = new TestVersionedEntity();
        testVersionedEntity.setStringValue(stringValue);
        testVersionedEntity.setVersionedBooleanValue(booleanValue);

        final byte[] stringValueBytes = {
                1, 2, 3
        };

        when(objectMapper.writeValueAsBytes(stringValue)).thenReturn(stringValueBytes);

        final byte[] booleanValueBytes = {
                0
        };

        when(objectMapper.writeValueAsBytes(booleanValue)).thenReturn(booleanValueBytes);

        final long atLeastTimestamp = Instant.now().toEpochMilli();

        final TestVersionedEntity result = testVersionedEntityDao.save(new StringKey<>("key"), testVersionedEntity);

        assertNotNull(result);

        assertEquals(stringValue, result.getStringValue());
        assertEquals(booleanValue, result.getVersionedBooleanValue());
        assertTrue(result.getVersionedBooleanValueTimestamp() >= atLeastTimestamp);

        final ArgumentCaptor<Put> putArgumentCaptor = ArgumentCaptor.forClass(Put.class);

        verify(table).put(putArgumentCaptor.capture());

        final Put put = putArgumentCaptor.getValue();
        assertNotNull(put);

        assertTrue(put.has(Bytes.toBytes(TestVersionedColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestVersionedColumns.STRING_VALUE.getQualifier()), stringValueBytes));
        assertTrue(put.has(Bytes.toBytes(TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getQualifier()), booleanValueBytes));
    }

    @Test(expected = NullPointerException.class)
    public void testDeleteWithNullKeyThrowsNullPointerException() throws IOException {
        testEntityDao.delete(null);
    }

    @Test
    public void testDeleteDeletesAllColumns() throws IOException {
        testEntityDao.delete(new StringKey<>("key"));

        final ArgumentCaptor<Delete> putArgumentCaptor = ArgumentCaptor.forClass(Delete.class);

        verify(table).delete(putArgumentCaptor.capture());

        // Delete has no easy way to verify which column family/qualifiers are being deleted
        final Delete delete = putArgumentCaptor.getValue();
        assertNotNull(delete);
    }

    private static class TestEntity implements Entity {

        private String stringValue;
        private Boolean booleanValue;
        private TestNestedObject nestedObject;

        public String getStringValue() {
            return stringValue;
        }

        public void setStringValue(final String stringValue) {
            this.stringValue = stringValue;
        }

        public Boolean getBooleanValue() {
            return booleanValue;
        }

        public void setBooleanValue(final Boolean booleanValue) {
            this.booleanValue = booleanValue;
        }

        public TestNestedObject getNestedObject() {
            return nestedObject;
        }

        public void setNestedObject(final TestNestedObject nestedObject) {
            this.nestedObject = nestedObject;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final TestEntity that = (TestEntity) o;

            return Objects.equals(stringValue, that.stringValue)
                    && Objects.equals(booleanValue, that.booleanValue)
                    && Objects.equals(nestedObject, that.nestedObject);
        }

        @Override
        public int hashCode() {
            return Objects.hash(stringValue, booleanValue, nestedObject);
        }
    }

    private static class TestNestedObject {

        private int someValue;

        public int getSomeValue() {
            return someValue;
        }

        public void setSomeValue(final int someValue) {
            this.someValue = someValue;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final TestNestedObject that = (TestNestedObject) o;

            return someValue == that.someValue;
        }

        @Override
        public int hashCode() {
            return Objects.hash(someValue);
        }
    }

    private enum TestColumns implements Column {

        STRING_VALUE("stringValueFamily", "stringValueQualifier", new TypeReference<String>() { }, false),
        BOOLEAN_VALUE("booleanValueFamily", "booleanValueQualifier", new TypeReference<Boolean>() { }, false),
        NESTED_OBJECT("nestedObjectFamily", "nestedObjectQualifier", new TypeReference<TestNestedObject>() { }, false);

        private final String family;
        private final String qualifier;
        private final TypeReference<?> typeReference;
        private final boolean isVersioned;

        TestColumns(final String family, final String qualifier, final TypeReference<?> typeReference, final boolean isVersioned) {
            this.family = family;
            this.qualifier = qualifier;
            this.typeReference = typeReference;
            this.isVersioned = isVersioned;
        }

        @Override
        public String getFamily() {
            return family;
        }

        @Override
        public String getQualifier() {
            return qualifier;
        }

        @Override
        public TypeReference<?> getTypeReference() {
            return typeReference;
        }

        @Override
        public boolean isVersioned() {
            return isVersioned;
        }
    }

    private static class TestDelegate implements EntityConfiguration.EntityDelegate<TestEntity> {

        private final TestEntity entity;

        TestDelegate(final TestEntity entity) {
            this.entity = entity;
        }

        @Override
        public Object getColumnValue(final Column column) {
            if (TestColumns.STRING_VALUE.equals(column)) {
                return entity.getStringValue();
            } else if (TestColumns.BOOLEAN_VALUE.equals(column)) {
                return entity.getBooleanValue();
            } else if (TestColumns.NESTED_OBJECT.equals(column)) {
                return entity.getNestedObject();
            } else {
                throw new IllegalArgumentException();
            }
        }

        @Override
        public void setColumnValue(final Column column, final Object value) {
            if (TestColumns.STRING_VALUE.equals(column)) {
                entity.setStringValue((String) value);
            } else if (TestColumns.BOOLEAN_VALUE.equals(column)) {
                entity.setBooleanValue((Boolean) value);
            } else if (TestColumns.NESTED_OBJECT.equals(column)) {
                entity.setNestedObject((TestNestedObject) value);
            } else {
                throw new IllegalArgumentException();
            }
        }

        @Override
        public Long getColumnTimestamp(final Column column) {
            throw new IllegalArgumentException();
        }

        @Override
        public void setColumnTimestamp(final Column column, final long timestamp) {
            throw new IllegalArgumentException();
        }
    }

    private static class TestVersionedEntity implements Entity {

        private String stringValue;
        private Boolean versionedBooleanValue;
        private Long versionedBooleanValueTimestamp;

        public String getStringValue() {
            return stringValue;
        }

        public void setStringValue(final String stringValue) {
            this.stringValue = stringValue;
        }

        public Boolean getVersionedBooleanValue() {
            return versionedBooleanValue;
        }

        public Long getVersionedBooleanValueTimestamp() {
            return versionedBooleanValueTimestamp;
        }

        public void setVersionedBooleanValue(final Boolean versionedBooleanValue) {
            this.versionedBooleanValue = versionedBooleanValue;
            this.versionedBooleanValueTimestamp = null;
        }

        public void setVersionedBooleanValue(final Boolean versionedBooleanValue, final long timestamp) {
            this.versionedBooleanValue = versionedBooleanValue;
            this.versionedBooleanValueTimestamp = timestamp;
        }

        private void setVersionedBooleanValueTimestamp(final Long timestamp) {
            this.versionedBooleanValueTimestamp = timestamp;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final TestVersionedEntity that = (TestVersionedEntity) o;

            return Objects.equals(stringValue, that.stringValue)
                    && Objects.equals(versionedBooleanValue, that.versionedBooleanValue);
        }

        @Override
        public int hashCode() {
            return Objects.hash(stringValue, versionedBooleanValue);
        }
    }

    private enum TestVersionedColumns implements Column {

        STRING_VALUE("stringValueFamily", "stringValueQualifier", new TypeReference<String>() { }, false),
        VERSIONED_BOOLEAN_VALUE("versionedBooleanValueFamily", "versionedBooleanValueQualifier", new TypeReference<Boolean>() { }, true);

        private final String family;
        private final String qualifier;
        private final TypeReference<?> typeReference;
        private final boolean isVersioned;

        TestVersionedColumns(final String family, final String qualifier, final TypeReference<?> typeReference, final boolean isVersioned) {
            this.family = family;
            this.qualifier = qualifier;
            this.typeReference = typeReference;
            this.isVersioned = isVersioned;
        }

        @Override
        public String getFamily() {
            return family;
        }

        @Override
        public String getQualifier() {
            return qualifier;
        }

        @Override
        public TypeReference<?> getTypeReference() {
            return typeReference;
        }

        @Override
        public boolean isVersioned() {
            return isVersioned;
        }
    }

    private static class TestVersionedDelegate implements EntityConfiguration.EntityDelegate<TestVersionedEntity> {

        private final TestVersionedEntity entity;

        TestVersionedDelegate(final TestVersionedEntity entity) {
            this.entity = entity;
        }

        @Override
        public Object getColumnValue(final Column column) {
            if (TestVersionedColumns.STRING_VALUE.equals(column)) {
                return entity.getStringValue();
            } else if (TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.equals(column)) {
                return entity.getVersionedBooleanValue();
            } else {
                throw new IllegalArgumentException();
            }
        }

        @Override
        public void setColumnValue(final Column column, final Object value) {
            if (TestVersionedColumns.STRING_VALUE.equals(column)) {
                entity.setStringValue((String) value);
            } else if (TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.equals(column)) {
                entity.setVersionedBooleanValue((Boolean) value);
            } else {
                throw new IllegalArgumentException();
            }
        }

        @Override
        public Long getColumnTimestamp(final Column column) {
            if (TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.equals(column)) {
                return entity.getVersionedBooleanValueTimestamp();
            } else {
                throw new IllegalArgumentException();
            }
        }

        @Override
        public void setColumnTimestamp(final Column column, final long timestamp) {
            if (TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.equals(column)) {
                entity.setVersionedBooleanValueTimestamp(timestamp);
            } else {
                throw new IllegalArgumentException();
            }
        }
    }
}
