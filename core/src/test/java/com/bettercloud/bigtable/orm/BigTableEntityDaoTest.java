package com.bettercloud.bigtable.orm;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class BigTableEntityDaoTest extends AbstractBigTableEntityDaoTest {

    @Mock
    private Table table;

    @Mock
    private ResultScanner scanner;

    private final List<Column> columns = Lists.newArrayList(TestColumns.BOOLEAN_VALUE, TestColumns.STRING_VALUE, TestColumns.NESTED_OBJECT);

    private final Supplier<TestEntity> entityFactory = TestEntity::new;

    private final Function<TestEntity, EntityConfiguration.EntityDelegate<TestEntity>> delegateFactory = TestDelegate::new;

    private final List<Column> versionedColumns = Lists.newArrayList(TestVersionedColumns.STRING_VALUE, TestVersionedColumns.VERSIONED_BOOLEAN_VALUE);

    private final Supplier<TestVersionedEntity> versionedEntityFactory = TestVersionedEntity::new;

    private final Function<TestVersionedEntity, EntityConfiguration.EntityDelegate<TestVersionedEntity>> versionedDelegateFactory = TestVersionedDelegate::new;

    @Mock
    private ObjectMapper objectMapper;

    private ObjectMapper liveObjectMapper = new ObjectMapper();

    private Dao<TestEntity> testEntityDao;

    private Dao<TestVersionedEntity> testVersionedEntityDao;

    @Captor
    private ArgumentCaptor<List<Put>> putArgumentCaptor;

    @Captor
    private ArgumentCaptor<List<Delete>> deleteArgumentCaptor;

    @Captor
    private ArgumentCaptor<Scan> scanArgumentCaptor;

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

        when(objectMapper.readValue(stringValueBytes, TestColumns.STRING_VALUE.getTestTypeReference()))
                .thenReturn(stringValue);

        final Cell booleanValueCell = mock(Cell.class);

        final byte[] booleanValueBytes = new byte[] {
                0
        };

        when(booleanValueCell.getValueArray()).thenReturn(booleanValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getQualifier()))).thenReturn(booleanValueCell);

        when(objectMapper.readValue(booleanValueBytes, TestColumns.BOOLEAN_VALUE.getTestTypeReference()))
                .thenReturn(booleanValue);

        final Cell nestedObjectCell = mock(Cell.class);

        final byte[] nestedObjectBytes = new byte[] {
                5, 4, 3, 2, 1, 0
        };

        when(nestedObjectCell.getValueArray()).thenReturn(nestedObjectBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.NESTED_OBJECT.getFamily()),
                Bytes.toBytes(TestColumns.NESTED_OBJECT.getQualifier()))).thenReturn(nestedObjectCell);

        when(objectMapper.readValue(nestedObjectBytes, TestColumns.NESTED_OBJECT.getTestTypeReference()))
                .thenReturn(nestedObject);

        when(table.get(anyList())).thenReturn(new Result[] { result });

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

        when(objectMapper.readValue(stringValueBytes, TestColumns.STRING_VALUE.getTestTypeReference()))
                .thenReturn(stringValue);

        final Cell booleanValueCell = mock(Cell.class);

        final byte[] booleanValueBytes = new byte[] {
                0
        };

        when(booleanValueCell.getValueArray()).thenReturn(booleanValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getQualifier()))).thenReturn(booleanValueCell);

        when(objectMapper.readValue(booleanValueBytes, TestColumns.BOOLEAN_VALUE.getTestTypeReference()))
                .thenReturn(booleanValue);

        final Cell nestedObjectCell = mock(Cell.class);

        final byte[] nestedObjectBytes = new byte[0];

        when(nestedObjectCell.getValueArray()).thenReturn(nestedObjectBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.NESTED_OBJECT.getFamily()),
                Bytes.toBytes(TestColumns.NESTED_OBJECT.getQualifier()))).thenReturn(nestedObjectCell);

        when(objectMapper.readValue(nestedObjectBytes, TestColumns.NESTED_OBJECT.getTestTypeReference()))
                .thenReturn(nestedObject);

        when(table.get(anyList())).thenReturn(new Result[] { result });

        final Optional<TestEntity> retrievedEntity = testEntityDao.get(new StringKey<>("key"));

        assertTrue(retrievedEntity.isPresent());

        final TestEntity entity = retrievedEntity.get();
        assertNull(entity.getNestedObject());
    }

    @Test
    public void testGetRetrievesNonExistentCellsAsNullValues() throws IOException {
        final Result result = mock(Result.class);
        when(result.isEmpty()).thenReturn(false);

        final Cell stringValueCell = null;

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.STRING_VALUE.getQualifier()))).thenReturn(stringValueCell);

        final Cell booleanValueCell = null;

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getQualifier()))).thenReturn(booleanValueCell);

        final Cell nestedObjectCell = null;

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.NESTED_OBJECT.getFamily()),
                Bytes.toBytes(TestColumns.NESTED_OBJECT.getQualifier()))).thenReturn(nestedObjectCell);

        when(table.get(anyList())).thenReturn(new Result[] { result });

        final Optional<TestEntity> retrievedEntity = testEntityDao.get(new StringKey<>("key"));

        assertTrue(retrievedEntity.isPresent());

        final TestEntity entity = retrievedEntity.get();
        assertNull(entity.getStringValue());
        assertNull(entity.getBooleanValue());
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

        when(objectMapper.readValue(stringValueBytes, TestVersionedColumns.STRING_VALUE.getTestTypeReference()))
                .thenReturn(stringValue);

        final Cell booleanValueCell = mock(Cell.class);

        final byte[] booleanValueBytes = new byte[] {
                0
        };

        when(booleanValueCell.getValueArray()).thenReturn(booleanValueBytes);
        when(booleanValueCell.getTimestamp()).thenReturn(booleanValueTimestamp);

        when(result.getColumnLatestCell(Bytes.toBytes(TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getQualifier()))).thenReturn(booleanValueCell);

        when(objectMapper.readValue(booleanValueBytes, TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getTestTypeReference()))
                .thenReturn(booleanValue);

        when(table.get(anyList())).thenReturn(new Result[] { result });

        final Optional<TestVersionedEntity> retrievedEntity = testVersionedEntityDao.get(new StringKey<>("key"));

        assertTrue(retrievedEntity.isPresent());

        final TestVersionedEntity entity = retrievedEntity.get();
        assertEquals(stringValue, entity.getStringValue());
        assertEquals(booleanValue, entity.getVersionedBooleanValue());
        assertEquals(booleanValueTimestamp, (long) entity.getVersionedBooleanValueTimestamp());
    }

    @Test
    public void testGetRetrievesNonExistentVersionedCellsAsNullValuesAndNullTimestamps() throws IOException {
        final Result result = mock(Result.class);
        when(result.isEmpty()).thenReturn(false);

        final Cell stringValueCell = null;

        when(result.getColumnLatestCell(Bytes.toBytes(TestVersionedColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestVersionedColumns.STRING_VALUE.getQualifier()))).thenReturn(stringValueCell);

        final Cell booleanValueCell = null;

        when(result.getColumnLatestCell(Bytes.toBytes(TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getQualifier()))).thenReturn(booleanValueCell);

        when(table.get(anyList())).thenReturn(new Result[] { result });

        final Optional<TestVersionedEntity> retrievedEntity = testVersionedEntityDao.get(new StringKey<>("key"));

        assertTrue(retrievedEntity.isPresent());

        final TestVersionedEntity entity = retrievedEntity.get();
        assertNull(entity.getStringValue());
        assertNull(entity.getVersionedBooleanValue());
        assertNull(entity.getVersionedBooleanValueTimestamp());
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

        when(table.get(anyList())).thenReturn(new Result[] { result });

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

        when(table.get(anyList())).thenReturn(new Result[] { result });

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

        when(table.get(anyList())).thenReturn(new Result[] { result });

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

        when(table.get(anyList())).thenReturn(new Result[] { result });

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

        when(table.get(anyList())).thenReturn(new Result[] { result });

        final Optional<TestEntity> retrievedEntity = testEntityDao.get(new StringKey<>("key"));

        assertFalse(retrievedEntity.isPresent());
    }

    @Test(expected = NullPointerException.class)
    public void testGetAllWithNullKeysThrowsNullPointerException() throws IOException {
        testEntityDao.getAll(null);
    }

    @Test
    public void testGetAllRetrievesAllColumns() throws IOException {
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

        when(objectMapper.readValue(stringValueBytes, TestColumns.STRING_VALUE.getTestTypeReference()))
                .thenReturn(stringValue);

        final Cell booleanValueCell = mock(Cell.class);

        final byte[] booleanValueBytes = new byte[] {
                0
        };

        when(booleanValueCell.getValueArray()).thenReturn(booleanValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getQualifier()))).thenReturn(booleanValueCell);

        when(objectMapper.readValue(booleanValueBytes, TestColumns.BOOLEAN_VALUE.getTestTypeReference()))
                .thenReturn(booleanValue);

        final Cell nestedObjectCell = mock(Cell.class);

        final byte[] nestedObjectBytes = new byte[] {
                5, 4, 3, 2, 1, 0
        };

        when(nestedObjectCell.getValueArray()).thenReturn(nestedObjectBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.NESTED_OBJECT.getFamily()),
                Bytes.toBytes(TestColumns.NESTED_OBJECT.getQualifier()))).thenReturn(nestedObjectCell);

        when(objectMapper.readValue(nestedObjectBytes, TestColumns.NESTED_OBJECT.getTestTypeReference()))
                .thenReturn(nestedObject);

        when(table.get(anyList())).thenReturn(new Result[] { result });

        final Key<TestEntity> key = new StringKey<>("key");
        final Set<Key<TestEntity>> keys = Collections.singleton(key);
        final Map<Key<TestEntity>, TestEntity> retrievedEntities = testEntityDao.getAll(keys);

        assertTrue(retrievedEntities.containsKey(key));

        final TestEntity entity = retrievedEntities.get(key);
        assertEquals(stringValue, entity.getStringValue());
        assertEquals(booleanValue, entity.getBooleanValue());
        assertEquals(nestedObject, entity.getNestedObject());
    }

    @Test
    public void testGetAllRetrievesAllColumnsForMultipleKeys() throws IOException {
        final String stringValue1 = "some string";

        final Boolean booleanValue1 = true;

        final TestNestedObject nestedObject1 = new TestNestedObject();
        nestedObject1.setSomeValue(3);

        final Result result1 = mock(Result.class);
        when(result1.isEmpty()).thenReturn(false);

        final Cell stringValueCell1 = mock(Cell.class);

        final byte[] stringValueBytes1 = new byte[] {
                1, 2, 3
        };

        when(stringValueCell1.getValueArray()).thenReturn(stringValueBytes1);

        when(result1.getColumnLatestCell(Bytes.toBytes(TestColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.STRING_VALUE.getQualifier()))).thenReturn(stringValueCell1);

        when(objectMapper.readValue(stringValueBytes1, TestColumns.STRING_VALUE.getTestTypeReference()))
                .thenReturn(stringValue1);

        final Cell booleanValueCell1 = mock(Cell.class);

        final byte[] booleanValueBytes1 = new byte[] {
                0
        };

        when(booleanValueCell1.getValueArray()).thenReturn(booleanValueBytes1);

        when(result1.getColumnLatestCell(Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getQualifier()))).thenReturn(booleanValueCell1);

        when(objectMapper.readValue(booleanValueBytes1, TestColumns.BOOLEAN_VALUE.getTestTypeReference()))
                .thenReturn(booleanValue1);

        final Cell nestedObjectCell1 = mock(Cell.class);

        final byte[] nestedObjectBytes1 = new byte[] {
                5, 4, 3, 2, 1, 0
        };

        when(nestedObjectCell1.getValueArray()).thenReturn(nestedObjectBytes1);

        when(result1.getColumnLatestCell(Bytes.toBytes(TestColumns.NESTED_OBJECT.getFamily()),
                Bytes.toBytes(TestColumns.NESTED_OBJECT.getQualifier()))).thenReturn(nestedObjectCell1);

        when(objectMapper.readValue(nestedObjectBytes1, TestColumns.NESTED_OBJECT.getTestTypeReference()))
                .thenReturn(nestedObject1);

        final String stringValue2 = "some other string";

        final Boolean booleanValue2 = false;

        final TestNestedObject nestedObject2 = new TestNestedObject();
        nestedObject2.setSomeValue(8);

        final Result result2 = mock(Result.class);
        when(result2.isEmpty()).thenReturn(false);

        final Cell stringValueCell2 = mock(Cell.class);

        final byte[] stringValueBytes2 = new byte[] {
                9, 8, 7
        };

        when(stringValueCell2.getValueArray()).thenReturn(stringValueBytes2);

        when(result2.getColumnLatestCell(Bytes.toBytes(TestColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.STRING_VALUE.getQualifier()))).thenReturn(stringValueCell2);

        when(objectMapper.readValue(stringValueBytes2, TestColumns.STRING_VALUE.getTestTypeReference()))
                .thenReturn(stringValue2);

        final Cell booleanValueCell2 = mock(Cell.class);

        final byte[] booleanValueBytes2 = new byte[] {
                9
        };

        when(booleanValueCell2.getValueArray()).thenReturn(booleanValueBytes2);

        when(result2.getColumnLatestCell(Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getQualifier()))).thenReturn(booleanValueCell2);

        when(objectMapper.readValue(booleanValueBytes2, TestColumns.BOOLEAN_VALUE.getTestTypeReference()))
                .thenReturn(booleanValue2);

        final Cell nestedObjectCell2 = mock(Cell.class);

        final byte[] nestedObjectBytes2 = new byte[] {
                3, 3, 3, 3, 3
        };

        when(nestedObjectCell2.getValueArray()).thenReturn(nestedObjectBytes2);

        when(result2.getColumnLatestCell(Bytes.toBytes(TestColumns.NESTED_OBJECT.getFamily()),
                Bytes.toBytes(TestColumns.NESTED_OBJECT.getQualifier()))).thenReturn(nestedObjectCell2);

        when(objectMapper.readValue(nestedObjectBytes2, TestColumns.NESTED_OBJECT.getTestTypeReference()))
                .thenReturn(nestedObject2);

        final Key<TestEntity> key1 = new StringKey<>("key1");
        final Key<TestEntity> key2 = new StringKey<>("key2");

        // The keys are passed in as a Set to remove duplicates, but converted to a List for use with the HBase API.
        // The HBase API returns an array of Results, with indices corresponding to the List of keys.
        // Since the conversion from a Set to a List does not maintain order, we need to mock this interaction manually.
        when(table.get(anyList())).then(invocation -> {
            final List<Get> gets = invocation.getArgument(0);

            if (Arrays.equals(gets.get(0).getRow(), key1.toBytes())) {
                return new Result[] { result1, result2 };
            } else {
                return new Result[] { result2, result1 };
            }
        });

        final Set<Key<TestEntity>> keys = Stream.of(key1, key2).collect(Collectors.toSet());
        final Map<Key<TestEntity>, TestEntity> retrievedEntities = testEntityDao.getAll(keys);

        assertTrue(retrievedEntities.containsKey(key1));

        final TestEntity entity1 = retrievedEntities.get(key1);
        assertEquals(stringValue1, entity1.getStringValue());
        assertEquals(booleanValue1, entity1.getBooleanValue());
        assertEquals(nestedObject1, entity1.getNestedObject());

        assertTrue(retrievedEntities.containsKey(key2));

        final TestEntity entity2 = retrievedEntities.get(key2);
        assertEquals(stringValue2, entity2.getStringValue());
        assertEquals(booleanValue2, entity2.getBooleanValue());
        assertEquals(nestedObject2, entity2.getNestedObject());
    }

    @Test
    public void testGetAllRetrievesNullValues() throws IOException {
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

        when(objectMapper.readValue(stringValueBytes, TestColumns.STRING_VALUE.getTestTypeReference()))
                .thenReturn(stringValue);

        final Cell booleanValueCell = mock(Cell.class);

        final byte[] booleanValueBytes = new byte[] {
                0
        };

        when(booleanValueCell.getValueArray()).thenReturn(booleanValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getQualifier()))).thenReturn(booleanValueCell);

        when(objectMapper.readValue(booleanValueBytes, TestColumns.BOOLEAN_VALUE.getTestTypeReference()))
                .thenReturn(booleanValue);

        final Cell nestedObjectCell = mock(Cell.class);

        final byte[] nestedObjectBytes = new byte[0];

        when(nestedObjectCell.getValueArray()).thenReturn(nestedObjectBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.NESTED_OBJECT.getFamily()),
                Bytes.toBytes(TestColumns.NESTED_OBJECT.getQualifier()))).thenReturn(nestedObjectCell);

        when(objectMapper.readValue(nestedObjectBytes, TestColumns.NESTED_OBJECT.getTestTypeReference()))
                .thenReturn(nestedObject);

        when(table.get(anyList())).thenReturn(new Result[] { result });

        final Key<TestEntity> key = new StringKey<>("key");
        final Set<Key<TestEntity>> keys = Collections.singleton(key);
        final Map<Key<TestEntity>, TestEntity> retrievedEntities = testEntityDao.getAll(keys);

        assertTrue(retrievedEntities.containsKey(key));

        final TestEntity entity = retrievedEntities.get(key);
        assertNull(entity.getNestedObject());
    }

    @Test
    public void testGetAllRetrievesNonExistentCellsAsNullValues() throws IOException {
        final Result result = mock(Result.class);
        when(result.isEmpty()).thenReturn(false);

        final Cell stringValueCell = null;

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.STRING_VALUE.getQualifier()))).thenReturn(stringValueCell);

        final Cell booleanValueCell = null;

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getQualifier()))).thenReturn(booleanValueCell);

        final Cell nestedObjectCell = null;

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.NESTED_OBJECT.getFamily()),
                Bytes.toBytes(TestColumns.NESTED_OBJECT.getQualifier()))).thenReturn(nestedObjectCell);

        when(table.get(anyList())).thenReturn(new Result[] { result });

        final Key<TestEntity> key = new StringKey<>("key");
        final Set<Key<TestEntity>> keys = Collections.singleton(key);
        final Map<Key<TestEntity>, TestEntity> retrievedEntities = testEntityDao.getAll(keys);

        assertTrue(retrievedEntities.containsKey(key));

        final TestEntity entity = retrievedEntities.get(key);
        assertNull(entity.getStringValue());
        assertNull(entity.getBooleanValue());
        assertNull(entity.getNestedObject());
    }

    @Test
    public void testGetAllRetrievesVersionedValuesAndTimestamps() throws IOException {
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

        when(objectMapper.readValue(stringValueBytes, TestVersionedColumns.STRING_VALUE.getTestTypeReference()))
                .thenReturn(stringValue);

        final Cell booleanValueCell = mock(Cell.class);

        final byte[] booleanValueBytes = new byte[] {
                0
        };

        when(booleanValueCell.getValueArray()).thenReturn(booleanValueBytes);
        when(booleanValueCell.getTimestamp()).thenReturn(booleanValueTimestamp);

        when(result.getColumnLatestCell(Bytes.toBytes(TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getQualifier()))).thenReturn(booleanValueCell);

        when(objectMapper.readValue(booleanValueBytes, TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getTestTypeReference()))
                .thenReturn(booleanValue);

        when(table.get(anyList())).thenReturn(new Result[] { result });

        final Key<TestVersionedEntity> key = new StringKey<>("key");
        final Set<Key<TestVersionedEntity>> keys = Collections.singleton(key);
        final Map<Key<TestVersionedEntity>, TestVersionedEntity> retrievedEntities = testVersionedEntityDao.getAll(keys);

        assertTrue(retrievedEntities.containsKey(key));

        final TestVersionedEntity entity = retrievedEntities.get(key);
        assertEquals(stringValue, entity.getStringValue());
        assertEquals(booleanValue, entity.getVersionedBooleanValue());
        assertEquals(booleanValueTimestamp, (long) entity.getVersionedBooleanValueTimestamp());
    }

    @Test
    public void testGetAllRetrievesNonExistentVersionedCellsAsNullValuesAndNullTimestamps() throws IOException {
        final Result result = mock(Result.class);
        when(result.isEmpty()).thenReturn(false);

        final Cell stringValueCell = null;

        when(result.getColumnLatestCell(Bytes.toBytes(TestVersionedColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestVersionedColumns.STRING_VALUE.getQualifier()))).thenReturn(stringValueCell);

        final Cell booleanValueCell = null;

        when(result.getColumnLatestCell(Bytes.toBytes(TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getQualifier()))).thenReturn(booleanValueCell);

        when(table.get(anyList())).thenReturn(new Result[] { result });

        final Key<TestVersionedEntity> key = new StringKey<>("key");
        final Set<Key<TestVersionedEntity>> keys = Collections.singleton(key);
        final Map<Key<TestVersionedEntity>, TestVersionedEntity> retrievedEntities = testVersionedEntityDao.getAll(keys);

        assertTrue(retrievedEntities.containsKey(key));

        final TestVersionedEntity entity = retrievedEntities.get(key);
        assertNull(entity.getStringValue());
        assertNull(entity.getVersionedBooleanValue());
        assertNull(entity.getVersionedBooleanValueTimestamp());
    }

    @Test
    public void testGetAllWithLiveObjectMapper() throws IOException {
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

        when(table.get(anyList())).thenReturn(new Result[] { result });

        final Key<TestEntity> key = new StringKey<>("key");
        final Set<Key<TestEntity>> keys = Collections.singleton(key);
        final Map<Key<TestEntity>, TestEntity> retrievedEntities = testEntityDao.getAll(keys);

        assertTrue(retrievedEntities.containsKey(key));

        final TestEntity entity = retrievedEntities.get(key);
        assertEquals(stringValue, entity.getStringValue());
        assertEquals(booleanValue, entity.getBooleanValue());
        assertEquals(nestedObject, entity.getNestedObject());
    }

    @Test
    public void testGetAllRetrievesNullStringValueWithLiveObjectMapper() throws IOException {
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

        when(table.get(anyList())).thenReturn(new Result[] { result });

        final Key<TestEntity> key = new StringKey<>("key");
        final Set<Key<TestEntity>> keys = Collections.singleton(key);
        final Map<Key<TestEntity>, TestEntity> retrievedEntities = testEntityDao.getAll(keys);

        assertTrue(retrievedEntities.containsKey(key));

        final TestEntity entity = retrievedEntities.get(key);
        assertEquals(stringValue, entity.getStringValue());
        assertEquals(booleanValue, entity.getBooleanValue());
        assertEquals(nestedObject, entity.getNestedObject());
    }

    @Test
    public void testGetAllRetrievesNullBooleanValueWithLiveObjectMapper() throws IOException {
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

        when(table.get(anyList())).thenReturn(new Result[] { result });

        final Key<TestEntity> key = new StringKey<>("key");
        final Set<Key<TestEntity>> keys = Collections.singleton(key);
        final Map<Key<TestEntity>, TestEntity> retrievedEntities = testEntityDao.getAll(keys);

        assertTrue(retrievedEntities.containsKey(key));

        final TestEntity entity = retrievedEntities.get(key);
        assertEquals(stringValue, entity.getStringValue());
        assertEquals(booleanValue, entity.getBooleanValue());
        assertEquals(nestedObject, entity.getNestedObject());
    }

    @Test
    public void testGetAllRetrievesNullNestedObjectWithLiveObjectMapper() throws IOException {
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

        when(table.get(anyList())).thenReturn(new Result[] { result });

        final Key<TestEntity> key = new StringKey<>("key");
        final Set<Key<TestEntity>> keys = Collections.singleton(key);
        final Map<Key<TestEntity>, TestEntity> retrievedEntities = testEntityDao.getAll(keys);

        assertTrue(retrievedEntities.containsKey(key));

        final TestEntity entity = retrievedEntities.get(key);
        assertEquals(stringValue, entity.getStringValue());
        assertEquals(booleanValue, entity.getBooleanValue());
        assertEquals(nestedObject, entity.getNestedObject());
    }

    @Test
    public void testGetAllDoesNotContainKeyWhenRowDoesNotExist() throws IOException {
        final Result result = mock(Result.class);
        when(result.isEmpty()).thenReturn(true);

        when(table.get(anyList())).thenReturn(new Result[] { result });

        final Key<TestEntity> key = new StringKey<>("key");
        final Set<Key<TestEntity>> keys = Collections.singleton(key);
        final Map<Key<TestEntity>, TestEntity> retrievedEntities = testEntityDao.getAll(keys);

        assertFalse(retrievedEntities.containsKey(key));
    }

    @Test(expected = NullPointerException.class)
    public void testScanWithNullStartKeyThrowsNullPointerException() throws IOException {
        Key<TestEntity> endKey = new StringKey<>("key");

        testEntityDao.scan(null, true, endKey, true, 1);
    }

    @Test(expected = NullPointerException.class)
    public void testScanWithNullEndKeyThrowsNullPointerException() throws IOException {
        Key<TestEntity> startKey = new StringKey<>("key");

        testEntityDao.scan(startKey, true, null, true, 1);
    }

    @Test
    public void testScanRetrievesAllColumns() throws IOException {
        final Key<TestEntity> startKey = new StringKey<>("a"); // [97]
        final Key<TestEntity> endKey = new StringKey<>("z"); // [122]
        final String stringValue = "some string";

        final Boolean booleanValue = true;

        final TestNestedObject nestedObject = new TestNestedObject();
        nestedObject.setSomeValue(3);

        final Result result = mock(Result.class);
        when(result.getRow()).thenReturn(startKey.toBytes());
        when(result.isEmpty()).thenReturn(false);

        final Cell stringValueCell = mock(Cell.class);

        final byte[] stringValueBytes = new byte[] {
                1, 2, 3
        };

        when(stringValueCell.getValueArray()).thenReturn(stringValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.STRING_VALUE.getQualifier()))).thenReturn(stringValueCell);

        when(objectMapper.readValue(stringValueBytes, TestColumns.STRING_VALUE.getTestTypeReference()))
                .thenReturn(stringValue);

        final Cell booleanValueCell = mock(Cell.class);

        final byte[] booleanValueBytes = new byte[] {
                0
        };

        when(booleanValueCell.getValueArray()).thenReturn(booleanValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getQualifier()))).thenReturn(booleanValueCell);

        when(objectMapper.readValue(booleanValueBytes, TestColumns.BOOLEAN_VALUE.getTestTypeReference()))
                .thenReturn(booleanValue);

        final Cell nestedObjectCell = mock(Cell.class);

        final byte[] nestedObjectBytes = new byte[] {
                5, 4, 3, 2, 1, 0
        };

        when(nestedObjectCell.getValueArray()).thenReturn(nestedObjectBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.NESTED_OBJECT.getFamily()),
                Bytes.toBytes(TestColumns.NESTED_OBJECT.getQualifier()))).thenReturn(nestedObjectCell);

        when(objectMapper.readValue(nestedObjectBytes, TestColumns.NESTED_OBJECT.getTestTypeReference()))
                .thenReturn(nestedObject);

        when(table.getScanner(any(Scan.class))).thenReturn(scanner);
        when(scanner.next()).thenAnswer(new Answer() {
            private int count = 0;

            @Override
            public Object answer(InvocationOnMock invocation) {
                if (count == 0) {
                    count++;
                    return result;
                }

                return null;
            }
        });

        final SortedMap<Key<TestEntity>, TestEntity> retrievedEntities =
                testEntityDao.scan(startKey, true, endKey, true, 1);

        assertEquals(1, retrievedEntities.size());

        final TestEntity entity = retrievedEntities.get(retrievedEntities.firstKey());
        assertEquals(stringValue, entity.getStringValue());
        assertEquals(booleanValue, entity.getBooleanValue());
        assertEquals(nestedObject, entity.getNestedObject());
    }

    @Test
    public void testScanRetrievesAllColumnsForMultipleRows() throws IOException {
        final String stringValue1 = "some string";
        final Key<TestEntity> startKey = new StringKey<>("a"); // [97]
        final Key<TestEntity> endKey = new StringKey<>("z"); // [122]

        final Boolean booleanValue1 = true;

        final TestNestedObject nestedObject1 = new TestNestedObject();
        nestedObject1.setSomeValue(3);

        final Result result1 = mock(Result.class);
        when(result1.getRow()).thenReturn(startKey.toBytes());
        when(result1.isEmpty()).thenReturn(false);

        final Cell stringValueCell1 = mock(Cell.class);

        final byte[] stringValueBytes1 = new byte[] {
                1, 2, 3
        };

        when(stringValueCell1.getValueArray()).thenReturn(stringValueBytes1);

        when(result1.getColumnLatestCell(Bytes.toBytes(TestColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.STRING_VALUE.getQualifier()))).thenReturn(stringValueCell1);

        when(objectMapper.readValue(stringValueBytes1, TestColumns.STRING_VALUE.getTestTypeReference()))
                .thenReturn(stringValue1);

        final Cell booleanValueCell1 = mock(Cell.class);

        final byte[] booleanValueBytes1 = new byte[] {
                0
        };

        when(booleanValueCell1.getValueArray()).thenReturn(booleanValueBytes1);

        when(result1.getColumnLatestCell(Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getQualifier()))).thenReturn(booleanValueCell1);

        when(objectMapper.readValue(booleanValueBytes1, TestColumns.BOOLEAN_VALUE.getTestTypeReference()))
                .thenReturn(booleanValue1);

        final Cell nestedObjectCell1 = mock(Cell.class);

        final byte[] nestedObjectBytes1 = new byte[] {
                5, 4, 3, 2, 1, 0
        };

        when(nestedObjectCell1.getValueArray()).thenReturn(nestedObjectBytes1);

        when(result1.getColumnLatestCell(Bytes.toBytes(TestColumns.NESTED_OBJECT.getFamily()),
                Bytes.toBytes(TestColumns.NESTED_OBJECT.getQualifier()))).thenReturn(nestedObjectCell1);

        when(objectMapper.readValue(nestedObjectBytes1, TestColumns.NESTED_OBJECT.getTestTypeReference()))
                .thenReturn(nestedObject1);

        final String stringValue2 = "some other string";

        final Boolean booleanValue2 = false;

        final TestNestedObject nestedObject2 = new TestNestedObject();
        nestedObject2.setSomeValue(8);

        final Result result2 = mock(Result.class);
        when(result2.getRow()).thenReturn(endKey.toBytes());
        when(result2.isEmpty()).thenReturn(false);

        final Cell stringValueCell2 = mock(Cell.class);

        final byte[] stringValueBytes2 = new byte[] {
                9, 8, 7
        };

        when(stringValueCell2.getValueArray()).thenReturn(stringValueBytes2);

        when(result2.getColumnLatestCell(Bytes.toBytes(TestColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.STRING_VALUE.getQualifier()))).thenReturn(stringValueCell2);

        when(objectMapper.readValue(stringValueBytes2, TestColumns.STRING_VALUE.getTestTypeReference()))
                .thenReturn(stringValue2);

        final Cell booleanValueCell2 = mock(Cell.class);

        final byte[] booleanValueBytes2 = new byte[] {
                9
        };

        when(booleanValueCell2.getValueArray()).thenReturn(booleanValueBytes2);

        when(result2.getColumnLatestCell(Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getQualifier()))).thenReturn(booleanValueCell2);

        when(objectMapper.readValue(booleanValueBytes2, TestColumns.BOOLEAN_VALUE.getTestTypeReference()))
                .thenReturn(booleanValue2);

        final Cell nestedObjectCell2 = mock(Cell.class);

        final byte[] nestedObjectBytes2 = new byte[] {
                3, 3, 3, 3, 3
        };

        when(nestedObjectCell2.getValueArray()).thenReturn(nestedObjectBytes2);

        when(result2.getColumnLatestCell(Bytes.toBytes(TestColumns.NESTED_OBJECT.getFamily()),
                Bytes.toBytes(TestColumns.NESTED_OBJECT.getQualifier()))).thenReturn(nestedObjectCell2);

        when(objectMapper.readValue(nestedObjectBytes2, TestColumns.NESTED_OBJECT.getTestTypeReference()))
                .thenReturn(nestedObject2);

        when(table.getScanner(any(Scan.class))).thenReturn(scanner);
        when(scanner.next()).thenAnswer(new Answer() {
            private int count = 0;

            @Override
            public Object answer(InvocationOnMock invocation) {
                switch (count) {
                    case 0:
                        count++;
                        return result1;
                    case 1:
                        count++;
                        return result2;
                    default:
                        count++;
                        return null;
                }
            }
        });

        final SortedMap<Key<TestEntity>, TestEntity> retrievedEntities = testEntityDao.scan(startKey, true, endKey, true, 5);

        assertEquals(2, retrievedEntities.size());

        final TestEntity entity1 = retrievedEntities.get(retrievedEntities.firstKey());
        assertEquals(stringValue1, entity1.getStringValue());
        assertEquals(booleanValue1, entity1.getBooleanValue());
        assertEquals(nestedObject1, entity1.getNestedObject());

        final TestEntity entity2 = retrievedEntities.get(retrievedEntities.lastKey());
        assertEquals(stringValue2, entity2.getStringValue());
        assertEquals(booleanValue2, entity2.getBooleanValue());
        assertEquals(nestedObject2, entity2.getNestedObject());
    }

    @Test
    public void testScanRetrievesNullValues() throws IOException {
        final String stringValue = "some string";
        final Key<TestEntity> startKey = new StringKey<>("a"); // [97]
        final Key<TestEntity> endKey = new StringKey<>("z"); // [122]

        final Boolean booleanValue = true;

        final TestNestedObject nestedObject = null;

        final Result result = mock(Result.class);
        when(result.getRow()).thenReturn(startKey.toBytes());
        when(result.isEmpty()).thenReturn(false);

        final Cell stringValueCell = mock(Cell.class);

        final byte[] stringValueBytes = new byte[] {
                1, 2, 3
        };

        when(stringValueCell.getValueArray()).thenReturn(stringValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.STRING_VALUE.getQualifier()))).thenReturn(stringValueCell);

        when(objectMapper.readValue(stringValueBytes, TestColumns.STRING_VALUE.getTestTypeReference()))
                .thenReturn(stringValue);

        final Cell booleanValueCell = mock(Cell.class);

        final byte[] booleanValueBytes = new byte[] {
                0
        };

        when(booleanValueCell.getValueArray()).thenReturn(booleanValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getQualifier()))).thenReturn(booleanValueCell);

        when(objectMapper.readValue(booleanValueBytes, TestColumns.BOOLEAN_VALUE.getTestTypeReference()))
                .thenReturn(booleanValue);

        final Cell nestedObjectCell = mock(Cell.class);

        final byte[] nestedObjectBytes = new byte[0];

        when(nestedObjectCell.getValueArray()).thenReturn(nestedObjectBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.NESTED_OBJECT.getFamily()),
                Bytes.toBytes(TestColumns.NESTED_OBJECT.getQualifier()))).thenReturn(nestedObjectCell);

        when(objectMapper.readValue(nestedObjectBytes, TestColumns.NESTED_OBJECT.getTestTypeReference()))
                .thenReturn(nestedObject);

        when(table.getScanner(any(Scan.class))).thenReturn(scanner);
        when(scanner.next()).thenAnswer(new Answer() {
            private int count = 0;

            @Override
            public Object answer(InvocationOnMock invocation) {
                if (count == 0) {
                    count++;
                    return result;
                }

                return null;
            }
        });

        final SortedMap<Key<TestEntity>, TestEntity> retrievedEntities = testEntityDao.scan(startKey, true, endKey, true, 1);

        assertEquals(1, retrievedEntities.size());

        final TestEntity entity = retrievedEntities.get(retrievedEntities.firstKey());
        assertNull(entity.getNestedObject());
    }

    @Test
    public void testScanRetrievesNonExistentCellsAsNullValues() throws IOException {
        final Key<TestEntity> startKey = new StringKey<>("a"); // [97]
        final Key<TestEntity> endKey = new StringKey<>("z"); // [122]

        final Result result = mock(Result.class);
        when(result.getRow()).thenReturn(startKey.toBytes());
        when(result.isEmpty()).thenReturn(false);

        final Cell stringValueCell = null;

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.STRING_VALUE.getQualifier()))).thenReturn(stringValueCell);

        final Cell booleanValueCell = null;

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getQualifier()))).thenReturn(booleanValueCell);

        final Cell nestedObjectCell = null;

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.NESTED_OBJECT.getFamily()),
                Bytes.toBytes(TestColumns.NESTED_OBJECT.getQualifier()))).thenReturn(nestedObjectCell);

        when(table.getScanner(any(Scan.class))).thenReturn(scanner);
        when(scanner.next()).thenAnswer(new Answer() {
            private int count = 0;

            @Override
            public Object answer(InvocationOnMock invocation) {
                if (count == 0) {
                    count++;
                    return result;
                }

                return null;
            }
        });

        final SortedMap<Key<TestEntity>, TestEntity> retrievedEntities = testEntityDao.scan(startKey, true, endKey, true, 1);

        assertEquals(1, retrievedEntities.size());

        final TestEntity entity = retrievedEntities.get(retrievedEntities.firstKey());
        assertNull(entity.getStringValue());
        assertNull(entity.getBooleanValue());
        assertNull(entity.getNestedObject());
    }

    @Test
    public void testScanRetrievesVersionedValuesAndTimestamps() throws IOException {
        final String stringValue = "some string";
        final Key<TestVersionedEntity> startKey = new StringKey<>("a"); // [97]
        final Key<TestVersionedEntity> endKey = new StringKey<>("z"); // [122]

        final Boolean booleanValue = true;
        final long booleanValueTimestamp = 1234L;

        final Result result = mock(Result.class);
        when(result.getRow()).thenReturn(startKey.toBytes());
        when(result.isEmpty()).thenReturn(false);

        final Cell stringValueCell = mock(Cell.class);

        final byte[] stringValueBytes = new byte[] {
                1, 2, 3
        };

        when(stringValueCell.getValueArray()).thenReturn(stringValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestVersionedColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestVersionedColumns.STRING_VALUE.getQualifier()))).thenReturn(stringValueCell);

        when(objectMapper.readValue(stringValueBytes, TestVersionedColumns.STRING_VALUE.getTestTypeReference()))
                .thenReturn(stringValue);

        final Cell booleanValueCell = mock(Cell.class);

        final byte[] booleanValueBytes = new byte[] {
                0
        };

        when(booleanValueCell.getValueArray()).thenReturn(booleanValueBytes);
        when(booleanValueCell.getTimestamp()).thenReturn(booleanValueTimestamp);

        when(result.getColumnLatestCell(Bytes.toBytes(TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getQualifier()))).thenReturn(booleanValueCell);

        when(objectMapper.readValue(booleanValueBytes, TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getTestTypeReference()))
                .thenReturn(booleanValue);

        when(table.getScanner(any(Scan.class))).thenReturn(scanner);
        when(scanner.next()).thenAnswer(new Answer() {
            private int count = 0;

            @Override
            public Object answer(InvocationOnMock invocation) {
                if (count == 0) {
                    count++;
                    return result;
                }

                return null;
            }
        });

        final SortedMap<Key<TestVersionedEntity>, TestVersionedEntity> retrievedEntities =
                testVersionedEntityDao.scan(startKey, true, endKey, true, 1);

        assertEquals(1, retrievedEntities.size());

        final TestVersionedEntity entity = retrievedEntities.get(retrievedEntities.firstKey());
        assertEquals(stringValue, entity.getStringValue());
        assertEquals(booleanValue, entity.getVersionedBooleanValue());
        assertEquals(booleanValueTimestamp, (long) entity.getVersionedBooleanValueTimestamp());
    }

    @Test
    public void testScanRetrievesNonExistentVersionedCellsAsNullValuesAndNullTimestamps() throws IOException {
        final Key<TestVersionedEntity> startKey = new StringKey<>("a"); // [97]
        final Key<TestVersionedEntity> endKey = new StringKey<>("z"); // [122]

        final Result result = mock(Result.class);
        when(result.getRow()).thenReturn(startKey.toBytes());
        when(result.isEmpty()).thenReturn(false);

        final Cell stringValueCell = null;

        when(result.getColumnLatestCell(Bytes.toBytes(TestVersionedColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestVersionedColumns.STRING_VALUE.getQualifier()))).thenReturn(stringValueCell);

        final Cell booleanValueCell = null;

        when(result.getColumnLatestCell(Bytes.toBytes(TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getQualifier()))).thenReturn(booleanValueCell);

        when(table.getScanner(any(Scan.class))).thenReturn(scanner);
        when(scanner.next()).thenAnswer(new Answer() {
            private int count = 0;

            @Override
            public Object answer(InvocationOnMock invocation) {
                if (count == 0) {
                    count++;
                    return result;
                }

                return null;
            }
        });

        final SortedMap<Key<TestVersionedEntity>,TestVersionedEntity> retrievedEntities = testVersionedEntityDao.scan(startKey, true, endKey, true, 1);

        assertEquals(1, retrievedEntities.size());

        final TestVersionedEntity entity = retrievedEntities.get(retrievedEntities.firstKey());
        assertNull(entity.getStringValue());
        assertNull(entity.getVersionedBooleanValue());
        assertNull(entity.getVersionedBooleanValueTimestamp());
    }

    @Test
    public void testScanWithLiveObjectMapper() throws IOException {
        final Key<TestEntity> startKey = new StringKey<>("a"); // [97]
        final Key<TestEntity> endKey = new StringKey<>("z"); // [122]
        testEntityDao = new BigTableEntityDao<>(table, columns, entityFactory, delegateFactory);

        final String stringValue = "some string";

        final Boolean booleanValue = true;

        final TestNestedObject nestedObject = new TestNestedObject();
        nestedObject.setSomeValue(3);

        final Result result = mock(Result.class);
        when(result.getRow()).thenReturn(startKey.toBytes());
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

        when(table.getScanner(any(Scan.class))).thenReturn(scanner);
        when(scanner.next()).thenAnswer(new Answer() {
            private int count = 0;

            @Override
            public Object answer(InvocationOnMock invocation) {
                if (count == 0) {
                    count++;
                    return result;
                }

                return null;
            }
        });

        final SortedMap<Key<TestEntity>, TestEntity> retrievedEntities = testEntityDao.scan(startKey, true, endKey, true, 1);

        assertEquals(1, retrievedEntities.size());

        final TestEntity entity = retrievedEntities.get(retrievedEntities.firstKey());
        assertEquals(stringValue, entity.getStringValue());
        assertEquals(booleanValue, entity.getBooleanValue());
        assertEquals(nestedObject, entity.getNestedObject());
    }

    @Test
    public void testScanRetrievesNullStringValueWithLiveObjectMapper() throws IOException {
        final Key<TestEntity> startKey = new StringKey<>("a"); // [97]
        final Key<TestEntity> endKey = new StringKey<>("z"); // [122]
        testEntityDao = new BigTableEntityDao<>(table, columns, entityFactory, delegateFactory);

        final String stringValue = null;

        final Boolean booleanValue = true;

        final TestNestedObject nestedObject = new TestNestedObject();
        nestedObject.setSomeValue(3);

        final Result result = mock(Result.class);
        when(result.getRow()).thenReturn(startKey.toBytes());
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

        when(table.getScanner(any(Scan.class))).thenReturn(scanner);
        when(scanner.next()).thenAnswer(new Answer() {
            private int count = 0;

            @Override
            public Object answer(InvocationOnMock invocation) {
                if (count == 0) {
                    count++;
                    return result;
                }

                return null;
            }
        });

        final SortedMap<Key<TestEntity>, TestEntity> retrievedEntities = testEntityDao.scan(startKey, true, endKey, true, 1);

        assertEquals(1, retrievedEntities.size());

        final TestEntity entity = retrievedEntities.get(retrievedEntities.firstKey());
        assertEquals(stringValue, entity.getStringValue());
        assertEquals(booleanValue, entity.getBooleanValue());
        assertEquals(nestedObject, entity.getNestedObject());
    }

    @Test
    public void testScanRetrievesNullBooleanValueWithLiveObjectMapper() throws IOException {
        final Key<TestEntity> startKey = new StringKey<>("a"); // [97]
        final Key<TestEntity> endKey = new StringKey<>("z"); // [122]
        testEntityDao = new BigTableEntityDao<>(table, columns, entityFactory, delegateFactory);

        final String stringValue = "some string";

        final Boolean booleanValue = null;

        final TestNestedObject nestedObject = new TestNestedObject();
        nestedObject.setSomeValue(3);

        final Result result = mock(Result.class);
        when(result.getRow()).thenReturn(startKey.toBytes());
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

        when(table.getScanner(any(Scan.class))).thenReturn(scanner);
        when(scanner.next()).thenAnswer(new Answer() {
            private int count = 0;

            @Override
            public Object answer(InvocationOnMock invocation) {
                if (count == 0) {
                    count++;
                    return result;
                }

                return null;
            }
        });

        final SortedMap<Key<TestEntity>, TestEntity> retrievedEntities = testEntityDao.scan(startKey, true, endKey, true, 1);

        assertEquals(1, retrievedEntities.size());

        final TestEntity entity = retrievedEntities.get(retrievedEntities.firstKey());
        assertEquals(stringValue, entity.getStringValue());
        assertEquals(booleanValue, entity.getBooleanValue());
        assertEquals(nestedObject, entity.getNestedObject());
    }

    @Test
    public void testScanRetrievesNullNestedObjectWithLiveObjectMapper() throws IOException {
        final Key<TestEntity> startKey = new StringKey<>("a"); // [97]
        final Key<TestEntity> endKey = new StringKey<>("z"); // [122]

        testEntityDao = new BigTableEntityDao<>(table, columns, entityFactory, delegateFactory);

        final String stringValue = "some string";

        final Boolean booleanValue = true;

        final TestNestedObject nestedObject = null;

        final Result result = mock(Result.class);
        when(result.getRow()).thenReturn(startKey.toBytes());
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

        when(table.getScanner(any(Scan.class))).thenReturn(scanner);
        when(scanner.next()).thenAnswer(new Answer() {
            private int count = 0;

            @Override
            public Object answer(InvocationOnMock invocation) {
                if (count == 0) {
                    count++;
                    return result;
                }

                return null;
            }
        });

        final SortedMap<Key<TestEntity>, TestEntity> retrievedEntities = testEntityDao.scan(startKey, true, endKey, true, 1);

        assertEquals(1, retrievedEntities.size());

        final TestEntity entity = retrievedEntities.get(retrievedEntities.firstKey());
        assertEquals(stringValue, entity.getStringValue());
        assertEquals(booleanValue, entity.getBooleanValue());
        assertEquals(nestedObject, entity.getNestedObject());
    }

    @Test
    public void testScanRowFiltersOnConstant() throws IOException {
        final Key<TestEntity> startKey = new StringKey<>("a"); // [97]
        final Key<TestEntity> endKey = new StringKey<>("z"); // [122]
        final String constant = "constant";
        final String stringValue = "some string";

        final Boolean booleanValue = true;

        final TestNestedObject nestedObject = new TestNestedObject();
        nestedObject.setSomeValue(3);

        final Result result = mock(Result.class);
        when(result.getRow()).thenReturn(startKey.toBytes());
        when(result.isEmpty()).thenReturn(false);

        final Cell stringValueCell = mock(Cell.class);

        final byte[] stringValueBytes = new byte[] {
                1, 2, 3
        };

        when(stringValueCell.getValueArray()).thenReturn(stringValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.STRING_VALUE.getQualifier()))).thenReturn(stringValueCell);

        when(objectMapper.readValue(stringValueBytes, TestColumns.STRING_VALUE.getTestTypeReference()))
                .thenReturn(stringValue);

        final Cell booleanValueCell = mock(Cell.class);

        final byte[] booleanValueBytes = new byte[] {
                0
        };

        when(booleanValueCell.getValueArray()).thenReturn(booleanValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getQualifier()))).thenReturn(booleanValueCell);

        when(objectMapper.readValue(booleanValueBytes, TestColumns.BOOLEAN_VALUE.getTestTypeReference()))
                .thenReturn(booleanValue);

        final Cell nestedObjectCell = mock(Cell.class);

        final byte[] nestedObjectBytes = new byte[] {
                5, 4, 3, 2, 1, 0
        };

        when(nestedObjectCell.getValueArray()).thenReturn(nestedObjectBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.NESTED_OBJECT.getFamily()),
                Bytes.toBytes(TestColumns.NESTED_OBJECT.getQualifier()))).thenReturn(nestedObjectCell);

        when(objectMapper.readValue(nestedObjectBytes, TestColumns.NESTED_OBJECT.getTestTypeReference()))
                .thenReturn(nestedObject);

        when(table.getScanner(scanArgumentCaptor.capture())).thenReturn(scanner);
        when(scanner.next()).thenAnswer(new Answer() {
            private int count = 0;

            @Override
            public Object answer(InvocationOnMock invocation) {
                if (count == 0) {
                    count++;
                    return result;
                }

                return null;
            }
        });

        int numRows = 5;
        testEntityDao.scan(startKey, true, endKey, true, numRows, constant);

        FilterList filterList = (FilterList) scanArgumentCaptor.getValue().getFilter();
        List<Filter> listOfFilters = filterList.getFilters();
        assertEquals(2, listOfFilters.size());
        assertTrue(listOfFilters.stream()
                .filter(PageFilter.class::isInstance)
                .map(PageFilter.class::cast)
                .findFirst()
                .isPresent());
        assertTrue(listOfFilters.stream()
                .filter(RowFilter.class::isInstance)
                .map(RowFilter.class::cast)
                .findFirst()
                .isPresent());
    }

    @Test
    public void testScanDoesNotRowFiltersOnEmptyConstant() throws IOException {
        final Key<TestEntity> startKey = new StringKey<>("a"); // [97]
        final Key<TestEntity> endKey = new StringKey<>("z"); // [122]
        final String constant = ""; // empty
        final String stringValue = "some string";

        final Boolean booleanValue = true;

        final TestNestedObject nestedObject = new TestNestedObject();
        nestedObject.setSomeValue(3);

        final Result result = mock(Result.class);
        when(result.getRow()).thenReturn(startKey.toBytes());
        when(result.isEmpty()).thenReturn(false);

        final Cell stringValueCell = mock(Cell.class);

        final byte[] stringValueBytes = new byte[] {
                1, 2, 3
        };

        when(stringValueCell.getValueArray()).thenReturn(stringValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.STRING_VALUE.getQualifier()))).thenReturn(stringValueCell);

        when(objectMapper.readValue(stringValueBytes, TestColumns.STRING_VALUE.getTestTypeReference()))
                .thenReturn(stringValue);

        final Cell booleanValueCell = mock(Cell.class);

        final byte[] booleanValueBytes = new byte[] {
                0
        };

        when(booleanValueCell.getValueArray()).thenReturn(booleanValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getQualifier()))).thenReturn(booleanValueCell);

        when(objectMapper.readValue(booleanValueBytes, TestColumns.BOOLEAN_VALUE.getTestTypeReference()))
                .thenReturn(booleanValue);

        final Cell nestedObjectCell = mock(Cell.class);

        final byte[] nestedObjectBytes = new byte[] {
                5, 4, 3, 2, 1, 0
        };

        when(nestedObjectCell.getValueArray()).thenReturn(nestedObjectBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.NESTED_OBJECT.getFamily()),
                Bytes.toBytes(TestColumns.NESTED_OBJECT.getQualifier()))).thenReturn(nestedObjectCell);

        when(objectMapper.readValue(nestedObjectBytes, TestColumns.NESTED_OBJECT.getTestTypeReference()))
                .thenReturn(nestedObject);

        when(table.getScanner(scanArgumentCaptor.capture())).thenReturn(scanner);
        when(scanner.next()).thenAnswer(new Answer() {
            private int count = 0;

            @Override
            public Object answer(InvocationOnMock invocation) {
                if (count == 0) {
                    count++;
                    return result;
                }

                return null;
            }
        });

        int numRows = 5;
        testEntityDao.scan(startKey, true, endKey, true, numRows, constant);

        FilterList filterList = (FilterList) scanArgumentCaptor.getValue().getFilter();
        List<Filter> listOfFilters = filterList.getFilters();
        assertEquals(1, listOfFilters.size());
        assertTrue(listOfFilters.stream()
                .filter(PageFilter.class::isInstance)
                .map(PageFilter.class::cast)
                .findFirst()
                .isPresent());
    }

    @Test
    public void testScanDoesNotRowFiltersOnNullConstant() throws IOException {
        final Key<TestEntity> startKey = new StringKey<>("a"); // [97]
        final Key<TestEntity> endKey = new StringKey<>("z"); // [122]
        final String constant = null;
        final String stringValue = "some string";

        final Boolean booleanValue = true;

        final TestNestedObject nestedObject = new TestNestedObject();
        nestedObject.setSomeValue(3);

        final Result result = mock(Result.class);
        when(result.getRow()).thenReturn(startKey.toBytes());
        when(result.isEmpty()).thenReturn(false);

        final Cell stringValueCell = mock(Cell.class);

        final byte[] stringValueBytes = new byte[] {
                1, 2, 3
        };

        when(stringValueCell.getValueArray()).thenReturn(stringValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.STRING_VALUE.getQualifier()))).thenReturn(stringValueCell);

        when(objectMapper.readValue(stringValueBytes, TestColumns.STRING_VALUE.getTestTypeReference()))
                .thenReturn(stringValue);

        final Cell booleanValueCell = mock(Cell.class);

        final byte[] booleanValueBytes = new byte[] {
                0
        };

        when(booleanValueCell.getValueArray()).thenReturn(booleanValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getQualifier()))).thenReturn(booleanValueCell);

        when(objectMapper.readValue(booleanValueBytes, TestColumns.BOOLEAN_VALUE.getTestTypeReference()))
                .thenReturn(booleanValue);

        final Cell nestedObjectCell = mock(Cell.class);

        final byte[] nestedObjectBytes = new byte[] {
                5, 4, 3, 2, 1, 0
        };

        when(nestedObjectCell.getValueArray()).thenReturn(nestedObjectBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.NESTED_OBJECT.getFamily()),
                Bytes.toBytes(TestColumns.NESTED_OBJECT.getQualifier()))).thenReturn(nestedObjectCell);

        when(objectMapper.readValue(nestedObjectBytes, TestColumns.NESTED_OBJECT.getTestTypeReference()))
                .thenReturn(nestedObject);

        when(table.getScanner(scanArgumentCaptor.capture())).thenReturn(scanner);
        when(scanner.next()).thenAnswer(new Answer() {
            private int count = 0;

            @Override
            public Object answer(InvocationOnMock invocation) {
                if (count == 0) {
                    count++;
                    return result;
                }

                return null;
            }
        });

        int numRows = 5;
        testEntityDao.scan(startKey, true, endKey, true, numRows, constant);

        FilterList filterList = (FilterList) scanArgumentCaptor.getValue().getFilter();
        List<Filter> listOfFilters = filterList.getFilters();
        assertEquals(1, listOfFilters.size());
        assertTrue(listOfFilters.stream()
                .filter(PageFilter.class::isInstance)
                .map(PageFilter.class::cast)
                .findFirst()
                .isPresent());
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

        verify(table).put(putArgumentCaptor.capture());

        final List<Put> puts = putArgumentCaptor.getValue();
        assertNotNull(puts);
        assertEquals(1, puts.size());

        final Put put = puts.get(0);

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

        verify(table).put(putArgumentCaptor.capture());

        final List<Put> puts = putArgumentCaptor.getValue();
        assertNotNull(puts);
        assertEquals(1, puts.size());

        final Put put = puts.get(0);

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

        verify(table).put(putArgumentCaptor.capture());

        final List<Put> puts = putArgumentCaptor.getValue();
        assertNotNull(puts);
        assertEquals(1, puts.size());

        final Put put = puts.get(0);

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

        verify(table).put(putArgumentCaptor.capture());

        final List<Put> puts = putArgumentCaptor.getValue();
        assertNotNull(puts);
        assertEquals(1, puts.size());

        final Put put = puts.get(0);

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

        verify(table).put(putArgumentCaptor.capture());

        final List<Put> puts = putArgumentCaptor.getValue();
        assertNotNull(puts);
        assertEquals(1, puts.size());

        final Put put = puts.get(0);

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

        verify(table).put(putArgumentCaptor.capture());

        final List<Put> puts = putArgumentCaptor.getValue();
        assertNotNull(puts);
        assertEquals(1, puts.size());

        final Put put = puts.get(0);

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

        verify(table).put(putArgumentCaptor.capture());

        final List<Put> puts = putArgumentCaptor.getValue();
        assertNotNull(puts);
        assertEquals(1, puts.size());

        final Put put = puts.get(0);

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

        verify(table).put(putArgumentCaptor.capture());

        final List<Put> puts = putArgumentCaptor.getValue();
        assertNotNull(puts);
        assertEquals(1, puts.size());

        final Put put = puts.get(0);

        assertTrue(put.has(Bytes.toBytes(TestVersionedColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestVersionedColumns.STRING_VALUE.getQualifier()), stringValueBytes));
        assertTrue(put.has(Bytes.toBytes(TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getQualifier()), booleanValueBytes));
    }

    @Test(expected = NullPointerException.class)
    public void testSaveAllWithNullMapThrowsNullPointerException() throws IOException {
        testEntityDao.saveAll(null);
    }

    @Test
    public void testSaveAllPutsAllColumns() throws IOException {
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

        final Key<TestEntity> key = new StringKey<>("key");
        final Map<Key<TestEntity>, TestEntity> entities = Collections.singletonMap(key, testEntity);

        final Map<Key<TestEntity>, TestEntity> savedEntities = testEntityDao.saveAll(entities);

        assertNotNull(savedEntities);
        assertTrue(savedEntities.containsKey(key));

        final TestEntity result = savedEntities.get(key);
        assertEquals(stringValue, result.getStringValue());
        assertEquals(booleanValue, result.getBooleanValue());
        assertEquals(nestedObject, result.getNestedObject());

        verify(table).put(putArgumentCaptor.capture());

        final List<Put> puts = putArgumentCaptor.getValue();
        assertNotNull(puts);
        assertEquals(1, puts.size());

        final Put put = puts.get(0);

        assertTrue(put.has(Bytes.toBytes(TestColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.STRING_VALUE.getQualifier()), stringValueBytes));
        assertTrue(put.has(Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getQualifier()), booleanValueBytes));
        assertTrue(put.has(Bytes.toBytes(TestColumns.NESTED_OBJECT.getFamily()),
                Bytes.toBytes(TestColumns.NESTED_OBJECT.getQualifier()), nestedObjectBytes));
    }

    @Test
    public void testSaveAllPutsAllColumnsForMultipleEntities() throws IOException {
        final String stringValue1 = "some string";

        final Boolean booleanValue1 = true;

        final TestNestedObject nestedObject1 = new TestNestedObject();
        nestedObject1.setSomeValue(3);

        final TestEntity testEntity1 = new TestEntity();
        testEntity1.setStringValue(stringValue1);
        testEntity1.setBooleanValue(booleanValue1);
        testEntity1.setNestedObject(nestedObject1);

        final byte[] stringValueBytes1 = {
                1, 2, 3
        };

        when(objectMapper.writeValueAsBytes(stringValue1)).thenReturn(stringValueBytes1);

        final byte[] booleanValueBytes1 = {
                0
        };

        when(objectMapper.writeValueAsBytes(booleanValue1)).thenReturn(booleanValueBytes1);

        final byte[] nestedObjectBytes1 = {
                5, 4, 3, 2, 1, 0
        };

        when(objectMapper.writeValueAsBytes(nestedObject1)).thenReturn(nestedObjectBytes1);

        final String stringValue2 = "some other string";

        final Boolean booleanValue2 = false;

        final TestNestedObject nestedObject2 = new TestNestedObject();
        nestedObject2.setSomeValue(8);

        final TestEntity testEntity2 = new TestEntity();
        testEntity2.setStringValue(stringValue2);
        testEntity2.setBooleanValue(booleanValue2);
        testEntity2.setNestedObject(nestedObject2);

        final byte[] stringValueBytes2 = {
                3, 2, 1
        };

        when(objectMapper.writeValueAsBytes(stringValue2)).thenReturn(stringValueBytes2);

        final byte[] booleanValueBytes2 = {
                9
        };

        when(objectMapper.writeValueAsBytes(booleanValue2)).thenReturn(booleanValueBytes2);

        final byte[] nestedObjectBytes2 = {
                3, 3, 3, 3, 3
        };

        when(objectMapper.writeValueAsBytes(nestedObject2)).thenReturn(nestedObjectBytes2);

        final Key<TestEntity> key1 = new StringKey<>("key1");
        final Key<TestEntity> key2 = new StringKey<>("key2");

        final Map<Key<TestEntity>, TestEntity> entities = new HashMap<>();
        entities.put(key1, testEntity1);
        entities.put(key2, testEntity2);

        final Map<Key<TestEntity>, TestEntity> savedEntities = testEntityDao.saveAll(entities);

        assertNotNull(savedEntities);

        assertTrue(savedEntities.containsKey(key1));

        final TestEntity result1 = savedEntities.get(key1);
        assertEquals(stringValue1, result1.getStringValue());
        assertEquals(booleanValue1, result1.getBooleanValue());
        assertEquals(nestedObject1, result1.getNestedObject());

        assertTrue(savedEntities.containsKey(key2));

        final TestEntity result2 = savedEntities.get(key2);
        assertEquals(stringValue2, result2.getStringValue());
        assertEquals(booleanValue2, result2.getBooleanValue());
        assertEquals(nestedObject2, result2.getNestedObject());

        verify(table).put(putArgumentCaptor.capture());

        final List<Put> puts = putArgumentCaptor.getValue();
        assertNotNull(puts);
        assertEquals(2, puts.size());

        final boolean entity1Found = puts.stream().anyMatch(put ->
                put.has(Bytes.toBytes(TestColumns.STRING_VALUE.getFamily()),
                        Bytes.toBytes(TestColumns.STRING_VALUE.getQualifier()), stringValueBytes1)
                        && put.has(Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getFamily()),
                        Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getQualifier()), booleanValueBytes1)
                        && put.has(Bytes.toBytes(TestColumns.NESTED_OBJECT.getFamily()),
                        Bytes.toBytes(TestColumns.NESTED_OBJECT.getQualifier()), nestedObjectBytes1));

        assertTrue(entity1Found);

        final boolean entity2Found = puts.stream().anyMatch(put ->
                put.has(Bytes.toBytes(TestColumns.STRING_VALUE.getFamily()),
                        Bytes.toBytes(TestColumns.STRING_VALUE.getQualifier()), stringValueBytes2)
                        && put.has(Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getFamily()),
                        Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getQualifier()), booleanValueBytes2)
                        && put.has(Bytes.toBytes(TestColumns.NESTED_OBJECT.getFamily()),
                        Bytes.toBytes(TestColumns.NESTED_OBJECT.getQualifier()), nestedObjectBytes2));

        assertTrue(entity2Found);
    }

    @Test
    public void testSaveAllPutsAllColumnsWithLiveObjectMapper() throws IOException {
        testEntityDao = new BigTableEntityDao<>(table, columns, entityFactory, delegateFactory);

        final String stringValue = "some string";

        final Boolean booleanValue = true;

        final TestNestedObject nestedObject = new TestNestedObject();
        nestedObject.setSomeValue(3);

        final TestEntity testEntity = new TestEntity();
        testEntity.setStringValue(stringValue);
        testEntity.setBooleanValue(booleanValue);
        testEntity.setNestedObject(nestedObject);

        final Key<TestEntity> key = new StringKey<>("key");
        final Map<Key<TestEntity>, TestEntity> entities = Collections.singletonMap(key, testEntity);

        final Map<Key<TestEntity>, TestEntity> savedEntities = testEntityDao.saveAll(entities);

        assertNotNull(savedEntities);
        assertTrue(savedEntities.containsKey(key));

        final TestEntity result = savedEntities.get(key);
        assertEquals(stringValue, result.getStringValue());
        assertEquals(booleanValue, result.getBooleanValue());
        assertEquals(nestedObject, result.getNestedObject());

        verify(table).put(putArgumentCaptor.capture());

        final List<Put> puts = putArgumentCaptor.getValue();
        assertNotNull(puts);
        assertEquals(1, puts.size());

        final Put put = puts.get(0);

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
    public void testSaveAllPutsNullStringValueWithLiveObjectMapper() throws IOException {
        testEntityDao = new BigTableEntityDao<>(table, columns, entityFactory, delegateFactory);

        final String stringValue = null;

        final Boolean booleanValue = true;

        final TestNestedObject nestedObject = new TestNestedObject();
        nestedObject.setSomeValue(3);

        final TestEntity testEntity = new TestEntity();
        testEntity.setStringValue(stringValue);
        testEntity.setBooleanValue(booleanValue);
        testEntity.setNestedObject(nestedObject);

        final Key<TestEntity> key = new StringKey<>("key");
        final Map<Key<TestEntity>, TestEntity> entities = Collections.singletonMap(key, testEntity);

        final Map<Key<TestEntity>, TestEntity> savedEntities = testEntityDao.saveAll(entities);

        assertNotNull(savedEntities);
        assertTrue(savedEntities.containsKey(key));

        final TestEntity result = savedEntities.get(key);
        assertEquals(stringValue, result.getStringValue());
        assertEquals(booleanValue, result.getBooleanValue());
        assertEquals(nestedObject, result.getNestedObject());

        verify(table).put(putArgumentCaptor.capture());

        final List<Put> puts = putArgumentCaptor.getValue();
        assertNotNull(puts);
        assertEquals(1, puts.size());

        final Put put = puts.get(0);

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
    public void testSaveAllPutsNullBooleanValueWithLiveObjectMapper() throws IOException {
        testEntityDao = new BigTableEntityDao<>(table, columns, entityFactory, delegateFactory);

        final String stringValue = "some string";

        final Boolean booleanValue = null;

        final TestNestedObject nestedObject = new TestNestedObject();
        nestedObject.setSomeValue(3);

        final TestEntity testEntity = new TestEntity();
        testEntity.setStringValue(stringValue);
        testEntity.setBooleanValue(booleanValue);
        testEntity.setNestedObject(nestedObject);

        final Key<TestEntity> key = new StringKey<>("key");
        final Map<Key<TestEntity>, TestEntity> entities = Collections.singletonMap(key, testEntity);

        final Map<Key<TestEntity>, TestEntity> savedEntities = testEntityDao.saveAll(entities);

        assertNotNull(savedEntities);
        assertTrue(savedEntities.containsKey(key));

        final TestEntity result = savedEntities.get(key);
        assertEquals(stringValue, result.getStringValue());
        assertEquals(booleanValue, result.getBooleanValue());
        assertEquals(nestedObject, result.getNestedObject());

        verify(table).put(putArgumentCaptor.capture());

        final List<Put> puts = putArgumentCaptor.getValue();
        assertNotNull(puts);
        assertEquals(1, puts.size());

        final Put put = puts.get(0);

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
    public void testSaveAllWithPutsNullNestedObjectWithLiveObjectMapper() throws IOException {
        testEntityDao = new BigTableEntityDao<>(table, columns, entityFactory, delegateFactory);

        final String stringValue = "some string";

        final Boolean booleanValue = true;

        final TestNestedObject nestedObject = null;

        final TestEntity testEntity = new TestEntity();
        testEntity.setStringValue(stringValue);
        testEntity.setBooleanValue(booleanValue);
        testEntity.setNestedObject(nestedObject);

        final Key<TestEntity> key = new StringKey<>("key");
        final Map<Key<TestEntity>, TestEntity> entities = Collections.singletonMap(key, testEntity);

        final Map<Key<TestEntity>, TestEntity> savedEntities = testEntityDao.saveAll(entities);

        assertNotNull(savedEntities);
        assertTrue(savedEntities.containsKey(key));

        final TestEntity result = savedEntities.get(key);
        assertEquals(stringValue, result.getStringValue());
        assertEquals(booleanValue, result.getBooleanValue());
        assertEquals(nestedObject, result.getNestedObject());

        verify(table).put(putArgumentCaptor.capture());

        final List<Put> puts = putArgumentCaptor.getValue();
        assertNotNull(puts);
        assertEquals(1, puts.size());

        final Put put = puts.get(0);

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
    public void testSaveAllPutsNullValues() throws IOException {
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

        final Key<TestEntity> key = new StringKey<>("key");
        final Map<Key<TestEntity>, TestEntity> entities = Collections.singletonMap(key, testEntity);

        final Map<Key<TestEntity>, TestEntity> savedEntities = testEntityDao.saveAll(entities);

        assertNotNull(savedEntities);
        assertTrue(savedEntities.containsKey(key));

        final TestEntity result = savedEntities.get(key);
        assertEquals(stringValue, result.getStringValue());
        assertEquals(booleanValue, result.getBooleanValue());
        assertEquals(nestedObject, result.getNestedObject());

        verify(table).put(putArgumentCaptor.capture());

        final List<Put> puts = putArgumentCaptor.getValue();
        assertNotNull(puts);
        assertEquals(1, puts.size());

        final Put put = puts.get(0);

        assertTrue(put.has(Bytes.toBytes(TestColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.STRING_VALUE.getQualifier()), stringValueBytes));
        assertTrue(put.has(Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getQualifier()), booleanValueBytes));
        assertTrue(put.has(Bytes.toBytes(TestColumns.NESTED_OBJECT.getFamily()),
                Bytes.toBytes(TestColumns.NESTED_OBJECT.getQualifier()), nestedObjectBytes));
    }

    @Test
    public void testSaveAllPutsVersionedColumnsWithDefinedTimestamp() throws IOException {
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

        final Key<TestVersionedEntity> key = new StringKey<>("key");
        final Map<Key<TestVersionedEntity>, TestVersionedEntity> entities = Collections.singletonMap(key, testVersionedEntity);

        final Map<Key<TestVersionedEntity>, TestVersionedEntity> savedEntities = testVersionedEntityDao.saveAll(entities);

        assertNotNull(savedEntities);
        assertTrue(savedEntities.containsKey(key));

        final TestVersionedEntity result = savedEntities.get(key);
        assertEquals(stringValue, result.getStringValue());
        assertEquals(booleanValue, result.getVersionedBooleanValue());
        assertEquals(booleanValueTimestamp, (long) result.getVersionedBooleanValueTimestamp());

        verify(table).put(putArgumentCaptor.capture());

        final List<Put> puts = putArgumentCaptor.getValue();
        assertNotNull(puts);
        assertEquals(1, puts.size());

        final Put put = puts.get(0);

        assertTrue(put.has(Bytes.toBytes(TestVersionedColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestVersionedColumns.STRING_VALUE.getQualifier()), stringValueBytes));
        assertTrue(put.has(Bytes.toBytes(TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getQualifier()), booleanValueBytes));
    }

    @Test
    public void testSaveAllPutsVersionedColumnsWithUndefinedTimestamp() throws IOException {
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

        final Key<TestVersionedEntity> key = new StringKey<>("key");
        final Map<Key<TestVersionedEntity>, TestVersionedEntity> entities = Collections.singletonMap(key, testVersionedEntity);

        final Map<Key<TestVersionedEntity>, TestVersionedEntity> savedEntities = testVersionedEntityDao.saveAll(entities);

        assertNotNull(savedEntities);
        assertTrue(savedEntities.containsKey(key));

        final TestVersionedEntity result = savedEntities.get(key);
        assertEquals(stringValue, result.getStringValue());
        assertEquals(booleanValue, result.getVersionedBooleanValue());
        assertTrue(result.getVersionedBooleanValueTimestamp() >= atLeastTimestamp);

        verify(table).put(putArgumentCaptor.capture());

        final List<Put> puts = putArgumentCaptor.getValue();
        assertNotNull(puts);
        assertEquals(1, puts.size());

        final Put put = puts.get(0);

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

        verify(table).delete(deleteArgumentCaptor.capture());

        final List<Delete> deletes = deleteArgumentCaptor.getValue();
        assertNotNull(deletes);
        assertEquals(1, deletes.size());

        // Delete has no easy way to verify which column family/qualifiers are being deleted
        final Delete delete = deletes.get(0);
        assertNotNull(delete);
    }

    @Test(expected = NullPointerException.class)
    public void testDeleteWithNullKeysThrowsNullPointerException() throws IOException {
        testEntityDao.deleteAll(null);
    }

    @Test
    public void testDeleteAllDeletesAllColumns() throws IOException {
        final Key<TestEntity> key = new StringKey<>("key");
        final Set<Key<TestEntity>> keys = Collections.singleton(key);

        testEntityDao.deleteAll(keys);

        verify(table).delete(deleteArgumentCaptor.capture());

        final List<Delete> deletes = deleteArgumentCaptor.getValue();
        assertNotNull(deletes);
        assertEquals(1, deletes.size());

        // Delete has no easy way to verify which column family/qualifiers are being deleted
        final Delete delete = deletes.get(0);
        assertNotNull(delete);
    }

    @Test
    public void testDeleteAllDeletesAllColumnsForMultipleKeys() throws IOException {
        final Key<TestEntity> key1 = new StringKey<>("key1");
        final Key<TestEntity> key2 = new StringKey<>("key2");

        final Set<Key<TestEntity>> keys = Stream.of(key1, key2).collect(Collectors.toSet());

        testEntityDao.deleteAll(keys);

        verify(table).delete(deleteArgumentCaptor.capture());

        final List<Delete> deletes = deleteArgumentCaptor.getValue();
        assertNotNull(deletes);
        assertEquals(2, deletes.size());

        // Delete has no easy way to verify which column family/qualifiers are being deleted
        final Delete delete1 = deletes.get(0);
        assertNotNull(delete1);

        final Delete delete2 = deletes.get(1);
        assertNotNull(delete2);
    }
}
