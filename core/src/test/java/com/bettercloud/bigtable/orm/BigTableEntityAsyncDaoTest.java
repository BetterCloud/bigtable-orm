package com.bettercloud.bigtable.orm;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class BigTableEntityAsyncDaoTest extends AbstractBigTableEntityDaoTest {

    @Mock
    private AsyncTable<?> table;

    private final List<Column> columns = Arrays.asList(TestColumns.values());

    private final Supplier<TestEntity> entityFactory = TestEntity::new;

    private final Function<TestEntity, EntityConfiguration.EntityDelegate<TestEntity>> delegateFactory = TestDelegate::new;

    private final List<Column> versionedColumns = Arrays.asList(TestVersionedColumns.values());

    private final Supplier<TestVersionedEntity> versionedEntityFactory = TestVersionedEntity::new;

    private final Function<TestVersionedEntity, EntityConfiguration.EntityDelegate<TestVersionedEntity>> versionedDelegateFactory = TestVersionedDelegate::new;

    @Mock
    private ObjectMapper objectMapper;

    private ObjectMapper liveObjectMapper = new ObjectMapper();

    private AsyncDao<TestEntity> testEntityDao;

    private AsyncDao<TestVersionedEntity> testVersionedEntityDao;

    @Captor
    private ArgumentCaptor<List<Put>> putsArgumentCaptor;

    @Captor
    private ArgumentCaptor<Put> putArgumentCaptor;

    @Captor
    private ArgumentCaptor<List<Delete>> deletesArgumentCaptor;

    @Captor
    private ArgumentCaptor<Delete> deleteArgumentCaptor;

    @Captor
    private ArgumentCaptor<Scan> scanArgumentCaptor;

    @Before
    public void setup() {
        initMocks(this);

        testEntityDao = new BigTableEntityAsyncDao<>(table, columns, entityFactory, delegateFactory, objectMapper);
        testVersionedEntityDao = new BigTableEntityAsyncDao<>(table, versionedColumns, versionedEntityFactory,
                versionedDelegateFactory, objectMapper);
    }

    @Test(expected = NullPointerException.class)
    public void testGetWithNullKeyThrowsNullPointerException() {
        final Key<TestEntity> key = null;
        testEntityDao.get(key);
    }

    @Test(expected = IOException.class)
    public void testGetIOExceptionsAreWrappedInExecutionException() throws Throwable {
        final Result result = mock(Result.class);
        when(result.isEmpty()).thenReturn(false);

        final Cell stringValueCell = mock(Cell.class);

        final byte[] stringValueBytes = new byte[]{
                1, 2, 3
        };

        when(stringValueCell.getValueArray()).thenReturn(stringValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.STRING_VALUE.getQualifier()))).thenReturn(stringValueCell);

        when(objectMapper.readValue(any(byte[].class), any(TypeReference.class))).thenThrow(new IOException());

        when(table.get(any(Get.class))).thenReturn(CompletableFuture.completedFuture(result));

        final CompletableFuture<TestEntity> retrievedEntity = testEntityDao.get(new StringKey<>("key"));

        assertTrue(retrievedEntity.isDone());

        try {
            retrievedEntity.get();
        } catch (InterruptedException e) {
            fail("Exception should not be thrown");
        } catch (ExecutionException e) {
            throw e.getCause();
        }

    }

    @Test
    public void testGetRetrievesAllColumns() throws IOException, ExecutionException, InterruptedException {
        final String stringValue = "some string";

        final Boolean booleanValue = true;

        final TestNestedObject nestedObject = new TestNestedObject();
        nestedObject.setSomeValue(3);

        final Result result = mock(Result.class);
        when(result.isEmpty()).thenReturn(false);

        final Cell stringValueCell = mock(Cell.class);

        final byte[] stringValueBytes = new byte[]{
                1, 2, 3
        };

        when(stringValueCell.getValueArray()).thenReturn(stringValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.STRING_VALUE.getQualifier()))).thenReturn(stringValueCell);

        when(objectMapper.readValue(stringValueBytes, TestColumns.STRING_VALUE.getTypeReference()))
                .thenReturn(stringValue);

        final Cell booleanValueCell = mock(Cell.class);

        final byte[] booleanValueBytes = new byte[]{
                0
        };

        when(booleanValueCell.getValueArray()).thenReturn(booleanValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getQualifier()))).thenReturn(booleanValueCell);

        when(objectMapper.readValue(booleanValueBytes, TestColumns.BOOLEAN_VALUE.getTypeReference()))
                .thenReturn(booleanValue);

        final Cell nestedObjectCell = mock(Cell.class);

        final byte[] nestedObjectBytes = new byte[]{
                5, 4, 3, 2, 1, 0
        };

        when(nestedObjectCell.getValueArray()).thenReturn(nestedObjectBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.NESTED_OBJECT.getFamily()),
                Bytes.toBytes(TestColumns.NESTED_OBJECT.getQualifier()))).thenReturn(nestedObjectCell);

        when(objectMapper.readValue(nestedObjectBytes, TestColumns.NESTED_OBJECT.getTypeReference()))
                .thenReturn(nestedObject);

        when(table.get(any(Get.class))).thenReturn(CompletableFuture.completedFuture(result));

        final CompletableFuture<TestEntity> retrievedEntity = testEntityDao.get(new StringKey<>("key"));

        assertTrue(retrievedEntity.isDone());

        final TestEntity entity = retrievedEntity.get();
        assertEquals(stringValue, entity.getStringValue());
        assertEquals(booleanValue, entity.getBooleanValue());
        assertEquals(nestedObject, entity.getNestedObject());
    }

    @Test
    public void testGetRetrievesNullValues() throws IOException, ExecutionException, InterruptedException {
        final String stringValue = "some string";

        final Boolean booleanValue = true;

        final TestNestedObject nestedObject = null;

        final Result result = mock(Result.class);
        when(result.isEmpty()).thenReturn(false);

        final Cell stringValueCell = mock(Cell.class);

        final byte[] stringValueBytes = new byte[]{
                1, 2, 3
        };

        when(stringValueCell.getValueArray()).thenReturn(stringValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.STRING_VALUE.getQualifier()))).thenReturn(stringValueCell);

        when(objectMapper.readValue(stringValueBytes, TestColumns.STRING_VALUE.getTypeReference()))
                .thenReturn(stringValue);

        final Cell booleanValueCell = mock(Cell.class);

        final byte[] booleanValueBytes = new byte[]{
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

        when(table.get(any(Get.class))).thenReturn(CompletableFuture.completedFuture(result));

        final CompletableFuture<TestEntity> retrievedEntity = testEntityDao.get(new StringKey<>("key"));

        assertTrue(retrievedEntity.isDone());

        final TestEntity entity = retrievedEntity.get();
        assertNull(entity.getNestedObject());
    }

    @Test
    public void testGetRetrievesNonExistentCellsAsNullValues() throws ExecutionException, InterruptedException {
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

        when(table.get(any(Get.class))).thenReturn(CompletableFuture.completedFuture(result));

        final CompletableFuture<TestEntity> retrievedEntity = testEntityDao.get(new StringKey<>("key"));

        assertTrue(retrievedEntity.isDone());

        final TestEntity entity = retrievedEntity.get();
        assertNull(entity.getStringValue());
        assertNull(entity.getBooleanValue());
        assertNull(entity.getNestedObject());
    }

    @Test
    public void testGetRetrievesVersionedValuesAndTimestamps() throws IOException, ExecutionException, InterruptedException {
        final String stringValue = "some string";

        final Boolean booleanValue = true;
        final long booleanValueTimestamp = 1234L;

        final Result result = mock(Result.class);
        when(result.isEmpty()).thenReturn(false);

        final Cell stringValueCell = mock(Cell.class);

        final byte[] stringValueBytes = new byte[]{
                1, 2, 3
        };

        when(stringValueCell.getValueArray()).thenReturn(stringValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestVersionedColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestVersionedColumns.STRING_VALUE.getQualifier()))).thenReturn(stringValueCell);

        when(objectMapper.readValue(stringValueBytes, TestVersionedColumns.STRING_VALUE.getTypeReference()))
                .thenReturn(stringValue);

        final Cell booleanValueCell = mock(Cell.class);

        final byte[] booleanValueBytes = new byte[]{
                0
        };

        when(booleanValueCell.getValueArray()).thenReturn(booleanValueBytes);
        when(booleanValueCell.getTimestamp()).thenReturn(booleanValueTimestamp);

        when(result.getColumnLatestCell(Bytes.toBytes(TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getQualifier()))).thenReturn(booleanValueCell);

        when(objectMapper.readValue(booleanValueBytes, TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getTypeReference()))
                .thenReturn(booleanValue);

        when(table.get(any(Get.class))).thenReturn(CompletableFuture.completedFuture(result));

        final CompletableFuture<TestVersionedEntity> retrievedEntity = testVersionedEntityDao.get(new StringKey<>("key"));

        assertTrue(retrievedEntity.isDone());

        final TestVersionedEntity entity = retrievedEntity.get();
        assertEquals(stringValue, entity.getStringValue());
        assertEquals(booleanValue, entity.getVersionedBooleanValue());
        assertEquals(booleanValueTimestamp, (long) entity.getVersionedBooleanValueTimestamp());
    }

    @Test
    public void testGetRetrievesNonExistentVersionedCellsAsNullValuesAndNullTimestamps() throws ExecutionException, InterruptedException {
        final Result result = mock(Result.class);
        when(result.isEmpty()).thenReturn(false);

        final Cell stringValueCell = null;

        when(result.getColumnLatestCell(Bytes.toBytes(TestVersionedColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestVersionedColumns.STRING_VALUE.getQualifier()))).thenReturn(stringValueCell);

        final Cell booleanValueCell = null;

        when(result.getColumnLatestCell(Bytes.toBytes(TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getQualifier()))).thenReturn(booleanValueCell);

        when(table.get(any(Get.class))).thenReturn(CompletableFuture.completedFuture(result));

        final CompletableFuture<TestVersionedEntity> retrievedEntity = testVersionedEntityDao.get(new StringKey<>("key"));

        assertTrue(retrievedEntity.isDone());

        final TestVersionedEntity entity = retrievedEntity.get();
        assertNull(entity.getStringValue());
        assertNull(entity.getVersionedBooleanValue());
        assertNull(entity.getVersionedBooleanValueTimestamp());
    }

    @Test
    public void testGetWithLiveObjectMapper() throws IOException, ExecutionException, InterruptedException {
        testEntityDao = new BigTableEntityAsyncDao<>(table, columns, entityFactory, delegateFactory);

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

        when(table.get(any(Get.class))).thenReturn(CompletableFuture.completedFuture(result));

        final CompletableFuture<TestEntity> retrievedEntity = testEntityDao.get(new StringKey<>("key"));

        assertTrue(retrievedEntity.isDone());

        final TestEntity entity = retrievedEntity.get();
        assertEquals(stringValue, entity.getStringValue());
        assertEquals(booleanValue, entity.getBooleanValue());
        assertEquals(nestedObject, entity.getNestedObject());
    }

    @Test
    public void testGetRetrievesNullStringValueWithLiveObjectMapper() throws IOException, ExecutionException, InterruptedException {
        testEntityDao = new BigTableEntityAsyncDao<>(table, columns, entityFactory, delegateFactory);

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

        when(table.get(any(Get.class))).thenReturn(CompletableFuture.completedFuture(result));

        final CompletableFuture<TestEntity> retrievedEntity = testEntityDao.get(new StringKey<>("key"));

        assertTrue(retrievedEntity.isDone());

        final TestEntity entity = retrievedEntity.get();
        assertEquals(stringValue, entity.getStringValue());
        assertEquals(booleanValue, entity.getBooleanValue());
        assertEquals(nestedObject, entity.getNestedObject());
    }

    @Test
    public void testGetRetrievesNullBooleanValueWithLiveObjectMapper() throws IOException, ExecutionException, InterruptedException {
        testEntityDao = new BigTableEntityAsyncDao<>(table, columns, entityFactory, delegateFactory);

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

        when(table.get(any(Get.class))).thenReturn(CompletableFuture.completedFuture(result));

        final CompletableFuture<TestEntity> retrievedEntity = testEntityDao.get(new StringKey<>("key"));

        assertTrue(retrievedEntity.isDone());

        final TestEntity entity = retrievedEntity.get();
        assertEquals(stringValue, entity.getStringValue());
        assertEquals(booleanValue, entity.getBooleanValue());
        assertEquals(nestedObject, entity.getNestedObject());
    }

    @Test
    public void testGetRetrievesNullNestedObjectWithLiveObjectMapper() throws IOException, ExecutionException, InterruptedException {
        testEntityDao = new BigTableEntityAsyncDao<>(table, columns, entityFactory, delegateFactory);

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

        when(table.get(any(Get.class))).thenReturn(CompletableFuture.completedFuture(result));

        final CompletableFuture<TestEntity> retrievedEntity = testEntityDao.get(new StringKey<>("key"));

        assertTrue(retrievedEntity.isDone());

        final TestEntity entity = retrievedEntity.get();
        assertEquals(stringValue, entity.getStringValue());
        assertEquals(booleanValue, entity.getBooleanValue());
        assertEquals(nestedObject, entity.getNestedObject());
    }

    @Test
    public void testGetReturnsNullWhenResultIsEmpty() throws ExecutionException, InterruptedException {
        final Result result = mock(Result.class);
        when(result.isEmpty()).thenReturn(true);

        when(table.get(any(Get.class))).thenReturn(CompletableFuture.completedFuture(result));

        final CompletableFuture<TestEntity> retrievedEntity = testEntityDao.get(new StringKey<>("key"));

        assertTrue(retrievedEntity.isDone());

        assertNull(retrievedEntity.get());
    }

    @Test(expected = NullPointerException.class)
    public void testGetWithNullSetThrowsNullPointerException() {
        final Set<StringKey<TestEntity>> keys = null;
        testEntityDao.get(keys);
    }

    @Test(expected = IOException.class)
    public void testGetKeysIOExceptionWrappedInExecutionException() throws Throwable {
        final Result result = mock(Result.class);
        when(result.isEmpty()).thenReturn(false);

        final Cell stringValueCell = mock(Cell.class);

        final byte[] stringValueBytes = new byte[]{
                1, 2, 3
        };

        when(stringValueCell.getValueArray()).thenReturn(stringValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.STRING_VALUE.getQualifier()))).thenReturn(stringValueCell);

        when(objectMapper.readValue(any(byte[].class), any(TypeReference.class))).thenThrow(new IOException());

        when(table.get(anyList())).thenReturn(Collections.singletonList(CompletableFuture.completedFuture(result)));

        final Key<TestEntity> key = new StringKey<>("key");
        final Set<Key<TestEntity>> keys = Collections.singleton(key);
        final Map<Key<TestEntity>, CompletableFuture<TestEntity>> retrievedEntities = testEntityDao.get(keys);

        assertTrue(retrievedEntities.containsKey(key));

        final CompletableFuture<TestEntity> futureEntity = retrievedEntities.get(key);
        assertTrue(futureEntity.isDone());

        try {
            futureEntity.get();
        } catch (InterruptedException e) {
            fail("Exception should not be thrown");
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }

    @Test
    public void testGetKeysRetrievesAllColumns() throws IOException, ExecutionException, InterruptedException {
        final String stringValue = "some string";

        final Boolean booleanValue = true;

        final TestNestedObject nestedObject = new TestNestedObject();
        nestedObject.setSomeValue(3);

        final Result result = mock(Result.class);
        when(result.isEmpty()).thenReturn(false);

        final Cell stringValueCell = mock(Cell.class);

        final byte[] stringValueBytes = new byte[]{
                1, 2, 3
        };

        when(stringValueCell.getValueArray()).thenReturn(stringValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.STRING_VALUE.getQualifier()))).thenReturn(stringValueCell);

        when(objectMapper.readValue(stringValueBytes, TestColumns.STRING_VALUE.getTypeReference()))
                .thenReturn(stringValue);

        final Cell booleanValueCell = mock(Cell.class);

        final byte[] booleanValueBytes = new byte[]{
                0
        };

        when(booleanValueCell.getValueArray()).thenReturn(booleanValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getQualifier()))).thenReturn(booleanValueCell);

        when(objectMapper.readValue(booleanValueBytes, TestColumns.BOOLEAN_VALUE.getTypeReference()))
                .thenReturn(booleanValue);

        final Cell nestedObjectCell = mock(Cell.class);

        final byte[] nestedObjectBytes = new byte[]{
                5, 4, 3, 2, 1, 0
        };

        when(nestedObjectCell.getValueArray()).thenReturn(nestedObjectBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.NESTED_OBJECT.getFamily()),
                Bytes.toBytes(TestColumns.NESTED_OBJECT.getQualifier()))).thenReturn(nestedObjectCell);

        when(objectMapper.readValue(nestedObjectBytes, TestColumns.NESTED_OBJECT.getTypeReference()))
                .thenReturn(nestedObject);

        when(table.get(anyList())).thenReturn(Collections.singletonList(CompletableFuture.completedFuture(result)));

        final Key<TestEntity> key = new StringKey<>("key");
        final Set<Key<TestEntity>> keys = Collections.singleton(key);
        final Map<Key<TestEntity>, CompletableFuture<TestEntity>> retrievedEntities = testEntityDao.get(keys);

        assertTrue(retrievedEntities.containsKey(key));

        final CompletableFuture<TestEntity> futureEntity = retrievedEntities.get(key);
        assertTrue(futureEntity.isDone());

        final TestEntity entity = futureEntity.get();
        assertEquals(stringValue, entity.getStringValue());
        assertEquals(booleanValue, entity.getBooleanValue());
        assertEquals(nestedObject, entity.getNestedObject());
    }

    @Test
    public void testGetKeysRetrievesAllColumnsForMultipleKeys() throws IOException, ExecutionException, InterruptedException {
        final String stringValue1 = "some string";

        final Boolean booleanValue1 = true;

        final TestNestedObject nestedObject1 = new TestNestedObject();
        nestedObject1.setSomeValue(3);

        final Result result1 = mock(Result.class);
        when(result1.isEmpty()).thenReturn(false);

        final Cell stringValueCell1 = mock(Cell.class);

        final byte[] stringValueBytes1 = new byte[]{
                1, 2, 3
        };

        when(stringValueCell1.getValueArray()).thenReturn(stringValueBytes1);

        when(result1.getColumnLatestCell(Bytes.toBytes(TestColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.STRING_VALUE.getQualifier()))).thenReturn(stringValueCell1);

        when(objectMapper.readValue(stringValueBytes1, TestColumns.STRING_VALUE.getTypeReference()))
                .thenReturn(stringValue1);

        final Cell booleanValueCell1 = mock(Cell.class);

        final byte[] booleanValueBytes1 = new byte[]{
                0
        };

        when(booleanValueCell1.getValueArray()).thenReturn(booleanValueBytes1);

        when(result1.getColumnLatestCell(Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getQualifier()))).thenReturn(booleanValueCell1);

        when(objectMapper.readValue(booleanValueBytes1, TestColumns.BOOLEAN_VALUE.getTypeReference()))
                .thenReturn(booleanValue1);

        final Cell nestedObjectCell1 = mock(Cell.class);

        final byte[] nestedObjectBytes1 = new byte[]{
                5, 4, 3, 2, 1, 0
        };

        when(nestedObjectCell1.getValueArray()).thenReturn(nestedObjectBytes1);

        when(result1.getColumnLatestCell(Bytes.toBytes(TestColumns.NESTED_OBJECT.getFamily()),
                Bytes.toBytes(TestColumns.NESTED_OBJECT.getQualifier()))).thenReturn(nestedObjectCell1);

        when(objectMapper.readValue(nestedObjectBytes1, TestColumns.NESTED_OBJECT.getTypeReference()))
                .thenReturn(nestedObject1);

        final String stringValue2 = "some other string";

        final Boolean booleanValue2 = false;

        final TestNestedObject nestedObject2 = new TestNestedObject();
        nestedObject2.setSomeValue(8);

        final Result result2 = mock(Result.class);
        when(result2.isEmpty()).thenReturn(false);

        final Cell stringValueCell2 = mock(Cell.class);

        final byte[] stringValueBytes2 = new byte[]{
                9, 8, 7
        };

        when(stringValueCell2.getValueArray()).thenReturn(stringValueBytes2);

        when(result2.getColumnLatestCell(Bytes.toBytes(TestColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.STRING_VALUE.getQualifier()))).thenReturn(stringValueCell2);

        when(objectMapper.readValue(stringValueBytes2, TestColumns.STRING_VALUE.getTypeReference()))
                .thenReturn(stringValue2);

        final Cell booleanValueCell2 = mock(Cell.class);

        final byte[] booleanValueBytes2 = new byte[]{
                9
        };

        when(booleanValueCell2.getValueArray()).thenReturn(booleanValueBytes2);

        when(result2.getColumnLatestCell(Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getQualifier()))).thenReturn(booleanValueCell2);

        when(objectMapper.readValue(booleanValueBytes2, TestColumns.BOOLEAN_VALUE.getTypeReference()))
                .thenReturn(booleanValue2);

        final Cell nestedObjectCell2 = mock(Cell.class);

        final byte[] nestedObjectBytes2 = new byte[]{
                3, 3, 3, 3, 3
        };

        when(nestedObjectCell2.getValueArray()).thenReturn(nestedObjectBytes2);

        when(result2.getColumnLatestCell(Bytes.toBytes(TestColumns.NESTED_OBJECT.getFamily()),
                Bytes.toBytes(TestColumns.NESTED_OBJECT.getQualifier()))).thenReturn(nestedObjectCell2);

        when(objectMapper.readValue(nestedObjectBytes2, TestColumns.NESTED_OBJECT.getTypeReference()))
                .thenReturn(nestedObject2);

        final Key<TestEntity> key1 = new StringKey<>("key1");
        final Key<TestEntity> key2 = new StringKey<>("key2");

        // The keys are passed in as a Set to remove duplicates, but converted to a List for use with the HBase API.
        // The HBase API returns an array of Results, with indices corresponding to the List of keys.
        // Since the conversion from a Set to a List does not maintain order, we need to mock this interaction manually.
        when(table.get(anyList())).then(invocation -> {
            final List<Get> gets = invocation.getArgument(0);

            if (Arrays.equals(gets.get(0).getRow(), key1.toBytes())) {
                return Arrays.asList(CompletableFuture.completedFuture(result1), CompletableFuture.completedFuture(result2));
            } else {
                return Arrays.asList(CompletableFuture.completedFuture(result2), CompletableFuture.completedFuture(result1));
            }
        });

        final Set<Key<TestEntity>> keys = Stream.of(key1, key2).collect(Collectors.toSet());
        final Map<Key<TestEntity>, CompletableFuture<TestEntity>> retrievedEntities = testEntityDao.get(keys);

        assertTrue(retrievedEntities.containsKey(key1));

        final CompletableFuture<TestEntity> entity1Future = retrievedEntities.get(key1);
        final TestEntity entity1 = entity1Future.get();
        assertEquals(stringValue1, entity1.getStringValue());
        assertEquals(booleanValue1, entity1.getBooleanValue());
        assertEquals(nestedObject1, entity1.getNestedObject());

        assertTrue(retrievedEntities.containsKey(key2));

        final CompletableFuture<TestEntity> entity2Future = retrievedEntities.get(key2);
        final TestEntity entity2 = entity2Future.get();
        assertEquals(stringValue2, entity2.getStringValue());
        assertEquals(booleanValue2, entity2.getBooleanValue());
        assertEquals(nestedObject2, entity2.getNestedObject());
    }

    @Test
    public void testGetKeysRetrievesNullValues() throws IOException, ExecutionException, InterruptedException {
        final String stringValue = "some string";

        final Boolean booleanValue = true;

        final TestNestedObject nestedObject = null;

        final Result result = mock(Result.class);
        when(result.isEmpty()).thenReturn(false);

        final Cell stringValueCell = mock(Cell.class);

        final byte[] stringValueBytes = new byte[]{
                1, 2, 3
        };

        when(stringValueCell.getValueArray()).thenReturn(stringValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.STRING_VALUE.getQualifier()))).thenReturn(stringValueCell);

        when(objectMapper.readValue(stringValueBytes, TestColumns.STRING_VALUE.getTypeReference()))
                .thenReturn(stringValue);

        final Cell booleanValueCell = mock(Cell.class);

        final byte[] booleanValueBytes = new byte[]{
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

        when(table.get(anyList())).thenReturn(Collections.singletonList(CompletableFuture.completedFuture(result)));

        final Key<TestEntity> key = new StringKey<>("key");
        final Set<Key<TestEntity>> keys = Collections.singleton(key);
        final Map<Key<TestEntity>, CompletableFuture<TestEntity>> retrievedEntities = testEntityDao.get(keys);

        assertTrue(retrievedEntities.containsKey(key));

        final CompletableFuture<TestEntity> entityFuture = retrievedEntities.get(key);
        assertTrue(entityFuture.isDone());

        final TestEntity entity = entityFuture.get();
        assertNull(entity.getNestedObject());
    }

    @Test
    public void testGetKeysRetrievesNonExistentCellsAsNullValues() throws ExecutionException, InterruptedException {
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

        when(table.get(anyList())).thenReturn(Collections.singletonList(CompletableFuture.completedFuture(result)));

        final Key<TestEntity> key = new StringKey<>("key");
        final Set<Key<TestEntity>> keys = Collections.singleton(key);
        final Map<Key<TestEntity>, CompletableFuture<TestEntity>> retrievedEntities = testEntityDao.get(keys);

        assertTrue(retrievedEntities.containsKey(key));

        final CompletableFuture<TestEntity> entityFuture = retrievedEntities.get(key);

        assertTrue(entityFuture.isDone());

        final TestEntity entity = entityFuture.get();
        assertNull(entity.getStringValue());
        assertNull(entity.getBooleanValue());
        assertNull(entity.getNestedObject());
    }

    @Test
    public void testGetKeysRetrievesVersionedValuesAndTimestamps() throws IOException, ExecutionException, InterruptedException {
        final String stringValue = "some string";

        final Boolean booleanValue = true;
        final long booleanValueTimestamp = 1234L;

        final Result result = mock(Result.class);
        when(result.isEmpty()).thenReturn(false);

        final Cell stringValueCell = mock(Cell.class);

        final byte[] stringValueBytes = new byte[]{
                1, 2, 3
        };

        when(stringValueCell.getValueArray()).thenReturn(stringValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestVersionedColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestVersionedColumns.STRING_VALUE.getQualifier()))).thenReturn(stringValueCell);

        when(objectMapper.readValue(stringValueBytes, TestVersionedColumns.STRING_VALUE.getTypeReference()))
                .thenReturn(stringValue);

        final Cell booleanValueCell = mock(Cell.class);

        final byte[] booleanValueBytes = new byte[]{
                0
        };

        when(booleanValueCell.getValueArray()).thenReturn(booleanValueBytes);
        when(booleanValueCell.getTimestamp()).thenReturn(booleanValueTimestamp);

        when(result.getColumnLatestCell(Bytes.toBytes(TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getQualifier()))).thenReturn(booleanValueCell);

        when(objectMapper.readValue(booleanValueBytes, TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getTypeReference()))
                .thenReturn(booleanValue);

        when(table.get(anyList())).thenReturn(Collections.singletonList(CompletableFuture.completedFuture(result)));

        final Key<TestVersionedEntity> key = new StringKey<>("key");
        final Set<Key<TestVersionedEntity>> keys = Collections.singleton(key);
        final Map<Key<TestVersionedEntity>, CompletableFuture<TestVersionedEntity>> retrievedEntities = testVersionedEntityDao.get(keys);

        assertTrue(retrievedEntities.containsKey(key));

        final CompletableFuture<TestVersionedEntity> entityFuture = retrievedEntities.get(key);

        assertTrue(entityFuture.isDone());

        final TestVersionedEntity entity = entityFuture.get();
        assertEquals(stringValue, entity.getStringValue());
        assertEquals(booleanValue, entity.getVersionedBooleanValue());
        assertEquals(booleanValueTimestamp, (long) entity.getVersionedBooleanValueTimestamp());
    }

    @Test
    public void testGetKeysRetrievesNonExistentVersionedCellsAsNullValuesAndNullTimestamps() throws ExecutionException, InterruptedException {
        final Result result = mock(Result.class);
        when(result.isEmpty()).thenReturn(false);

        final Cell stringValueCell = null;

        when(result.getColumnLatestCell(Bytes.toBytes(TestVersionedColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestVersionedColumns.STRING_VALUE.getQualifier()))).thenReturn(stringValueCell);

        final Cell booleanValueCell = null;

        when(result.getColumnLatestCell(Bytes.toBytes(TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getQualifier()))).thenReturn(booleanValueCell);

        when(table.get(anyList())).thenReturn(Collections.singletonList(CompletableFuture.completedFuture(result)));

        final Key<TestVersionedEntity> key = new StringKey<>("key");
        final Set<Key<TestVersionedEntity>> keys = Collections.singleton(key);
        final Map<Key<TestVersionedEntity>, CompletableFuture<TestVersionedEntity>> retrievedEntities = testVersionedEntityDao.get(keys);

        assertTrue(retrievedEntities.containsKey(key));

        final CompletableFuture<TestVersionedEntity> entityFuture = retrievedEntities.get(key);

        assertTrue(entityFuture.isDone());

        final TestVersionedEntity entity = entityFuture.get();
        assertNull(entity.getStringValue());
        assertNull(entity.getVersionedBooleanValue());
        assertNull(entity.getVersionedBooleanValueTimestamp());
    }

    @Test
    public void testGetKeysWithLiveObjectMapper() throws IOException, ExecutionException, InterruptedException {
        testEntityDao = new BigTableEntityAsyncDao<>(table, columns, entityFactory, delegateFactory);

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

        when(table.get(anyList())).thenReturn(Collections.singletonList(CompletableFuture.completedFuture(result)));

        final Key<TestEntity> key = new StringKey<>("key");
        final Set<Key<TestEntity>> keys = Collections.singleton(key);
        final Map<Key<TestEntity>, CompletableFuture<TestEntity>> retrievedEntities = testEntityDao.get(keys);

        assertTrue(retrievedEntities.containsKey(key));

        final CompletableFuture<TestEntity> entityFuture = retrievedEntities.get(key);

        assertTrue(entityFuture.isDone());

        final TestEntity entity = entityFuture.get();
        assertEquals(stringValue, entity.getStringValue());
        assertEquals(booleanValue, entity.getBooleanValue());
        assertEquals(nestedObject, entity.getNestedObject());
    }

    @Test
    public void testGetKeysRetrievesNullStringValueWithLiveObjectMapper() throws IOException, ExecutionException, InterruptedException {
        testEntityDao = new BigTableEntityAsyncDao<>(table, columns, entityFactory, delegateFactory);

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

        when(table.get(anyList())).thenReturn(Collections.singletonList(CompletableFuture.completedFuture(result)));

        final Key<TestEntity> key = new StringKey<>("key");
        final Set<Key<TestEntity>> keys = Collections.singleton(key);
        final Map<Key<TestEntity>, CompletableFuture<TestEntity>> retrievedEntities = testEntityDao.get(keys);

        assertTrue(retrievedEntities.containsKey(key));

        final CompletableFuture<TestEntity> entityFuture = retrievedEntities.get(key);

        assertTrue(entityFuture.isDone());

        final TestEntity entity = entityFuture.get();
        assertEquals(stringValue, entity.getStringValue());
        assertEquals(booleanValue, entity.getBooleanValue());
        assertEquals(nestedObject, entity.getNestedObject());
    }

    @Test
    public void testGetKeysRetrievesNullBooleanValueWithLiveObjectMapper() throws IOException, ExecutionException, InterruptedException {
        testEntityDao = new BigTableEntityAsyncDao<>(table, columns, entityFactory, delegateFactory);

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

        when(table.get(anyList())).thenReturn(Collections.singletonList(CompletableFuture.completedFuture(result)));

        final Key<TestEntity> key = new StringKey<>("key");
        final Set<Key<TestEntity>> keys = Collections.singleton(key);
        final Map<Key<TestEntity>, CompletableFuture<TestEntity>> retrievedEntities = testEntityDao.get(keys);

        assertTrue(retrievedEntities.containsKey(key));

        final CompletableFuture<TestEntity> entityFuture = retrievedEntities.get(key);

        assertTrue(entityFuture.isDone());

        final TestEntity entity = entityFuture.get();
        assertEquals(stringValue, entity.getStringValue());
        assertEquals(booleanValue, entity.getBooleanValue());
        assertEquals(nestedObject, entity.getNestedObject());
    }

    @Test
    public void testGetKeysRetrievesNullNestedObjectWithLiveObjectMapper() throws IOException, ExecutionException, InterruptedException {
        testEntityDao = new BigTableEntityAsyncDao<>(table, columns, entityFactory, delegateFactory);

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

        when(table.get(anyList())).thenReturn(Collections.singletonList(CompletableFuture.completedFuture(result)));

        final Key<TestEntity> key = new StringKey<>("key");
        final Set<Key<TestEntity>> keys = Collections.singleton(key);
        final Map<Key<TestEntity>, CompletableFuture<TestEntity>> retrievedEntities = testEntityDao.get(keys);

        assertTrue(retrievedEntities.containsKey(key));

        final CompletableFuture<TestEntity> entityFuture = retrievedEntities.get(key);

        assertTrue(entityFuture.isDone());

        final TestEntity entity = entityFuture.get();
        assertEquals(stringValue, entity.getStringValue());
        assertEquals(booleanValue, entity.getBooleanValue());
        assertEquals(nestedObject, entity.getNestedObject());
    }

    @Test
    public void testGetKeysReturnsNullWhenRowDoesNotExist() throws ExecutionException, InterruptedException {
        final Result result = mock(Result.class);
        when(result.isEmpty()).thenReturn(true);

        when(table.get(anyList())).thenReturn(Collections.singletonList(CompletableFuture.completedFuture(result)));

        final Key<TestEntity> key = new StringKey<>("key");
        final Set<Key<TestEntity>> keys = Collections.singleton(key);
        final Map<Key<TestEntity>, CompletableFuture<TestEntity>> retrievedEntities = testEntityDao.get(keys);

        assertTrue(retrievedEntities.containsKey(key));

        final CompletableFuture<TestEntity> entityFuture = retrievedEntities.get(key);

        assertTrue(entityFuture.isDone());

        assertNull(entityFuture.get());
    }

    @Test(expected = NullPointerException.class)
    public void testGetAllWithNullKeysThrowsNullPointerException() {
        testEntityDao.getAll(null);
    }

    @Test(expected = IOException.class)
    public void testGetAllIOExceptionWrappedInExecutionException() throws Throwable {
        final Result result = mock(Result.class);
        when(result.isEmpty()).thenReturn(false);

        final Cell stringValueCell = mock(Cell.class);

        final byte[] stringValueBytes = new byte[]{
                1, 2, 3
        };

        when(stringValueCell.getValueArray()).thenReturn(stringValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.STRING_VALUE.getQualifier()))).thenReturn(stringValueCell);

        when(objectMapper.readValue(any(byte[].class), any(TypeReference.class))).thenThrow(new IOException());

        when(table.getAll(anyList())).thenReturn(CompletableFuture.completedFuture(Collections.singletonList(result)));

        final Key<TestEntity> key = new StringKey<>("key");
        final Set<Key<TestEntity>> keys = Collections.singleton(key);
        final CompletableFuture<Map<Key<TestEntity>, TestEntity>> resultFuture = testEntityDao.getAll(keys);

        assertTrue(resultFuture.isDone());

        try {
            final Map<Key<TestEntity>, TestEntity> retrievedEntities = resultFuture.get();
        } catch (InterruptedException e) {
            fail("Exception should not be thrown");
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }

    @Test
    public void testGetAllRetrievesAllColumns() throws IOException, ExecutionException, InterruptedException {
        final String stringValue = "some string";

        final Boolean booleanValue = true;

        final TestNestedObject nestedObject = new TestNestedObject();
        nestedObject.setSomeValue(3);

        final Result result = mock(Result.class);
        when(result.isEmpty()).thenReturn(false);

        final Cell stringValueCell = mock(Cell.class);

        final byte[] stringValueBytes = new byte[]{
                1, 2, 3
        };

        when(stringValueCell.getValueArray()).thenReturn(stringValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.STRING_VALUE.getQualifier()))).thenReturn(stringValueCell);

        when(objectMapper.readValue(stringValueBytes, TestColumns.STRING_VALUE.getTypeReference()))
                .thenReturn(stringValue);

        final Cell booleanValueCell = mock(Cell.class);

        final byte[] booleanValueBytes = new byte[]{
                0
        };

        when(booleanValueCell.getValueArray()).thenReturn(booleanValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getQualifier()))).thenReturn(booleanValueCell);

        when(objectMapper.readValue(booleanValueBytes, TestColumns.BOOLEAN_VALUE.getTypeReference()))
                .thenReturn(booleanValue);

        final Cell nestedObjectCell = mock(Cell.class);

        final byte[] nestedObjectBytes = new byte[]{
                5, 4, 3, 2, 1, 0
        };

        when(nestedObjectCell.getValueArray()).thenReturn(nestedObjectBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.NESTED_OBJECT.getFamily()),
                Bytes.toBytes(TestColumns.NESTED_OBJECT.getQualifier()))).thenReturn(nestedObjectCell);

        when(objectMapper.readValue(nestedObjectBytes, TestColumns.NESTED_OBJECT.getTypeReference()))
                .thenReturn(nestedObject);

        when(table.getAll(anyList())).thenReturn(CompletableFuture.completedFuture(Collections.singletonList(result)));

        final Key<TestEntity> key = new StringKey<>("key");
        final Set<Key<TestEntity>> keys = Collections.singleton(key);
        final CompletableFuture<Map<Key<TestEntity>, TestEntity>> resultFuture = testEntityDao.getAll(keys);

        assertTrue(resultFuture.isDone());

        final Map<Key<TestEntity>, TestEntity> retrievedEntities = resultFuture.get();
        assertTrue(retrievedEntities.containsKey(key));

        final TestEntity entity = retrievedEntities.get(key);
        assertEquals(stringValue, entity.getStringValue());
        assertEquals(booleanValue, entity.getBooleanValue());
        assertEquals(nestedObject, entity.getNestedObject());
    }

    @Test
    public void testGetAllRetrievesAllColumnsForMultipleKeys() throws IOException, ExecutionException, InterruptedException {
        final String stringValue1 = "some string";

        final Boolean booleanValue1 = true;

        final TestNestedObject nestedObject1 = new TestNestedObject();
        nestedObject1.setSomeValue(3);

        final Result result1 = mock(Result.class);
        when(result1.isEmpty()).thenReturn(false);

        final Cell stringValueCell1 = mock(Cell.class);

        final byte[] stringValueBytes1 = new byte[]{
                1, 2, 3
        };

        when(stringValueCell1.getValueArray()).thenReturn(stringValueBytes1);

        when(result1.getColumnLatestCell(Bytes.toBytes(TestColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.STRING_VALUE.getQualifier()))).thenReturn(stringValueCell1);

        when(objectMapper.readValue(stringValueBytes1, TestColumns.STRING_VALUE.getTypeReference()))
                .thenReturn(stringValue1);

        final Cell booleanValueCell1 = mock(Cell.class);

        final byte[] booleanValueBytes1 = new byte[]{
                0
        };

        when(booleanValueCell1.getValueArray()).thenReturn(booleanValueBytes1);

        when(result1.getColumnLatestCell(Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getQualifier()))).thenReturn(booleanValueCell1);

        when(objectMapper.readValue(booleanValueBytes1, TestColumns.BOOLEAN_VALUE.getTypeReference()))
                .thenReturn(booleanValue1);

        final Cell nestedObjectCell1 = mock(Cell.class);

        final byte[] nestedObjectBytes1 = new byte[]{
                5, 4, 3, 2, 1, 0
        };

        when(nestedObjectCell1.getValueArray()).thenReturn(nestedObjectBytes1);

        when(result1.getColumnLatestCell(Bytes.toBytes(TestColumns.NESTED_OBJECT.getFamily()),
                Bytes.toBytes(TestColumns.NESTED_OBJECT.getQualifier()))).thenReturn(nestedObjectCell1);

        when(objectMapper.readValue(nestedObjectBytes1, TestColumns.NESTED_OBJECT.getTypeReference()))
                .thenReturn(nestedObject1);

        final String stringValue2 = "some other string";

        final Boolean booleanValue2 = false;

        final TestNestedObject nestedObject2 = new TestNestedObject();
        nestedObject2.setSomeValue(8);

        final Result result2 = mock(Result.class);
        when(result2.isEmpty()).thenReturn(false);

        final Cell stringValueCell2 = mock(Cell.class);

        final byte[] stringValueBytes2 = new byte[]{
                9, 8, 7
        };

        when(stringValueCell2.getValueArray()).thenReturn(stringValueBytes2);

        when(result2.getColumnLatestCell(Bytes.toBytes(TestColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.STRING_VALUE.getQualifier()))).thenReturn(stringValueCell2);

        when(objectMapper.readValue(stringValueBytes2, TestColumns.STRING_VALUE.getTypeReference()))
                .thenReturn(stringValue2);

        final Cell booleanValueCell2 = mock(Cell.class);

        final byte[] booleanValueBytes2 = new byte[]{
                9
        };

        when(booleanValueCell2.getValueArray()).thenReturn(booleanValueBytes2);

        when(result2.getColumnLatestCell(Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getQualifier()))).thenReturn(booleanValueCell2);

        when(objectMapper.readValue(booleanValueBytes2, TestColumns.BOOLEAN_VALUE.getTypeReference()))
                .thenReturn(booleanValue2);

        final Cell nestedObjectCell2 = mock(Cell.class);

        final byte[] nestedObjectBytes2 = new byte[]{
                3, 3, 3, 3, 3
        };

        when(nestedObjectCell2.getValueArray()).thenReturn(nestedObjectBytes2);

        when(result2.getColumnLatestCell(Bytes.toBytes(TestColumns.NESTED_OBJECT.getFamily()),
                Bytes.toBytes(TestColumns.NESTED_OBJECT.getQualifier()))).thenReturn(nestedObjectCell2);

        when(objectMapper.readValue(nestedObjectBytes2, TestColumns.NESTED_OBJECT.getTypeReference()))
                .thenReturn(nestedObject2);

        final Key<TestEntity> key1 = new StringKey<>("key1");
        final Key<TestEntity> key2 = new StringKey<>("key2");

        // The keys are passed in as a Set to remove duplicates, but converted to a List for use with the HBase API.
        // The HBase API returns an array of Results, with indices corresponding to the List of keys.
        // Since the conversion from a Set to a List does not maintain order, we need to mock this interaction manually.
        when(table.getAll(anyList())).then(invocation -> {
            final List<Get> gets = invocation.getArgument(0);

            if (Arrays.equals(gets.get(0).getRow(), key1.toBytes())) {
                return CompletableFuture.completedFuture(Arrays.asList(result1, result2));
            } else {
                return CompletableFuture.completedFuture(Arrays.asList(result2, result1));
            }
        });

        final Set<Key<TestEntity>> keys = Stream.of(key1, key2).collect(Collectors.toSet());
        final CompletableFuture<Map<Key<TestEntity>, TestEntity>> resultFuture = testEntityDao.getAll(keys);

        assertTrue(resultFuture.isDone());

        final Map<Key<TestEntity>, TestEntity> retrievedEntities = resultFuture.get();

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
    public void testGetAllRetrievesNullValues() throws IOException, ExecutionException, InterruptedException {
        final String stringValue = "some string";

        final Boolean booleanValue = true;

        final TestNestedObject nestedObject = null;

        final Result result = mock(Result.class);
        when(result.isEmpty()).thenReturn(false);

        final Cell stringValueCell = mock(Cell.class);

        final byte[] stringValueBytes = new byte[]{
                1, 2, 3
        };

        when(stringValueCell.getValueArray()).thenReturn(stringValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.STRING_VALUE.getQualifier()))).thenReturn(stringValueCell);

        when(objectMapper.readValue(stringValueBytes, TestColumns.STRING_VALUE.getTypeReference()))
                .thenReturn(stringValue);

        final Cell booleanValueCell = mock(Cell.class);

        final byte[] booleanValueBytes = new byte[]{
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

        when(table.getAll(anyList())).thenReturn(CompletableFuture.completedFuture(Collections.singletonList(result)));

        final Key<TestEntity> key = new StringKey<>("key");
        final Set<Key<TestEntity>> keys = Collections.singleton(key);
        final CompletableFuture<Map<Key<TestEntity>, TestEntity>> resultFuture = testEntityDao.getAll(keys);

        assertTrue(resultFuture.isDone());

        final Map<Key<TestEntity>, TestEntity> retrievedEntities = resultFuture.get();

        assertTrue(retrievedEntities.containsKey(key));

        final TestEntity entity = retrievedEntities.get(key);
        assertNull(entity.getNestedObject());
    }

    @Test
    public void testGetAllRetrievesNonExistentCellsAsNullValues() throws ExecutionException, InterruptedException {
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

        when(table.getAll(anyList())).thenReturn(CompletableFuture.completedFuture(Collections.singletonList(result)));

        final Key<TestEntity> key = new StringKey<>("key");
        final Set<Key<TestEntity>> keys = Collections.singleton(key);
        final CompletableFuture<Map<Key<TestEntity>, TestEntity>> resultFuture = testEntityDao.getAll(keys);

        assertTrue(resultFuture.isDone());

        final Map<Key<TestEntity>, TestEntity> retrievedEntities = resultFuture.get();

        assertTrue(retrievedEntities.containsKey(key));

        final TestEntity entity = retrievedEntities.get(key);
        assertNull(entity.getStringValue());
        assertNull(entity.getBooleanValue());
        assertNull(entity.getNestedObject());
    }

    @Test
    public void testGetAllRetrievesVersionedValuesAndTimestamps() throws IOException, ExecutionException, InterruptedException {
        final String stringValue = "some string";

        final Boolean booleanValue = true;
        final long booleanValueTimestamp = 1234L;

        final Result result = mock(Result.class);
        when(result.isEmpty()).thenReturn(false);

        final Cell stringValueCell = mock(Cell.class);

        final byte[] stringValueBytes = new byte[]{
                1, 2, 3
        };

        when(stringValueCell.getValueArray()).thenReturn(stringValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestVersionedColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestVersionedColumns.STRING_VALUE.getQualifier()))).thenReturn(stringValueCell);

        when(objectMapper.readValue(stringValueBytes, TestVersionedColumns.STRING_VALUE.getTypeReference()))
                .thenReturn(stringValue);

        final Cell booleanValueCell = mock(Cell.class);

        final byte[] booleanValueBytes = new byte[]{
                0
        };

        when(booleanValueCell.getValueArray()).thenReturn(booleanValueBytes);
        when(booleanValueCell.getTimestamp()).thenReturn(booleanValueTimestamp);

        when(result.getColumnLatestCell(Bytes.toBytes(TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getQualifier()))).thenReturn(booleanValueCell);

        when(objectMapper.readValue(booleanValueBytes, TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getTypeReference()))
                .thenReturn(booleanValue);

        when(table.getAll(anyList())).thenReturn(CompletableFuture.completedFuture(Collections.singletonList(result)));

        final Key<TestVersionedEntity> key = new StringKey<>("key");
        final Set<Key<TestVersionedEntity>> keys = Collections.singleton(key);
        final CompletableFuture<Map<Key<TestVersionedEntity>, TestVersionedEntity>> resultFuture = testVersionedEntityDao.getAll(keys);

        assertTrue(resultFuture.isDone());

        final Map<Key<TestVersionedEntity>, TestVersionedEntity> retrievedEntities = resultFuture.get();

        assertTrue(retrievedEntities.containsKey(key));

        final TestVersionedEntity entity = retrievedEntities.get(key);
        assertEquals(stringValue, entity.getStringValue());
        assertEquals(booleanValue, entity.getVersionedBooleanValue());
        assertEquals(booleanValueTimestamp, (long) entity.getVersionedBooleanValueTimestamp());
    }

    @Test
    public void testGetAllRetrievesNonExistentVersionedCellsAsNullValuesAndNullTimestamps() throws ExecutionException, InterruptedException {
        final Result result = mock(Result.class);
        when(result.isEmpty()).thenReturn(false);

        final Cell stringValueCell = null;

        when(result.getColumnLatestCell(Bytes.toBytes(TestVersionedColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestVersionedColumns.STRING_VALUE.getQualifier()))).thenReturn(stringValueCell);

        final Cell booleanValueCell = null;

        when(result.getColumnLatestCell(Bytes.toBytes(TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getQualifier()))).thenReturn(booleanValueCell);

        when(table.getAll(anyList())).thenReturn(CompletableFuture.completedFuture(Collections.singletonList(result)));

        final Key<TestVersionedEntity> key = new StringKey<>("key");
        final Set<Key<TestVersionedEntity>> keys = Collections.singleton(key);
        final CompletableFuture<Map<Key<TestVersionedEntity>, TestVersionedEntity>> resultFuture = testVersionedEntityDao.getAll(keys);

        assertTrue(resultFuture.isDone());

        final Map<Key<TestVersionedEntity>, TestVersionedEntity> retrievedEntities = resultFuture.get();

        assertTrue(retrievedEntities.containsKey(key));

        final TestVersionedEntity entity = retrievedEntities.get(key);
        assertNull(entity.getStringValue());
        assertNull(entity.getVersionedBooleanValue());
        assertNull(entity.getVersionedBooleanValueTimestamp());
    }

    @Test
    public void testGetAllWithLiveObjectMapper() throws IOException, ExecutionException, InterruptedException {
        testEntityDao = new BigTableEntityAsyncDao<>(table, columns, entityFactory, delegateFactory);

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

        when(table.getAll(anyList())).thenReturn(CompletableFuture.completedFuture(Collections.singletonList(result)));

        final Key<TestEntity> key = new StringKey<>("key");
        final Set<Key<TestEntity>> keys = Collections.singleton(key);
        final CompletableFuture<Map<Key<TestEntity>, TestEntity>> resultFuture = testEntityDao.getAll(keys);

        assertTrue(resultFuture.isDone());

        final Map<Key<TestEntity>, TestEntity> retrievedEntities = resultFuture.get();

        assertTrue(retrievedEntities.containsKey(key));

        final TestEntity entity = retrievedEntities.get(key);
        assertEquals(stringValue, entity.getStringValue());
        assertEquals(booleanValue, entity.getBooleanValue());
        assertEquals(nestedObject, entity.getNestedObject());
    }

    @Test
    public void testGetAllRetrievesNullStringValueWithLiveObjectMapper() throws IOException, ExecutionException, InterruptedException {
        testEntityDao = new BigTableEntityAsyncDao<>(table, columns, entityFactory, delegateFactory);

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

        when(table.getAll(anyList())).thenReturn(CompletableFuture.completedFuture(Collections.singletonList(result)));

        final Key<TestEntity> key = new StringKey<>("key");
        final Set<Key<TestEntity>> keys = Collections.singleton(key);
        final CompletableFuture<Map<Key<TestEntity>, TestEntity>> resultFuture = testEntityDao.getAll(keys);

        assertTrue(resultFuture.isDone());

        final Map<Key<TestEntity>, TestEntity> retrievedEntities = resultFuture.get();

        assertTrue(retrievedEntities.containsKey(key));

        final TestEntity entity = retrievedEntities.get(key);
        assertEquals(stringValue, entity.getStringValue());
        assertEquals(booleanValue, entity.getBooleanValue());
        assertEquals(nestedObject, entity.getNestedObject());
    }

    @Test
    public void testGetAllRetrievesNullBooleanValueWithLiveObjectMapper() throws IOException, ExecutionException, InterruptedException {
        testEntityDao = new BigTableEntityAsyncDao<>(table, columns, entityFactory, delegateFactory);

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

        when(table.getAll(anyList())).thenReturn(CompletableFuture.completedFuture(Collections.singletonList(result)));

        final Key<TestEntity> key = new StringKey<>("key");
        final Set<Key<TestEntity>> keys = Collections.singleton(key);
        final CompletableFuture<Map<Key<TestEntity>, TestEntity>> resultFuture = testEntityDao.getAll(keys);

        assertTrue(resultFuture.isDone());

        final Map<Key<TestEntity>, TestEntity> retrievedEntities = resultFuture.get();

        assertTrue(retrievedEntities.containsKey(key));

        final TestEntity entity = retrievedEntities.get(key);
        assertEquals(stringValue, entity.getStringValue());
        assertEquals(booleanValue, entity.getBooleanValue());
        assertEquals(nestedObject, entity.getNestedObject());
    }

    @Test
    public void testGetAllRetrievesNullNestedObjectWithLiveObjectMapper() throws IOException, ExecutionException, InterruptedException {
        testEntityDao = new BigTableEntityAsyncDao<>(table, columns, entityFactory, delegateFactory);

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

        when(table.getAll(anyList())).thenReturn(CompletableFuture.completedFuture(Collections.singletonList(result)));

        final Key<TestEntity> key = new StringKey<>("key");
        final Set<Key<TestEntity>> keys = Collections.singleton(key);
        final CompletableFuture<Map<Key<TestEntity>, TestEntity>> resultFuture = testEntityDao.getAll(keys);

        assertTrue(resultFuture.isDone());

        final Map<Key<TestEntity>, TestEntity> retrievedEntities = resultFuture.get();

        assertTrue(retrievedEntities.containsKey(key));

        final TestEntity entity = retrievedEntities.get(key);
        assertEquals(stringValue, entity.getStringValue());
        assertEquals(booleanValue, entity.getBooleanValue());
        assertEquals(nestedObject, entity.getNestedObject());
    }

    @Test
    public void testGetAllDoesNotContainKeyWhenRowDoesNotExist() throws ExecutionException, InterruptedException {
        final Result result = mock(Result.class);
        when(result.isEmpty()).thenReturn(true);

        when(table.getAll(anyList())).thenReturn(CompletableFuture.completedFuture(Collections.singletonList(result)));

        final Key<TestEntity> key = new StringKey<>("key");
        final Set<Key<TestEntity>> keys = Collections.singleton(key);
        final CompletableFuture<Map<Key<TestEntity>, TestEntity>> resultFuture = testEntityDao.getAll(keys);

        assertTrue(resultFuture.isDone());

        final Map<Key<TestEntity>, TestEntity> retrievedEntities = resultFuture.get();

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

    @Test(expected = IOException.class)
    public void testScanIOExceptionWrappedInExecutionException() throws Throwable {
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

        final byte[] stringValueBytes = new byte[]{
                1, 2, 3
        };

        when(stringValueCell.getValueArray()).thenReturn(stringValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.STRING_VALUE.getQualifier()))).thenReturn(stringValueCell);

        when(objectMapper.readValue(stringValueBytes, TestColumns.STRING_VALUE.getTypeReference()))
                .thenThrow(new IOException());

        final Cell booleanValueCell = mock(Cell.class);

        final byte[] booleanValueBytes = new byte[]{
                0
        };

        when(booleanValueCell.getValueArray()).thenReturn(booleanValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getQualifier()))).thenReturn(booleanValueCell);

        when(objectMapper.readValue(booleanValueBytes, TestColumns.BOOLEAN_VALUE.getTypeReference()))
                .thenReturn(booleanValue);

        final Cell nestedObjectCell = mock(Cell.class);

        final byte[] nestedObjectBytes = new byte[]{
                5, 4, 3, 2, 1, 0
        };

        when(nestedObjectCell.getValueArray()).thenReturn(nestedObjectBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.NESTED_OBJECT.getFamily()),
                Bytes.toBytes(TestColumns.NESTED_OBJECT.getQualifier()))).thenReturn(nestedObjectCell);

        when(objectMapper.readValue(nestedObjectBytes, TestColumns.NESTED_OBJECT.getTypeReference()))
                .thenReturn(nestedObject);

        when(table.scanAll(any(Scan.class))).thenReturn(CompletableFuture.completedFuture(Collections.singletonList(result)));

        final CompletableFuture<SortedMap<Key<TestEntity>, TestEntity>> resultFuture =
                testEntityDao.scan(startKey, true, endKey, true, 1);

        assertTrue(resultFuture.isDone());

        try {
            resultFuture.get();
        } catch (InterruptedException e) {
            fail("Exception should not be thrown");
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }

    @Test
    public void testScanRetrievesAllColumns() throws IOException, ExecutionException, InterruptedException {
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

        final byte[] stringValueBytes = new byte[]{
                1, 2, 3
        };

        when(stringValueCell.getValueArray()).thenReturn(stringValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.STRING_VALUE.getQualifier()))).thenReturn(stringValueCell);

        when(objectMapper.readValue(stringValueBytes, TestColumns.STRING_VALUE.getTypeReference()))
                .thenReturn(stringValue);

        final Cell booleanValueCell = mock(Cell.class);

        final byte[] booleanValueBytes = new byte[]{
                0
        };

        when(booleanValueCell.getValueArray()).thenReturn(booleanValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getQualifier()))).thenReturn(booleanValueCell);

        when(objectMapper.readValue(booleanValueBytes, TestColumns.BOOLEAN_VALUE.getTypeReference()))
                .thenReturn(booleanValue);

        final Cell nestedObjectCell = mock(Cell.class);

        final byte[] nestedObjectBytes = new byte[]{
                5, 4, 3, 2, 1, 0
        };

        when(nestedObjectCell.getValueArray()).thenReturn(nestedObjectBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.NESTED_OBJECT.getFamily()),
                Bytes.toBytes(TestColumns.NESTED_OBJECT.getQualifier()))).thenReturn(nestedObjectCell);

        when(objectMapper.readValue(nestedObjectBytes, TestColumns.NESTED_OBJECT.getTypeReference()))
                .thenReturn(nestedObject);

        when(table.scanAll(any(Scan.class))).thenReturn(CompletableFuture.completedFuture(Collections.singletonList(result)));

        final CompletableFuture<SortedMap<Key<TestEntity>, TestEntity>> resultFuture =
                testEntityDao.scan(startKey, true, endKey, true, 1);

        assertTrue(resultFuture.isDone());

        final SortedMap<Key<TestEntity>, TestEntity> retrievedEntities = resultFuture.get();

        assertEquals(1, retrievedEntities.size());

        final TestEntity entity = retrievedEntities.get(retrievedEntities.firstKey());
        assertEquals(stringValue, entity.getStringValue());
        assertEquals(booleanValue, entity.getBooleanValue());
        assertEquals(nestedObject, entity.getNestedObject());
    }

    @Test
    public void testScanRetrievesAllColumnsForMultipleRows() throws IOException, ExecutionException, InterruptedException {
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

        final byte[] stringValueBytes1 = new byte[]{
                1, 2, 3
        };

        when(stringValueCell1.getValueArray()).thenReturn(stringValueBytes1);

        when(result1.getColumnLatestCell(Bytes.toBytes(TestColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.STRING_VALUE.getQualifier()))).thenReturn(stringValueCell1);

        when(objectMapper.readValue(stringValueBytes1, TestColumns.STRING_VALUE.getTypeReference()))
                .thenReturn(stringValue1);

        final Cell booleanValueCell1 = mock(Cell.class);

        final byte[] booleanValueBytes1 = new byte[]{
                0
        };

        when(booleanValueCell1.getValueArray()).thenReturn(booleanValueBytes1);

        when(result1.getColumnLatestCell(Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getQualifier()))).thenReturn(booleanValueCell1);

        when(objectMapper.readValue(booleanValueBytes1, TestColumns.BOOLEAN_VALUE.getTypeReference()))
                .thenReturn(booleanValue1);

        final Cell nestedObjectCell1 = mock(Cell.class);

        final byte[] nestedObjectBytes1 = new byte[]{
                5, 4, 3, 2, 1, 0
        };

        when(nestedObjectCell1.getValueArray()).thenReturn(nestedObjectBytes1);

        when(result1.getColumnLatestCell(Bytes.toBytes(TestColumns.NESTED_OBJECT.getFamily()),
                Bytes.toBytes(TestColumns.NESTED_OBJECT.getQualifier()))).thenReturn(nestedObjectCell1);

        when(objectMapper.readValue(nestedObjectBytes1, TestColumns.NESTED_OBJECT.getTypeReference()))
                .thenReturn(nestedObject1);

        final String stringValue2 = "some other string";

        final Boolean booleanValue2 = false;

        final TestNestedObject nestedObject2 = new TestNestedObject();
        nestedObject2.setSomeValue(8);

        final Result result2 = mock(Result.class);
        when(result2.getRow()).thenReturn(endKey.toBytes());
        when(result2.isEmpty()).thenReturn(false);

        final Cell stringValueCell2 = mock(Cell.class);

        final byte[] stringValueBytes2 = new byte[]{
                9, 8, 7
        };

        when(stringValueCell2.getValueArray()).thenReturn(stringValueBytes2);

        when(result2.getColumnLatestCell(Bytes.toBytes(TestColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.STRING_VALUE.getQualifier()))).thenReturn(stringValueCell2);

        when(objectMapper.readValue(stringValueBytes2, TestColumns.STRING_VALUE.getTypeReference()))
                .thenReturn(stringValue2);

        final Cell booleanValueCell2 = mock(Cell.class);

        final byte[] booleanValueBytes2 = new byte[]{
                9
        };

        when(booleanValueCell2.getValueArray()).thenReturn(booleanValueBytes2);

        when(result2.getColumnLatestCell(Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getQualifier()))).thenReturn(booleanValueCell2);

        when(objectMapper.readValue(booleanValueBytes2, TestColumns.BOOLEAN_VALUE.getTypeReference()))
                .thenReturn(booleanValue2);

        final Cell nestedObjectCell2 = mock(Cell.class);

        final byte[] nestedObjectBytes2 = new byte[]{
                3, 3, 3, 3, 3
        };

        when(nestedObjectCell2.getValueArray()).thenReturn(nestedObjectBytes2);

        when(result2.getColumnLatestCell(Bytes.toBytes(TestColumns.NESTED_OBJECT.getFamily()),
                Bytes.toBytes(TestColumns.NESTED_OBJECT.getQualifier()))).thenReturn(nestedObjectCell2);

        when(objectMapper.readValue(nestedObjectBytes2, TestColumns.NESTED_OBJECT.getTypeReference()))
                .thenReturn(nestedObject2);

        when(table.scanAll(any(Scan.class))).thenReturn(CompletableFuture.completedFuture(Arrays.asList(result1, result2)));

        final CompletableFuture<SortedMap<Key<TestEntity>, TestEntity>> resultFuture = testEntityDao.scan(startKey, true, endKey, true, 5);

        assertTrue(resultFuture.isDone());

        final SortedMap<Key<TestEntity>, TestEntity> retrievedEntities = resultFuture.get();

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
    public void testScanRetrievesNullValues() throws IOException, ExecutionException, InterruptedException {
        final String stringValue = "some string";
        final Key<TestEntity> startKey = new StringKey<>("a"); // [97]
        final Key<TestEntity> endKey = new StringKey<>("z"); // [122]

        final Boolean booleanValue = true;

        final TestNestedObject nestedObject = null;

        final Result result = mock(Result.class);
        when(result.getRow()).thenReturn(startKey.toBytes());
        when(result.isEmpty()).thenReturn(false);

        final Cell stringValueCell = mock(Cell.class);

        final byte[] stringValueBytes = new byte[]{
                1, 2, 3
        };

        when(stringValueCell.getValueArray()).thenReturn(stringValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.STRING_VALUE.getQualifier()))).thenReturn(stringValueCell);

        when(objectMapper.readValue(stringValueBytes, TestColumns.STRING_VALUE.getTypeReference()))
                .thenReturn(stringValue);

        final Cell booleanValueCell = mock(Cell.class);

        final byte[] booleanValueBytes = new byte[]{
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

        when(table.scanAll(any(Scan.class))).thenReturn(CompletableFuture.completedFuture(Collections.singletonList(result)));

        final CompletableFuture<SortedMap<Key<TestEntity>, TestEntity>> resultFuture = testEntityDao.scan(startKey, true, endKey, true, 1);

        assertTrue(resultFuture.isDone());

        final SortedMap<Key<TestEntity>, TestEntity> retrievedEntities = resultFuture.get();

        assertEquals(1, retrievedEntities.size());

        final TestEntity entity = retrievedEntities.get(retrievedEntities.firstKey());
        assertNull(entity.getNestedObject());
    }

    @Test
    public void testScanRetrievesNonExistentCellsAsNullValues() throws IOException, ExecutionException, InterruptedException {
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

        when(table.scanAll(any(Scan.class))).thenReturn(CompletableFuture.completedFuture(Collections.singletonList(result)));

        final CompletableFuture<SortedMap<Key<TestEntity>, TestEntity>> resultFuture = testEntityDao.scan(startKey, true, endKey, true, 1);

        assertTrue(resultFuture.isDone());

        final SortedMap<Key<TestEntity>, TestEntity> retrievedEntities = resultFuture.get();

        assertEquals(1, retrievedEntities.size());

        final TestEntity entity = retrievedEntities.get(retrievedEntities.firstKey());
        assertNull(entity.getStringValue());
        assertNull(entity.getBooleanValue());
        assertNull(entity.getNestedObject());
    }

    @Test
    public void testScanRetrievesVersionedValuesAndTimestamps() throws IOException, ExecutionException, InterruptedException {
        final String stringValue = "some string";
        final Key<TestVersionedEntity> startKey = new StringKey<>("a"); // [97]
        final Key<TestVersionedEntity> endKey = new StringKey<>("z"); // [122]

        final Boolean booleanValue = true;
        final long booleanValueTimestamp = 1234L;

        final Result result = mock(Result.class);
        when(result.getRow()).thenReturn(startKey.toBytes());
        when(result.isEmpty()).thenReturn(false);

        final Cell stringValueCell = mock(Cell.class);

        final byte[] stringValueBytes = new byte[]{
                1, 2, 3
        };

        when(stringValueCell.getValueArray()).thenReturn(stringValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestVersionedColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestVersionedColumns.STRING_VALUE.getQualifier()))).thenReturn(stringValueCell);

        when(objectMapper.readValue(stringValueBytes, TestVersionedColumns.STRING_VALUE.getTypeReference()))
                .thenReturn(stringValue);

        final Cell booleanValueCell = mock(Cell.class);

        final byte[] booleanValueBytes = new byte[]{
                0
        };

        when(booleanValueCell.getValueArray()).thenReturn(booleanValueBytes);
        when(booleanValueCell.getTimestamp()).thenReturn(booleanValueTimestamp);

        when(result.getColumnLatestCell(Bytes.toBytes(TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getQualifier()))).thenReturn(booleanValueCell);

        when(objectMapper.readValue(booleanValueBytes, TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getTypeReference()))
                .thenReturn(booleanValue);

        when(table.scanAll(any(Scan.class))).thenReturn(CompletableFuture.completedFuture(Collections.singletonList(result)));

        final CompletableFuture<SortedMap<Key<TestVersionedEntity>, TestVersionedEntity>> resultFuture
                = testVersionedEntityDao.scan(startKey, true, endKey, true, 1);

        assertTrue(resultFuture.isDone());

        final SortedMap<Key<TestVersionedEntity>, TestVersionedEntity> retrievedEntities = resultFuture.get();

        assertEquals(1, retrievedEntities.size());

        final TestVersionedEntity entity = retrievedEntities.get(retrievedEntities.firstKey());
        assertEquals(stringValue, entity.getStringValue());
        assertEquals(booleanValue, entity.getVersionedBooleanValue());
        assertEquals(booleanValueTimestamp, (long) entity.getVersionedBooleanValueTimestamp());
    }

    @Test
    public void testScanRetrievesNonExistentVersionedCellsAsNullValuesAndNullTimestamps() throws IOException, ExecutionException, InterruptedException {
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

        when(table.scanAll(any(Scan.class))).thenReturn(CompletableFuture.completedFuture(Collections.singletonList(result)));

        final CompletableFuture<SortedMap<Key<TestVersionedEntity>, TestVersionedEntity>> resultFuture
                = testVersionedEntityDao.scan(startKey, true, endKey, true, 1);

        assertTrue(resultFuture.isDone());

        final SortedMap<Key<TestVersionedEntity>, TestVersionedEntity> retrievedEntities = resultFuture.get();

        assertEquals(1, retrievedEntities.size());

        final TestVersionedEntity entity = retrievedEntities.get(retrievedEntities.firstKey());
        assertNull(entity.getStringValue());
        assertNull(entity.getVersionedBooleanValue());
        assertNull(entity.getVersionedBooleanValueTimestamp());
    }

    @Test
    public void testScanWithLiveObjectMapper() throws IOException, ExecutionException, InterruptedException {
        final Key<TestEntity> startKey = new StringKey<>("a"); // [97]
        final Key<TestEntity> endKey = new StringKey<>("z"); // [122]
        testEntityDao = new BigTableEntityAsyncDao<>(table, columns, entityFactory, delegateFactory);

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

        when(table.scanAll(any(Scan.class))).thenReturn(CompletableFuture.completedFuture(Collections.singletonList(result)));

        final CompletableFuture<SortedMap<Key<TestEntity>, TestEntity>> resultFuture
                = testEntityDao.scan(startKey, true, endKey, true, 1);

        assertTrue(resultFuture.isDone());

        final SortedMap<Key<TestEntity>, TestEntity> retrievedEntities = resultFuture.get();

        assertEquals(1, retrievedEntities.size());

        final TestEntity entity = retrievedEntities.get(retrievedEntities.firstKey());
        assertEquals(stringValue, entity.getStringValue());
        assertEquals(booleanValue, entity.getBooleanValue());
        assertEquals(nestedObject, entity.getNestedObject());
    }

    @Test
    public void testScanRetrievesNullStringValueWithLiveObjectMapper() throws IOException, ExecutionException, InterruptedException {
        final Key<TestEntity> startKey = new StringKey<>("a"); // [97]
        final Key<TestEntity> endKey = new StringKey<>("z"); // [122]
        testEntityDao = new BigTableEntityAsyncDao<>(table, columns, entityFactory, delegateFactory);

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

        when(table.scanAll(any(Scan.class))).thenReturn(CompletableFuture.completedFuture(Collections.singletonList(result)));

        final CompletableFuture<SortedMap<Key<TestEntity>, TestEntity>> resultFuture
                = testEntityDao.scan(startKey, true, endKey, true, 1);

        assertTrue(resultFuture.isDone());

        final SortedMap<Key<TestEntity>, TestEntity> retrievedEntities = resultFuture.get();

        assertEquals(1, retrievedEntities.size());

        final TestEntity entity = retrievedEntities.get(retrievedEntities.firstKey());
        assertEquals(stringValue, entity.getStringValue());
        assertEquals(booleanValue, entity.getBooleanValue());
        assertEquals(nestedObject, entity.getNestedObject());
    }

    @Test
    public void testScanRetrievesNullBooleanValueWithLiveObjectMapper() throws IOException, ExecutionException, InterruptedException {
        final Key<TestEntity> startKey = new StringKey<>("a"); // [97]
        final Key<TestEntity> endKey = new StringKey<>("z"); // [122]
        testEntityDao = new BigTableEntityAsyncDao<>(table, columns, entityFactory, delegateFactory);

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

        when(table.scanAll(any(Scan.class))).thenReturn(CompletableFuture.completedFuture(Collections.singletonList(result)));

        final CompletableFuture<SortedMap<Key<TestEntity>, TestEntity>> resultFuture
                = testEntityDao.scan(startKey, true, endKey, true, 1);

        assertTrue(resultFuture.isDone());

        final SortedMap<Key<TestEntity>, TestEntity> retrievedEntities = resultFuture.get();

        assertEquals(1, retrievedEntities.size());

        final TestEntity entity = retrievedEntities.get(retrievedEntities.firstKey());
        assertEquals(stringValue, entity.getStringValue());
        assertEquals(booleanValue, entity.getBooleanValue());
        assertEquals(nestedObject, entity.getNestedObject());
    }

    @Test
    public void testScanRetrievesNullNestedObjectWithLiveObjectMapper() throws IOException, ExecutionException, InterruptedException {
        final Key<TestEntity> startKey = new StringKey<>("a"); // [97]
        final Key<TestEntity> endKey = new StringKey<>("z"); // [122]

        testEntityDao = new BigTableEntityAsyncDao<>(table, columns, entityFactory, delegateFactory);

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

        when(table.scanAll(any(Scan.class))).thenReturn(CompletableFuture.completedFuture(Collections.singletonList(result)));

        final CompletableFuture<SortedMap<Key<TestEntity>, TestEntity>> resultFuture
                = testEntityDao.scan(startKey, true, endKey, true, 1);

        assertTrue(resultFuture.isDone());

        final SortedMap<Key<TestEntity>, TestEntity> retrievedEntities = resultFuture.get();

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

        final byte[] stringValueBytes = new byte[]{
                1, 2, 3
        };

        when(stringValueCell.getValueArray()).thenReturn(stringValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.STRING_VALUE.getQualifier()))).thenReturn(stringValueCell);

        when(objectMapper.readValue(stringValueBytes, TestColumns.STRING_VALUE.getTypeReference()))
                .thenReturn(stringValue);

        final Cell booleanValueCell = mock(Cell.class);

        final byte[] booleanValueBytes = new byte[]{
                0
        };

        when(booleanValueCell.getValueArray()).thenReturn(booleanValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getQualifier()))).thenReturn(booleanValueCell);

        when(objectMapper.readValue(booleanValueBytes, TestColumns.BOOLEAN_VALUE.getTypeReference()))
                .thenReturn(booleanValue);

        final Cell nestedObjectCell = mock(Cell.class);

        final byte[] nestedObjectBytes = new byte[]{
                5, 4, 3, 2, 1, 0
        };

        when(nestedObjectCell.getValueArray()).thenReturn(nestedObjectBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.NESTED_OBJECT.getFamily()),
                Bytes.toBytes(TestColumns.NESTED_OBJECT.getQualifier()))).thenReturn(nestedObjectCell);

        when(objectMapper.readValue(nestedObjectBytes, TestColumns.NESTED_OBJECT.getTypeReference()))
                .thenReturn(nestedObject);

        when(table.scanAll(scanArgumentCaptor.capture())).thenReturn(CompletableFuture.completedFuture(Collections.singletonList(result)));

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

        final byte[] stringValueBytes = new byte[]{
                1, 2, 3
        };

        when(stringValueCell.getValueArray()).thenReturn(stringValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.STRING_VALUE.getQualifier()))).thenReturn(stringValueCell);

        when(objectMapper.readValue(stringValueBytes, TestColumns.STRING_VALUE.getTypeReference()))
                .thenReturn(stringValue);

        final Cell booleanValueCell = mock(Cell.class);

        final byte[] booleanValueBytes = new byte[]{
                0
        };

        when(booleanValueCell.getValueArray()).thenReturn(booleanValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getQualifier()))).thenReturn(booleanValueCell);

        when(objectMapper.readValue(booleanValueBytes, TestColumns.BOOLEAN_VALUE.getTypeReference()))
                .thenReturn(booleanValue);

        final Cell nestedObjectCell = mock(Cell.class);

        final byte[] nestedObjectBytes = new byte[]{
                5, 4, 3, 2, 1, 0
        };

        when(nestedObjectCell.getValueArray()).thenReturn(nestedObjectBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.NESTED_OBJECT.getFamily()),
                Bytes.toBytes(TestColumns.NESTED_OBJECT.getQualifier()))).thenReturn(nestedObjectCell);

        when(objectMapper.readValue(nestedObjectBytes, TestColumns.NESTED_OBJECT.getTypeReference()))
                .thenReturn(nestedObject);

        when(table.scanAll(scanArgumentCaptor.capture())).thenReturn(CompletableFuture.completedFuture(Collections.singletonList(result)));

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

        final byte[] stringValueBytes = new byte[]{
                1, 2, 3
        };

        when(stringValueCell.getValueArray()).thenReturn(stringValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.STRING_VALUE.getQualifier()))).thenReturn(stringValueCell);

        when(objectMapper.readValue(stringValueBytes, TestColumns.STRING_VALUE.getTypeReference()))
                .thenReturn(stringValue);

        final Cell booleanValueCell = mock(Cell.class);

        final byte[] booleanValueBytes = new byte[]{
                0
        };

        when(booleanValueCell.getValueArray()).thenReturn(booleanValueBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestColumns.BOOLEAN_VALUE.getQualifier()))).thenReturn(booleanValueCell);

        when(objectMapper.readValue(booleanValueBytes, TestColumns.BOOLEAN_VALUE.getTypeReference()))
                .thenReturn(booleanValue);

        final Cell nestedObjectCell = mock(Cell.class);

        final byte[] nestedObjectBytes = new byte[]{
                5, 4, 3, 2, 1, 0
        };

        when(nestedObjectCell.getValueArray()).thenReturn(nestedObjectBytes);

        when(result.getColumnLatestCell(Bytes.toBytes(TestColumns.NESTED_OBJECT.getFamily()),
                Bytes.toBytes(TestColumns.NESTED_OBJECT.getQualifier()))).thenReturn(nestedObjectCell);

        when(objectMapper.readValue(nestedObjectBytes, TestColumns.NESTED_OBJECT.getTypeReference()))
                .thenReturn(nestedObject);

        when(table.scanAll(scanArgumentCaptor.capture())).thenReturn(CompletableFuture.completedFuture(Collections.singletonList(result)));

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
    public void testSavePutsAllColumns() throws IOException, ExecutionException, InterruptedException {
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

        when(table.put(any(Put.class))).thenReturn(CompletableFuture.runAsync(() -> {
        }));

        final CompletableFuture<TestEntity> resultFuture = testEntityDao.save(new StringKey<>("key"), testEntity);

        assertTrue(resultFuture.isDone());

        final TestEntity result = resultFuture.get();

        assertNotNull(result);

        assertEquals(stringValue, result.getStringValue());
        assertEquals(booleanValue, result.getBooleanValue());
        assertEquals(nestedObject, result.getNestedObject());

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
    public void testSavePutsAllColumnsWithLiveObjectMapper() throws IOException, ExecutionException, InterruptedException {
        testEntityDao = new BigTableEntityAsyncDao<>(table, columns, entityFactory, delegateFactory);

        final String stringValue = "some string";

        final Boolean booleanValue = true;

        final TestNestedObject nestedObject = new TestNestedObject();
        nestedObject.setSomeValue(3);

        final TestEntity testEntity = new TestEntity();
        testEntity.setStringValue(stringValue);
        testEntity.setBooleanValue(booleanValue);
        testEntity.setNestedObject(nestedObject);

        when(table.put(any(Put.class))).thenReturn(CompletableFuture.runAsync(() -> {
        }));

        final CompletableFuture<TestEntity> resultFuture = testEntityDao.save(new StringKey<>("key"), testEntity);

        assertTrue(resultFuture.isDone());

        final TestEntity result = resultFuture.get();

        assertNotNull(result);

        assertEquals(stringValue, result.getStringValue());
        assertEquals(booleanValue, result.getBooleanValue());
        assertEquals(nestedObject, result.getNestedObject());

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
    public void testSavePutsNullStringValueWithLiveObjectMapper() throws IOException, ExecutionException, InterruptedException {
        testEntityDao = new BigTableEntityAsyncDao<>(table, columns, entityFactory, delegateFactory);

        final String stringValue = null;

        final Boolean booleanValue = true;

        final TestNestedObject nestedObject = new TestNestedObject();
        nestedObject.setSomeValue(3);

        final TestEntity testEntity = new TestEntity();
        testEntity.setStringValue(stringValue);
        testEntity.setBooleanValue(booleanValue);
        testEntity.setNestedObject(nestedObject);

        when(table.put(any(Put.class))).thenReturn(CompletableFuture.runAsync(() -> {
        }));

        final CompletableFuture<TestEntity> resultFuture = testEntityDao.save(new StringKey<>("key"), testEntity);

        assertTrue(resultFuture.isDone());

        final TestEntity result = resultFuture.get();

        assertNotNull(result);

        assertEquals(stringValue, result.getStringValue());
        assertEquals(booleanValue, result.getBooleanValue());
        assertEquals(nestedObject, result.getNestedObject());

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
    public void testSavePutsNullBooleanValueWithLiveObjectMapper() throws IOException, ExecutionException, InterruptedException {
        testEntityDao = new BigTableEntityAsyncDao<>(table, columns, entityFactory, delegateFactory);

        final String stringValue = "some string";

        final Boolean booleanValue = null;

        final TestNestedObject nestedObject = new TestNestedObject();
        nestedObject.setSomeValue(3);

        final TestEntity testEntity = new TestEntity();
        testEntity.setStringValue(stringValue);
        testEntity.setBooleanValue(booleanValue);
        testEntity.setNestedObject(nestedObject);

        when(table.put(any(Put.class))).thenReturn(CompletableFuture.runAsync(() -> {
        }));

        final CompletableFuture<TestEntity> resultFuture = testEntityDao.save(new StringKey<>("key"), testEntity);

        assertTrue(resultFuture.isDone());

        final TestEntity result = resultFuture.get();

        assertNotNull(result);

        assertEquals(stringValue, result.getStringValue());
        assertEquals(booleanValue, result.getBooleanValue());
        assertEquals(nestedObject, result.getNestedObject());

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
    public void testSaveWithPutsNullNestedObjectWithLiveObjectMapper() throws IOException, ExecutionException, InterruptedException {
        testEntityDao = new BigTableEntityAsyncDao<>(table, columns, entityFactory, delegateFactory);

        final String stringValue = "some string";

        final Boolean booleanValue = true;

        final TestNestedObject nestedObject = null;

        final TestEntity testEntity = new TestEntity();
        testEntity.setStringValue(stringValue);
        testEntity.setBooleanValue(booleanValue);
        testEntity.setNestedObject(nestedObject);

        when(table.put(any(Put.class))).thenReturn(CompletableFuture.runAsync(() -> {
        }));

        final CompletableFuture<TestEntity> resultFuture = testEntityDao.save(new StringKey<>("key"), testEntity);

        assertTrue(resultFuture.isDone());

        final TestEntity result = resultFuture.get();

        assertNotNull(result);

        assertEquals(stringValue, result.getStringValue());
        assertEquals(booleanValue, result.getBooleanValue());
        assertEquals(nestedObject, result.getNestedObject());

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
    public void testSavePutsNullValues() throws IOException, ExecutionException, InterruptedException {
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

        when(table.put(any(Put.class))).thenReturn(CompletableFuture.runAsync(() -> {
        }));

        final CompletableFuture<TestEntity> resultFuture = testEntityDao.save(new StringKey<>("key"), testEntity);

        assertTrue(resultFuture.isDone());

        final TestEntity result = resultFuture.get();

        assertNotNull(result);

        assertEquals(stringValue, result.getStringValue());
        assertEquals(booleanValue, result.getBooleanValue());
        assertEquals(nestedObject, result.getNestedObject());

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
    public void testSavePutsVersionedColumnsWithDefinedTimestamp() throws IOException, ExecutionException, InterruptedException {
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

        when(table.put(any(Put.class))).thenReturn(CompletableFuture.runAsync(() -> {
        }));

        final CompletableFuture<TestVersionedEntity> resultFuture = testVersionedEntityDao.save(new StringKey<>("key"), testVersionedEntity);

        assertTrue(resultFuture.isDone());

        final TestVersionedEntity result = resultFuture.get();

        assertNotNull(result);

        assertEquals(stringValue, result.getStringValue());
        assertEquals(booleanValue, result.getVersionedBooleanValue());
        assertEquals(booleanValueTimestamp, (long) result.getVersionedBooleanValueTimestamp());

        verify(table).put(putArgumentCaptor.capture());

        final Put put = putArgumentCaptor.getValue();
        assertNotNull(put);

        assertTrue(put.has(Bytes.toBytes(TestVersionedColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestVersionedColumns.STRING_VALUE.getQualifier()), stringValueBytes));
        assertTrue(put.has(Bytes.toBytes(TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getQualifier()), booleanValueBytes));
    }

    @Test
    public void testSavePutsVersionedColumnsWithUndefinedTimestamp() throws IOException, ExecutionException, InterruptedException {
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

        when(table.put(any(Put.class))).thenReturn(CompletableFuture.runAsync(() -> {
        }));

        final CompletableFuture<TestVersionedEntity> resultFuture = testVersionedEntityDao.save(new StringKey<>("key"), testVersionedEntity);

        assertTrue(resultFuture.isDone());

        final TestVersionedEntity result = resultFuture.get();

        assertNotNull(result);

        assertEquals(stringValue, result.getStringValue());
        assertEquals(booleanValue, result.getVersionedBooleanValue());
        assertTrue(result.getVersionedBooleanValueTimestamp() >= atLeastTimestamp);

        verify(table).put(putArgumentCaptor.capture());

        final Put put = putArgumentCaptor.getValue();
        assertNotNull(put);

        assertTrue(put.has(Bytes.toBytes(TestVersionedColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestVersionedColumns.STRING_VALUE.getQualifier()), stringValueBytes));
        assertTrue(put.has(Bytes.toBytes(TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getQualifier()), booleanValueBytes));
    }

    @Test(expected = NullPointerException.class)
    public void testSaveEntitiesWithNullMapThrowsNullPointerException() throws IOException {
        testEntityDao.save(null);
    }

    @Test
    public void testSaveEntitiesPutsAllColumns() throws IOException, ExecutionException, InterruptedException {
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

        when(table.put(anyList())).thenReturn(Collections.singletonList(CompletableFuture.runAsync(() -> {
        })));

        final Key<TestEntity> key = new StringKey<>("key");
        final Map<Key<TestEntity>, TestEntity> entities = Collections.singletonMap(key, testEntity);

        final Map<Key<TestEntity>, CompletableFuture<TestEntity>> savedEntities = testEntityDao.save(entities);

        assertNotNull(savedEntities);
        assertTrue(savedEntities.containsKey(key));

        final CompletableFuture<TestEntity> entityFuture = savedEntities.get(key);
        assertTrue(entityFuture.isDone());

        final TestEntity result = entityFuture.get();
        assertEquals(stringValue, result.getStringValue());
        assertEquals(booleanValue, result.getBooleanValue());
        assertEquals(nestedObject, result.getNestedObject());

        verify(table).put(putsArgumentCaptor.capture());

        final List<Put> puts = putsArgumentCaptor.getValue();
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
    public void testSaveEntitiesPutsAllColumnsForMultipleEntities() throws IOException, ExecutionException, InterruptedException {
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

        when(table.put(anyList())).thenReturn(Arrays.asList(CompletableFuture.runAsync(() -> {
        }), CompletableFuture.runAsync(() -> {
        })));

        final Key<TestEntity> key1 = new StringKey<>("key1");
        final Key<TestEntity> key2 = new StringKey<>("key2");

        final Map<Key<TestEntity>, TestEntity> entities = new HashMap<>();
        entities.put(key1, testEntity1);
        entities.put(key2, testEntity2);

        final Map<Key<TestEntity>, CompletableFuture<TestEntity>> savedEntities = testEntityDao.save(entities);

        assertNotNull(savedEntities);
        assertTrue(savedEntities.containsKey(key1));

        final CompletableFuture<TestEntity> entityFuture1 = savedEntities.get(key1);
        assertTrue(entityFuture1.isDone());

        final TestEntity result1 = entityFuture1.get();
        assertEquals(stringValue1, result1.getStringValue());
        assertEquals(booleanValue1, result1.getBooleanValue());
        assertEquals(nestedObject1, result1.getNestedObject());

        assertTrue(savedEntities.containsKey(key2));

        final CompletableFuture<TestEntity> entityFuture2 = savedEntities.get(key2);
        assertTrue(entityFuture2.isDone());

        final TestEntity result2 = entityFuture2.get();
        assertEquals(stringValue2, result2.getStringValue());
        assertEquals(booleanValue2, result2.getBooleanValue());
        assertEquals(nestedObject2, result2.getNestedObject());

        verify(table).put(putsArgumentCaptor.capture());

        final List<Put> puts = putsArgumentCaptor.getValue();
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
    public void testSaveEntitiesPutsAllColumnsWithLiveObjectMapper() throws IOException, ExecutionException, InterruptedException {
        testEntityDao = new BigTableEntityAsyncDao<>(table, columns, entityFactory, delegateFactory);

        final String stringValue = "some string";

        final Boolean booleanValue = true;

        final TestNestedObject nestedObject = new TestNestedObject();
        nestedObject.setSomeValue(3);

        final TestEntity testEntity = new TestEntity();
        testEntity.setStringValue(stringValue);
        testEntity.setBooleanValue(booleanValue);
        testEntity.setNestedObject(nestedObject);

        when(table.put(anyList())).thenReturn(Collections.singletonList(CompletableFuture.runAsync(() -> {
        })));

        final Key<TestEntity> key = new StringKey<>("key");
        final Map<Key<TestEntity>, TestEntity> entities = Collections.singletonMap(key, testEntity);

        final Map<Key<TestEntity>, CompletableFuture<TestEntity>> savedEntities = testEntityDao.save(entities);

        assertNotNull(savedEntities);
        assertTrue(savedEntities.containsKey(key));

        final CompletableFuture<TestEntity> entityFuture = savedEntities.get(key);
        assertTrue(entityFuture.isDone());

        final TestEntity result = entityFuture.get();
        assertEquals(stringValue, result.getStringValue());
        assertEquals(booleanValue, result.getBooleanValue());
        assertEquals(nestedObject, result.getNestedObject());

        verify(table).put(putsArgumentCaptor.capture());

        final List<Put> puts = putsArgumentCaptor.getValue();
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
    public void testSaveEntitiesPutsNullStringValueWithLiveObjectMapper() throws IOException, ExecutionException, InterruptedException {
        testEntityDao = new BigTableEntityAsyncDao<>(table, columns, entityFactory, delegateFactory);

        final String stringValue = null;

        final Boolean booleanValue = true;

        final TestNestedObject nestedObject = new TestNestedObject();
        nestedObject.setSomeValue(3);

        final TestEntity testEntity = new TestEntity();
        testEntity.setStringValue(stringValue);
        testEntity.setBooleanValue(booleanValue);
        testEntity.setNestedObject(nestedObject);

        when(table.put(anyList())).thenReturn(Collections.singletonList(CompletableFuture.runAsync(() -> {
        })));

        final Key<TestEntity> key = new StringKey<>("key");
        final Map<Key<TestEntity>, TestEntity> entities = Collections.singletonMap(key, testEntity);

        final Map<Key<TestEntity>, CompletableFuture<TestEntity>> savedEntities = testEntityDao.save(entities);

        assertNotNull(savedEntities);
        assertTrue(savedEntities.containsKey(key));

        final CompletableFuture<TestEntity> entityFuture = savedEntities.get(key);
        assertTrue(entityFuture.isDone());

        final TestEntity result = entityFuture.get();
        assertEquals(stringValue, result.getStringValue());
        assertEquals(booleanValue, result.getBooleanValue());
        assertEquals(nestedObject, result.getNestedObject());

        verify(table).put(putsArgumentCaptor.capture());

        final List<Put> puts = putsArgumentCaptor.getValue();
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
    public void testSaveEntitiesPutsNullBooleanValueWithLiveObjectMapper() throws IOException, ExecutionException, InterruptedException {
        testEntityDao = new BigTableEntityAsyncDao<>(table, columns, entityFactory, delegateFactory);

        final String stringValue = "some string";

        final Boolean booleanValue = null;

        final TestNestedObject nestedObject = new TestNestedObject();
        nestedObject.setSomeValue(3);

        final TestEntity testEntity = new TestEntity();
        testEntity.setStringValue(stringValue);
        testEntity.setBooleanValue(booleanValue);
        testEntity.setNestedObject(nestedObject);

        when(table.put(anyList())).thenReturn(Collections.singletonList(CompletableFuture.runAsync(() -> {
        })));

        final Key<TestEntity> key = new StringKey<>("key");
        final Map<Key<TestEntity>, TestEntity> entities = Collections.singletonMap(key, testEntity);

        final Map<Key<TestEntity>, CompletableFuture<TestEntity>> savedEntities = testEntityDao.save(entities);

        assertNotNull(savedEntities);
        assertTrue(savedEntities.containsKey(key));

        final CompletableFuture<TestEntity> entityFuture = savedEntities.get(key);
        assertTrue(entityFuture.isDone());

        final TestEntity result = entityFuture.get();
        assertEquals(stringValue, result.getStringValue());
        assertEquals(booleanValue, result.getBooleanValue());
        assertEquals(nestedObject, result.getNestedObject());

        verify(table).put(putsArgumentCaptor.capture());

        final List<Put> puts = putsArgumentCaptor.getValue();
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
    public void testSaveEntitiesWithPutsNullNestedObjectWithLiveObjectMapper() throws IOException, ExecutionException, InterruptedException {
        testEntityDao = new BigTableEntityAsyncDao<>(table, columns, entityFactory, delegateFactory);

        final String stringValue = "some string";

        final Boolean booleanValue = true;

        final TestNestedObject nestedObject = null;

        final TestEntity testEntity = new TestEntity();
        testEntity.setStringValue(stringValue);
        testEntity.setBooleanValue(booleanValue);
        testEntity.setNestedObject(nestedObject);

        final Key<TestEntity> key = new StringKey<>("key");
        final Map<Key<TestEntity>, TestEntity> entities = Collections.singletonMap(key, testEntity);

        when(table.put(anyList())).thenReturn(Collections.singletonList(CompletableFuture.runAsync(() -> {
        })));

        final Map<Key<TestEntity>, CompletableFuture<TestEntity>> savedEntities = testEntityDao.save(entities);

        assertNotNull(savedEntities);
        assertTrue(savedEntities.containsKey(key));

        final CompletableFuture<TestEntity> entityFuture = savedEntities.get(key);
        assertTrue(entityFuture.isDone());

        final TestEntity result = entityFuture.get();
        assertEquals(stringValue, result.getStringValue());
        assertEquals(booleanValue, result.getBooleanValue());
        assertEquals(nestedObject, result.getNestedObject());

        verify(table).put(putsArgumentCaptor.capture());

        final List<Put> puts = putsArgumentCaptor.getValue();
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
    public void testSaveEntitiesPutsNullValues() throws IOException, ExecutionException, InterruptedException {
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

        when(table.put(anyList())).thenReturn(Collections.singletonList(CompletableFuture.runAsync(() -> {
        })));

        final byte[] nestedObjectBytes = new byte[0];

        final Key<TestEntity> key = new StringKey<>("key");
        final Map<Key<TestEntity>, TestEntity> entities = Collections.singletonMap(key, testEntity);

        final Map<Key<TestEntity>, CompletableFuture<TestEntity>> savedEntities = testEntityDao.save(entities);

        assertNotNull(savedEntities);
        assertTrue(savedEntities.containsKey(key));

        final CompletableFuture<TestEntity> entityFuture = savedEntities.get(key);
        assertTrue(entityFuture.isDone());

        final TestEntity result = entityFuture.get();
        assertEquals(stringValue, result.getStringValue());
        assertEquals(booleanValue, result.getBooleanValue());
        assertEquals(nestedObject, result.getNestedObject());

        verify(table).put(putsArgumentCaptor.capture());

        final List<Put> puts = putsArgumentCaptor.getValue();
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
    public void testSaveEntitiesPutsVersionedColumnsWithDefinedTimestamp() throws IOException, ExecutionException, InterruptedException {
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

        when(table.put(anyList())).thenReturn(Collections.singletonList(CompletableFuture.runAsync(() -> {
        })));

        final Key<TestVersionedEntity> key = new StringKey<>("key");
        final Map<Key<TestVersionedEntity>, TestVersionedEntity> entities = Collections.singletonMap(key, testVersionedEntity);

        final Map<Key<TestVersionedEntity>, CompletableFuture<TestVersionedEntity>> savedEntities = testVersionedEntityDao.save(entities);

        assertNotNull(savedEntities);
        assertTrue(savedEntities.containsKey(key));

        final CompletableFuture<TestVersionedEntity> entityFuture = savedEntities.get(key);
        assertTrue(entityFuture.isDone());

        final TestVersionedEntity result = entityFuture.get();
        assertEquals(stringValue, result.getStringValue());
        assertEquals(booleanValue, result.getVersionedBooleanValue());
        assertEquals(booleanValueTimestamp, (long) result.getVersionedBooleanValueTimestamp());

        verify(table).put(putsArgumentCaptor.capture());

        final List<Put> puts = putsArgumentCaptor.getValue();
        assertNotNull(puts);
        assertEquals(1, puts.size());

        final Put put = puts.get(0);

        assertTrue(put.has(Bytes.toBytes(TestVersionedColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestVersionedColumns.STRING_VALUE.getQualifier()), stringValueBytes));
        assertTrue(put.has(Bytes.toBytes(TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getQualifier()), booleanValueBytes));
    }

    @Test
    public void testSaveEntitiesPutsVersionedColumnsWithUndefinedTimestamp() throws IOException, ExecutionException, InterruptedException {
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

        when(table.put(anyList())).thenReturn(Collections.singletonList(CompletableFuture.runAsync(() -> {
        })));

        final long atLeastTimestamp = Instant.now().toEpochMilli();

        final Key<TestVersionedEntity> key = new StringKey<>("key");
        final Map<Key<TestVersionedEntity>, TestVersionedEntity> entities = Collections.singletonMap(key, testVersionedEntity);

        final Map<Key<TestVersionedEntity>, CompletableFuture<TestVersionedEntity>> savedEntities = testVersionedEntityDao.save(entities);

        assertNotNull(savedEntities);
        assertTrue(savedEntities.containsKey(key));

        final CompletableFuture<TestVersionedEntity> entityFuture = savedEntities.get(key);
        assertTrue(entityFuture.isDone());

        final TestVersionedEntity result = entityFuture.get();
        assertEquals(stringValue, result.getStringValue());
        assertEquals(booleanValue, result.getVersionedBooleanValue());
        assertTrue(result.getVersionedBooleanValueTimestamp() >= atLeastTimestamp);

        verify(table).put(putsArgumentCaptor.capture());

        final List<Put> puts = putsArgumentCaptor.getValue();
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
    public void testSaveAllPutsAllColumns() throws IOException, ExecutionException, InterruptedException {
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

        when(table.putAll(anyList())).thenReturn(CompletableFuture.runAsync(() -> {
        }));

        final Key<TestEntity> key = new StringKey<>("key");
        final Map<Key<TestEntity>, TestEntity> entities = Collections.singletonMap(key, testEntity);

        final CompletableFuture<Map<Key<TestEntity>, TestEntity>> entitiesFuture = testEntityDao.saveAll(entities);

        assertTrue(entitiesFuture.isDone());
        final Map<Key<TestEntity>, TestEntity> savedEntities = entitiesFuture.get();

        assertNotNull(savedEntities);
        assertTrue(savedEntities.containsKey(key));

        final TestEntity result = savedEntities.get(key);
        assertEquals(stringValue, result.getStringValue());
        assertEquals(booleanValue, result.getBooleanValue());
        assertEquals(nestedObject, result.getNestedObject());

        verify(table).putAll(putsArgumentCaptor.capture());

        final List<Put> puts = putsArgumentCaptor.getValue();
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
    public void testSaveAllPutsAllColumnsForMultipleEntities() throws IOException, ExecutionException, InterruptedException {
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

        when(table.putAll(anyList())).thenReturn(CompletableFuture.runAsync(() -> {
        }));

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

        final CompletableFuture<Map<Key<TestEntity>, TestEntity>> entitiesFuture = testEntityDao.saveAll(entities);

        assertTrue(entitiesFuture.isDone());
        final Map<Key<TestEntity>, TestEntity> savedEntities = entitiesFuture.get();

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

        verify(table).putAll(putsArgumentCaptor.capture());

        final List<Put> puts = putsArgumentCaptor.getValue();
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
    public void testSaveAllPutsAllColumnsWithLiveObjectMapper() throws IOException, ExecutionException, InterruptedException {
        testEntityDao = new BigTableEntityAsyncDao<>(table, columns, entityFactory, delegateFactory);

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

        when(table.putAll(anyList())).thenReturn(CompletableFuture.runAsync(() -> {
        }));

        final CompletableFuture<Map<Key<TestEntity>, TestEntity>> entitiesFuture = testEntityDao.saveAll(entities);

        assertTrue(entitiesFuture.isDone());
        final Map<Key<TestEntity>, TestEntity> savedEntities = entitiesFuture.get();

        assertNotNull(savedEntities);
        assertTrue(savedEntities.containsKey(key));

        final TestEntity result = savedEntities.get(key);
        assertEquals(stringValue, result.getStringValue());
        assertEquals(booleanValue, result.getBooleanValue());
        assertEquals(nestedObject, result.getNestedObject());

        verify(table).putAll(putsArgumentCaptor.capture());

        final List<Put> puts = putsArgumentCaptor.getValue();
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
    public void testSaveAllPutsNullStringValueWithLiveObjectMapper() throws IOException, ExecutionException, InterruptedException {
        testEntityDao = new BigTableEntityAsyncDao<>(table, columns, entityFactory, delegateFactory);

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

        when(table.putAll(anyList())).thenReturn(CompletableFuture.runAsync(() -> {
        }));

        final CompletableFuture<Map<Key<TestEntity>, TestEntity>> entitiesFuture = testEntityDao.saveAll(entities);

        assertTrue(entitiesFuture.isDone());
        final Map<Key<TestEntity>, TestEntity> savedEntities = entitiesFuture.get();

        assertNotNull(savedEntities);
        assertTrue(savedEntities.containsKey(key));

        final TestEntity result = savedEntities.get(key);
        assertEquals(stringValue, result.getStringValue());
        assertEquals(booleanValue, result.getBooleanValue());
        assertEquals(nestedObject, result.getNestedObject());

        verify(table).putAll(putsArgumentCaptor.capture());

        final List<Put> puts = putsArgumentCaptor.getValue();
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
    public void testSaveAllPutsNullBooleanValueWithLiveObjectMapper() throws IOException, ExecutionException, InterruptedException {
        testEntityDao = new BigTableEntityAsyncDao<>(table, columns, entityFactory, delegateFactory);

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

        when(table.putAll(anyList())).thenReturn(CompletableFuture.runAsync(() -> {
        }));

        final CompletableFuture<Map<Key<TestEntity>, TestEntity>> entitiesFuture = testEntityDao.saveAll(entities);

        assertTrue(entitiesFuture.isDone());
        final Map<Key<TestEntity>, TestEntity> savedEntities = entitiesFuture.get();

        assertNotNull(savedEntities);
        assertTrue(savedEntities.containsKey(key));

        final TestEntity result = savedEntities.get(key);
        assertEquals(stringValue, result.getStringValue());
        assertEquals(booleanValue, result.getBooleanValue());
        assertEquals(nestedObject, result.getNestedObject());

        verify(table).putAll(putsArgumentCaptor.capture());

        final List<Put> puts = putsArgumentCaptor.getValue();
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
    public void testSaveAllWithPutsNullNestedObjectWithLiveObjectMapper() throws IOException, ExecutionException, InterruptedException {
        testEntityDao = new BigTableEntityAsyncDao<>(table, columns, entityFactory, delegateFactory);

        final String stringValue = "some string";

        final Boolean booleanValue = true;

        final TestNestedObject nestedObject = null;

        final TestEntity testEntity = new TestEntity();
        testEntity.setStringValue(stringValue);
        testEntity.setBooleanValue(booleanValue);
        testEntity.setNestedObject(nestedObject);

        final Key<TestEntity> key = new StringKey<>("key");
        final Map<Key<TestEntity>, TestEntity> entities = Collections.singletonMap(key, testEntity);

        when(table.putAll(anyList())).thenReturn(CompletableFuture.runAsync(() -> {
        }));

        final CompletableFuture<Map<Key<TestEntity>, TestEntity>> entitiesFuture = testEntityDao.saveAll(entities);

        assertTrue(entitiesFuture.isDone());
        final Map<Key<TestEntity>, TestEntity> savedEntities = entitiesFuture.get();

        assertNotNull(savedEntities);
        assertTrue(savedEntities.containsKey(key));

        final TestEntity result = savedEntities.get(key);
        assertEquals(stringValue, result.getStringValue());
        assertEquals(booleanValue, result.getBooleanValue());
        assertEquals(nestedObject, result.getNestedObject());

        verify(table).putAll(putsArgumentCaptor.capture());

        final List<Put> puts = putsArgumentCaptor.getValue();
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
    public void testSaveAllPutsNullValues() throws IOException, ExecutionException, InterruptedException {
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

        when(table.putAll(anyList())).thenReturn(CompletableFuture.runAsync(() -> {
        }));

        final CompletableFuture<Map<Key<TestEntity>, TestEntity>> entitiesFuture = testEntityDao.saveAll(entities);

        assertTrue(entitiesFuture.isDone());
        final Map<Key<TestEntity>, TestEntity> savedEntities = entitiesFuture.get();

        assertNotNull(savedEntities);
        assertTrue(savedEntities.containsKey(key));

        final TestEntity result = savedEntities.get(key);
        assertEquals(stringValue, result.getStringValue());
        assertEquals(booleanValue, result.getBooleanValue());
        assertEquals(nestedObject, result.getNestedObject());

        verify(table).putAll(putsArgumentCaptor.capture());

        final List<Put> puts = putsArgumentCaptor.getValue();
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
    public void testSaveAllPutsVersionedColumnsWithDefinedTimestamp() throws IOException, ExecutionException, InterruptedException {
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

        when(table.putAll(anyList())).thenReturn(CompletableFuture.runAsync(() -> {
        }));

        final CompletableFuture<Map<Key<TestVersionedEntity>, TestVersionedEntity>> entitiesFuture = testVersionedEntityDao.saveAll(entities);

        assertTrue(entitiesFuture.isDone());
        final Map<Key<TestVersionedEntity>, TestVersionedEntity> savedEntities = entitiesFuture.get();

        assertNotNull(savedEntities);
        assertTrue(savedEntities.containsKey(key));

        final TestVersionedEntity result = savedEntities.get(key);
        assertEquals(stringValue, result.getStringValue());
        assertEquals(booleanValue, result.getVersionedBooleanValue());
        assertEquals(booleanValueTimestamp, (long) result.getVersionedBooleanValueTimestamp());

        verify(table).putAll(putsArgumentCaptor.capture());

        final List<Put> puts = putsArgumentCaptor.getValue();
        assertNotNull(puts);
        assertEquals(1, puts.size());

        final Put put = puts.get(0);

        assertTrue(put.has(Bytes.toBytes(TestVersionedColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestVersionedColumns.STRING_VALUE.getQualifier()), stringValueBytes));
        assertTrue(put.has(Bytes.toBytes(TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getQualifier()), booleanValueBytes));
    }

    @Test
    public void testSaveAllPutsVersionedColumnsWithUndefinedTimestamp() throws IOException, ExecutionException, InterruptedException {
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

        when(table.putAll(anyList())).thenReturn(CompletableFuture.runAsync(() -> {
        }));

        final CompletableFuture<Map<Key<TestVersionedEntity>, TestVersionedEntity>> entitiesFuture = testVersionedEntityDao.saveAll(entities);

        assertTrue(entitiesFuture.isDone());
        final Map<Key<TestVersionedEntity>, TestVersionedEntity> savedEntities = entitiesFuture.get();

        assertNotNull(savedEntities);
        assertTrue(savedEntities.containsKey(key));

        final TestVersionedEntity result = savedEntities.get(key);
        assertEquals(stringValue, result.getStringValue());
        assertEquals(booleanValue, result.getVersionedBooleanValue());
        assertTrue(result.getVersionedBooleanValueTimestamp() >= atLeastTimestamp);

        verify(table).putAll(putsArgumentCaptor.capture());

        final List<Put> puts = putsArgumentCaptor.getValue();
        assertNotNull(puts);
        assertEquals(1, puts.size());

        final Put put = puts.get(0);

        assertTrue(put.has(Bytes.toBytes(TestVersionedColumns.STRING_VALUE.getFamily()),
                Bytes.toBytes(TestVersionedColumns.STRING_VALUE.getQualifier()), stringValueBytes));
        assertTrue(put.has(Bytes.toBytes(TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getFamily()),
                Bytes.toBytes(TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.getQualifier()), booleanValueBytes));
    }

    @Test(expected = NullPointerException.class)
    public void testDeleteWithNullKeyThrowsNullPointerException() {
        final Key<TestEntity> key = null;
        testEntityDao.delete(key);
    }

    @Test
    public void testDeleteDeletesAllColumns() {
        testEntityDao.delete(new StringKey<>("key"));

        verify(table).delete(deleteArgumentCaptor.capture());

        final Delete delete = deleteArgumentCaptor.getValue();

        // Delete has no easy way to verify which column family/qualifiers are being deleted
        assertNotNull(delete);
    }

    @Test(expected = NullPointerException.class)
    public void testDeleteKeysWithNullKeysThrowsNullPointerException() {
        testEntityDao.deleteAll(null);
    }

    @Test
    public void testDeleteKeysDeletesAllColumns() {
        final Key<TestEntity> key = new StringKey<>("key");
        final Set<Key<TestEntity>> keys = Collections.singleton(key);

        testEntityDao.delete(keys);

        verify(table).delete(deletesArgumentCaptor.capture());

        final List<Delete> deletes = deletesArgumentCaptor.getValue();
        assertNotNull(deletes);
        assertEquals(1, deletes.size());

        // Delete has no easy way to verify which column family/qualifiers are being deleted
        final Delete delete = deletes.get(0);
        assertNotNull(delete);
    }

    @Test
    public void testDeleteKeysDeletesAllColumnsForMultipleKeys() {
        final Key<TestEntity> key1 = new StringKey<>("key1");
        final Key<TestEntity> key2 = new StringKey<>("key2");

        final Set<Key<TestEntity>> keys = Stream.of(key1, key2).collect(Collectors.toSet());

        testEntityDao.delete(keys);

        verify(table).delete(deletesArgumentCaptor.capture());

        final List<Delete> deletes = deletesArgumentCaptor.getValue();
        assertNotNull(deletes);
        assertEquals(2, deletes.size());

        // Delete has no easy way to verify which column family/qualifiers are being deleted
        final Delete delete1 = deletes.get(0);
        assertNotNull(delete1);

        final Delete delete2 = deletes.get(1);
        assertNotNull(delete2);
    }

    @Test(expected = NullPointerException.class)
    public void testDeleteWithNullKeysThrowsNullPointerException() {
        testEntityDao.deleteAll(null);
    }

    @Test
    public void testDeleteAllDeletesAllColumns() {
        final Key<TestEntity> key = new StringKey<>("key");
        final Set<Key<TestEntity>> keys = Collections.singleton(key);

        testEntityDao.deleteAll(keys);

        verify(table).deleteAll(deletesArgumentCaptor.capture());

        final List<Delete> deletes = deletesArgumentCaptor.getValue();
        assertNotNull(deletes);
        assertEquals(1, deletes.size());

        // Delete has no easy way to verify which column family/qualifiers are being deleted
        final Delete delete = deletes.get(0);
        assertNotNull(delete);
    }

    @Test
    public void testDeleteAllDeletesAllColumnsForMultipleKeys() {
        final Key<TestEntity> key1 = new StringKey<>("key1");
        final Key<TestEntity> key2 = new StringKey<>("key2");

        final Set<Key<TestEntity>> keys = Stream.of(key1, key2).collect(Collectors.toSet());

        testEntityDao.deleteAll(keys);

        verify(table).deleteAll(deletesArgumentCaptor.capture());

        final List<Delete> deletes = deletesArgumentCaptor.getValue();
        assertNotNull(deletes);
        assertEquals(2, deletes.size());

        // Delete has no easy way to verify which column family/qualifiers are being deleted
        final Delete delete1 = deletes.get(0);
        assertNotNull(delete1);

        final Delete delete2 = deletes.get(1);
        assertNotNull(delete2);
    }
}
