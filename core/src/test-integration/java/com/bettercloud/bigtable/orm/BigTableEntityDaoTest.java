package com.bettercloud.bigtable.orm;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.bigtable.repackaged.com.google.cloud.NoCredentials;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BigTableEntityDaoTest {

    private static final String DOCKER_IMAGE = "spotify/bigtable-emulator:latest";
    private static final String TABLE_NAME = "metadata";
    private static final StringKey ENTITY_KEY_1 = new StringKey<TestEntity>("key::test1");
    private static final StringKey ENTITY_KEY_2 = new StringKey<TestEntity>("key::test2");
    private static final StringKey ENTITY_KEY_3 = new StringKey<TestEntity>("badkey::test");
    private static final TestEntity ENTITY_1 = generateTestEntity1();
    private static final TestEntity ENTITY_2 = generateTestEntity2();

    @ClassRule
    public static final GenericContainer container = new GenericContainer(DOCKER_IMAGE).withExposedPorts(8080);

    private static Dao<TestEntity> testEntityDao;

    @BeforeClass
    public static void setupBigTable() throws IOException {
        final Configuration configuration = BigtableConfiguration.configure("dummyProjectId", "dummyInstanceId");
        configuration.set(BigtableOptionsFactory.BIGTABLE_EMULATOR_HOST_KEY,
                          container.getContainerIpAddress() + ":" + container.getFirstMappedPort());

        final Connection connection = BigtableConfiguration.connect(
                BigtableConfiguration.withCredentials(configuration, NoCredentials.getInstance()));

        final TableName tableName = TableName.valueOf(TABLE_NAME);
        final Admin admin = connection.getAdmin();
        if (!admin.tableExists(tableName)) {
            final HTableDescriptor descriptor = new HTableDescriptor(tableName);
            descriptor.addFamily(new HColumnDescriptor("stringValueFamily"));
            descriptor.addFamily(new HColumnDescriptor("booleanValueFamily"));
            descriptor.addFamily(new HColumnDescriptor("nestedObjectFamily"));
            admin.createTable(descriptor);
        }

        final DaoFactory daoFactory = new DaoFactory(connection);

        testEntityDao = daoFactory.daoFor(TestEntity.class);
    }

    @Before
    public void setup() throws IOException {
        final Map<Key<TestEntity>, TestEntity> entityMap = new HashMap<>();
        entityMap.put(ENTITY_KEY_1, ENTITY_1);
        entityMap.put(ENTITY_KEY_2, ENTITY_2);
        entityMap.put(ENTITY_KEY_3, ENTITY_2);

        testEntityDao.saveAll(entityMap);
    }

    @After
    public void cleanup() throws IOException {
        testEntityDao.deleteAll(new HashSet<>(Arrays.asList(ENTITY_KEY_1, ENTITY_KEY_2)));
    }

    @Test
    public void testGetRetrievesAllColumns() throws IOException {
        final Optional<TestEntity> retrievedEntity = testEntityDao.get(ENTITY_KEY_1);

        assertTrue(retrievedEntity.isPresent());

        final TestEntity entity = retrievedEntity.get();
        assertEquals(ENTITY_1.getStringValue(), entity.getStringValue());
        assertEquals(ENTITY_1.getBooleanValue(), entity.getBooleanValue());
        assertEquals(ENTITY_1.getNestedObject(), entity.getNestedObject());
    }

    @Test
    public void testGetAllRetrievesAllColumns() throws IOException {
        final Map<Key<TestEntity>, TestEntity> retrievedEntities = testEntityDao.getAll(
                new HashSet<>(Arrays.asList((Key<TestEntity>) ENTITY_KEY_1, (Key<TestEntity>) ENTITY_KEY_2)));
        validateEntities(retrievedEntities);
    }

    @Test
    public void testScanRetrievesAllRows() throws IOException {
        final Scan scan = new Scan().setFilter(new PrefixFilter(Bytes.toBytes("key")));

        final Map<Key<TestEntity>, TestEntity> retrievedEntities = testEntityDao.scan(scan);
        validateEntities(retrievedEntities);
    }

    private void validateEntities(final Map<Key<TestEntity>, TestEntity> retrievedEntities) {
        assertEquals(2, retrievedEntities.size());
        assertTrue(retrievedEntities.containsKey(ENTITY_KEY_1));
        assertTrue(retrievedEntities.containsKey(ENTITY_KEY_2));
        assertFalse(retrievedEntities.containsKey(ENTITY_KEY_3));
    }

    private static TestEntity generateTestEntity1() {
        final String stringValue = "some string";

        final Boolean booleanValue = true;

        final TestNestedObject nestedObject = new TestNestedObject();
        nestedObject.setSomeValue(3);

        TestEntity testEntity = new TestEntity();
        testEntity.setBooleanValue(booleanValue);
        testEntity.setStringValue(stringValue);
        testEntity.setNestedObject(nestedObject);

        return testEntity;
    }

    private static TestEntity generateTestEntity2() {
        final String stringValue2 = "some other string";

        final Boolean booleanValue2 = false;

        final TestNestedObject nestedObject2 = new TestNestedObject();
        nestedObject2.setSomeValue(8);

        TestEntity testEntity = new TestEntity();
        testEntity.setBooleanValue(booleanValue2);
        testEntity.setStringValue(stringValue2);
        testEntity.setNestedObject(nestedObject2);

        return testEntity;
    }

    private static class TestEntity extends RegisterableEntity {

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

            return Objects.equals(stringValue, that.stringValue) && Objects.equals(booleanValue, that.booleanValue) && Objects.equals(nestedObject,
                                                                                                                                      that.nestedObject);
        }

        @Override
        public int hashCode() {
            return Objects.hash(stringValue, booleanValue, nestedObject);
        }

        static {
            register(TestEntity.TestConfiguration.INSTANCE, TestEntity.class);
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
            public void setColumnTimestamp(final Column column, final Long timestamp) {
                throw new IllegalArgumentException();
            }
        }

        private static class TestConfiguration implements EntityConfiguration<TestEntity> {
            private static final EntityConfiguration<TestEntity> INSTANCE = new TestEntity.TestConfiguration();
            private static final List<Column> COLUMNS = Arrays.asList(TestEntity.TestColumns.values());
            private static final Supplier<TestEntity> FACTORY = TestEntity::new;

            private TestConfiguration() {
            }

            public String getDefaultTableName() {
                return "metadata";
            }

            public Iterable<Column> getColumns() {
                return COLUMNS;
            }

            public Supplier<TestEntity> getEntityFactory() {
                return FACTORY;
            }

            public EntityDelegate<TestEntity> getDelegateForEntity(TestEntity entity) {
                return new TestEntity.TestDelegate(entity);
            }
        }

        private enum TestColumns implements Column {

            STRING_VALUE("stringValueFamily", "stringValueQualifier", new TypeReference<String>() {
            }, false), BOOLEAN_VALUE("booleanValueFamily", "booleanValueQualifier", new TypeReference<Boolean>() {
            }, false), NESTED_OBJECT("nestedObjectFamily", "nestedObjectQualifier", new TypeReference<TestNestedObject>() {
            }, false);

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
}