package com.bettercloud.bigtable.orm;

import com.fasterxml.jackson.core.type.TypeReference;

import java.util.Objects;

abstract class AbstractBigTableEntityDaoTest {

    static class TestEntity implements Entity {

        private String stringValue;
        private Boolean booleanValue;
        private TestNestedObject nestedObject;

        String getStringValue() {
            return stringValue;
        }

        void setStringValue(final String stringValue) {
            this.stringValue = stringValue;
        }

        Boolean getBooleanValue() {
            return booleanValue;
        }

        void setBooleanValue(final Boolean booleanValue) {
            this.booleanValue = booleanValue;
        }

        TestNestedObject getNestedObject() {
            return nestedObject;
        }

        void setNestedObject(final TestNestedObject nestedObject) {
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

    static class TestNestedObject {

        private int someValue;

        public int getSomeValue() {
            return someValue;
        }

        void setSomeValue(final int someValue) {
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

    enum TestColumns implements Column {

        STRING_VALUE("stringValueFamily", "stringValueQualifier", new TypeReference<String>() {
        }, false),
        BOOLEAN_VALUE("booleanValueFamily", "booleanValueQualifier", new TypeReference<Boolean>() {
        }, false),
        NESTED_OBJECT("nestedObjectFamily", "nestedObjectQualifier", new TypeReference<TestNestedObject>() {
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

    static class TestDelegate implements EntityConfiguration.EntityDelegate<TestEntity> {

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

    static class TestVersionedEntity implements Entity {

        private String stringValue;
        private Boolean versionedBooleanValue;
        private Long versionedBooleanValueTimestamp;

        String getStringValue() {
            return stringValue;
        }

        void setStringValue(final String stringValue) {
            this.stringValue = stringValue;
        }

        Boolean getVersionedBooleanValue() {
            return versionedBooleanValue;
        }

        Long getVersionedBooleanValueTimestamp() {
            return versionedBooleanValueTimestamp;
        }

        void setVersionedBooleanValue(final Boolean versionedBooleanValue) {
            this.versionedBooleanValue = versionedBooleanValue;
            this.versionedBooleanValueTimestamp = null;
        }

        void setVersionedBooleanValue(final Boolean versionedBooleanValue, final long timestamp) {
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

    enum TestVersionedColumns implements Column {

        STRING_VALUE("stringValueFamily", "stringValueQualifier", new TypeReference<String>() {
        }, false),
        VERSIONED_BOOLEAN_VALUE("versionedBooleanValueFamily", "versionedBooleanValueQualifier", new TypeReference<Boolean>() {
        }, true);

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

    static class TestVersionedDelegate implements EntityConfiguration.EntityDelegate<TestVersionedEntity> {

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
        public void setColumnTimestamp(final Column column, final Long timestamp) {
            if (TestVersionedColumns.VERSIONED_BOOLEAN_VALUE.equals(column)) {
                entity.setVersionedBooleanValueTimestamp(timestamp);
            } else {
                throw new IllegalArgumentException();
            }
        }
    }
}
