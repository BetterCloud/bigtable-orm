Java BigTable ORM
=================

This library abstracts the process of model creation for Google BigTable. Using annotation processing, Entity and DAO definitions are generated automatically, reducing the amount of boilerplate required for new Entities, thus removing the chance for copy/paste errors and making code reviews much simpler.

The design of this library takes a dual-front approach, attempting to create easy-to-use APIs for both the creation of new Entities and working with those entities and their respective DAOs from consuming code.

Table of Contents
-----------------

* [Quick Start](#quick-start)
    * [Gradle](#gradle)
    * [Entity Configuration](#entity-configuration)
    * [Establishing a BigTable Connection](#establishing-a-bigtable-connection)
    * [Retrieving an Entity DAO](#retrieving-an-entity-dao)
    * [DAO Usage](#dao-usage)
* [Detailed Usage](#detailed-usage)
    * [Table Declarations](#table-declarations)
    * [Entity Declarations](#entity-declarations)
        * [Key Components](#key-components)
    * [Column Declarations](#column-declarations)
        * [Primitive Types](#primitive-types)
        * [Custom Types](#custom-types)
        * [Column Sharing](#column-sharing)
    * [Unit Testing](#unit-testing)
* [Contributing](#contributing)
* [History](#history)
* [License](#license)

Quick Start
-----------

### Gradle

```groovy
repositories {
    mavenCentral()
}

dependencies {
    annotationProcessor "com.bettercloud:bigtable-orm-compiler:$bigtableOrmVersion"
    compileOnly "com.bettercloud:bigtable-orm-annotations:$bigtableOrmVersion"
    api "com.bettercloud:bigtable-orm-core:$bigtableOrmVersion"
}
```

### Entity Configuration

```java
@Table("table_name")
class TableConfiguration {
    
    @Entity(keyDelimiter = "|", keyComponents = {
            @KeyComponent(constant = "my_entity"),
            @KeyComponent(name = "id")
    })
    private class MyEntity {
        
        @Column(family = "column_family", qualifier = "column_qualifier")
        private Boolean myBoolean;
        
        @Column(family = "another_column_family")
        private String hello;
    }
}
```

### Establishing a BigTable Connection

```java
final String gcpProjectId = ...
final String bigTableInstanceId = ...

final DaoFactory daoFactory = new DaoFactory(gcpProjectId, bigTableInstanceId);
final AsyncDaoFactory asyncDaoFactory = new AsyncDaoFactory(gcpProjectId, bigTableInstanceId);
```

or

```java
final Connection connection = ... // Customized BigTable or HBase connection
final AsyncConnection asyncConnection = ... // Customized Async BigTable or HBase connection

final DaoFactory daoFactory = new DaoFactory(connection);
final AsyncDaoFactory asyncDaoFactory = new AsyncDaoFactory(asyncConnection);
```

### Retrieving an Entity DAO

```java
final Dao<MyEntity> myEntityDao = daoFactory.daoFor(MyEntity.class);
```

### DAO Usage

The `MyEntity` class is not available until the first `./gradlew build`, or by configuring annotation processing in your IDE.

Because of this, it is recommended to compile your Entity definitions into their own artifact, and declare a dependency on the resulting artifact from your service code.

```java
final Key<MyEntity> key = MyEntity.keyBuilder()
        .id("a_string_id")
        .build();

assertEquals("my_entity|a_string_id", key.toString());

// Save a new entity
final MyEntity initialEntity = new MyEntity();
initialEntity.setMyBoolean(true);

myEntityDao.saveAll(Collections.singletonMap(key, initialEntity));

// Retrieve it and verify values
final Map<Key<MyEntity>, MyEntity> retrievedEntities1 = myEntityDao.getAll(Collections.singleton(key));
assertTrue(retrievedEntities1.containsKey(key));

final MyEntity retrievedEntity1 = retrievedEntities1.get(key);

assertTrue(retrievedEntity1.getMyBoolean());
assertNull(retrievedEntity1.getHello());

// Make an update and save it
retrievedEntity1.setHello("world");

myEntityDao.save(Collections.singletonMap(key, retrievedEntity1));

// Retrieve it again and verify the update
final Map<Key<MyEntity>, MyEntity> retrievedEntities2 = myEntityDao.getAll(Collections.singleton(key));
assertTrue(retrievedEntities2.containsKey(key));

final MyEntity retrievedEntity2 = retrievedEntities2.get(key);

assertTrue(retrievedEntity2.getMyBoolean());
assertEquals("world", retrievedEntity2.getHello());

// Delete it
myEntityDao.deleteAll(Collections.singleton(key));

// Retrieve it again and verify that it no longer exists
final Map<Key<MyEntity>, MyEntity> retrievedEntities3 = myEntityDao.getAll(Collections.singleton(key));
assertFalse(retrievedEntities3.containsKey(key));
```

Detailed Usage
--------------

### Table Declarations

The `@Table` annotation requires a `String` value representing the name of the table within BigTable.

Any classes annotated with `@Table` _must not_ be `public`, to prevent leaking into consuming code.

Tables will not be automatically created for you, and it is up to the developer to ensure that the table actually exists, or else runtime exceptions may occur.

### Entity Declarations

Any classes annotated with `@Entity` _must_ be an inner class within a class annotated with `@Table`.

Any classes annotated with `@Entity` _must_ be `private`, to prevent leaking into consuming code. Generated Entities will always be `public`.

The `@Entity` annotation contains an optional `keyDelimiter` (defaulting to `::`), as well as a required array of `@KeyComponent` annotations.

Generated Entities will be placed in the same package as the `@Table` within which it was declared.

#### Key Components

The array of `@KeyComponent` annotations is used to generate a `KeyBuilder` for the corresponding Entity.

Each `@KeyComponent` must declare either a `constant` value, or a `name` value with an optional `type` (defaulting to `String.class`).

If the `constant` value is defined, then the generated `KeyBuilder` will always place the defined constant in the position relative to the other components.

If the `constant` value is not defined, then the generated `KeyBuilder` will build a "step" named after the defined `name` using the defined `type`. The `Object.toString()` method is used to convert any non-String types into a String during key generation.

Most key `type` declarations will be either a `String` or a `UUID`, though any Object with a valid `toString()` method may be used.

##### Example

Take this `@Entity` declaration, for example:

```java
@Entity(keyDelimiter = "|", keyComponents = {
        @KeyComponent(name = "global", type = UUID.class),
        @KeyComponent(name = "regional", type = UUID.class),
        @KeyComponent(constant = "MY_CONSTANT"),
        @KeyComponent(name = "local")
})
private class TestEntityPleaseIgnore {
    ...
}
```

The resulting `KeyBuilder` can generate a `Key` using the following syntax:

```java
final Key<TestEntityPleaseIgnore> myKey = TestEntityPleaseIgnore.keyBuilder()
        .global(UUID.fromString("00000000-0000-0000-0000-000000000000"))
        .regional(UUID.fromString("11111111-1111-1111-1111-111111111111"))
        .local("some random string")
        .build();
```

The String representation of the `Key` can then be accessed:

```java
final String myKeyString = myKey.toString();

assertEquals("00000000-0000-0000-0000-000000000000|11111111-1111-1111-1111-111111111111|MY_CONSTANT|some random string", myKeyString);
```

The String representation is encoded using UTF-8 into a byte array before being used by BigTable, so any UTF-8 character (including spaces) is supported.

It is the responsibility of the developer to ensure keys for multiple Entities within the same Table do not collide, or else data loss may occur.

### Column Declarations

Fields annotated with `@Column` will only be recognized if declared within a class annotated with `@Entity`.

The `@Column` annotation contains a required `family` and an optional `qualifier`. These values correspond to the column family and qualifier within BigTable. If `qualifier` is not defined, then the name of the field will be used as the column qualifier within BigTable.

The type and name of the field will be automatically detected, and corresponding getters and setters will be generated.

The field on the generated Entity will always be `private`, and its getter and setter will be `public`. Any other modifiers and default values are discarded.

#### Primitive Types

Because a column can contain no data, and is therefore inherently nullable, all primitive types will be boxed during processing.

For example, if your @Entity declares a primitive column:

```java
@Column(family = "f")
private long timestamp;
```

Then the processor will generate the boxed equivalent instead:

```java
private Long timestamp;

public Long getTimestamp() {
    return timestamp;
}

public void setTimestamp(final Long timestamp) {
    this.timestamp = timestamp;
}
```

This means that constructing your Entity for the first time will result in all fields being null, not their primitive default values.

#### Custom Types

The serialization step uses [Jackson](https://github.com/FasterXML/jackson) under the hood, and a `TypeReference` for each field is stored and used during the deserialization process.

This means that any POJO supported by the `ObjectMapper` is supported by this library. As always, thoroughly test your DAO interactions to ensure intended functionality works as expected.

It is recommended to put any non-table classes inside of a `models` package within the same package as your Table declaration.

It is up to the developer to implement any `hashCode()`, `equals(Object o)`, and `toString()` methods on non-Entity classes.

It is supported (though optional) to omit setter methods for fields that you wish to keep effectively immutable on your custom types.

#### Column Sharing

Many different Entities within a single Table may (and _should_) refer to the same column family/qualifier, even if the declared type is completely different. It is the responsibility of this library to enforce pseudo-schemas on otherwise arbitrary values.

##### Example

```java
@Table("Monsters")
class MonstersTableConfiguration {
    
    @Entity(keyComponents = {
            @KeyComponent(constant = "pokemon"),
            @KeyComponent(name = "id", type = Integer.class)
    })
    private class Pokemon {
        
        @Column(family = "combat", qualifier = "data")
        private List<Attack> attacks;
    }
    
    @Entity(keyComponents = {
            @KeyComponent(constant = "digimon"),
            @KeyComponent(name = "reference", type = UUID.class)
    })
    private class Digimon {
        
        @Column(family = "combat", qualifier = "data")
        private Digivolution digivolution;
    }
}
```

Note that both Entities store values within the "combat" column family and the "data" column qualifier, even though the type of data they store is completely different.

From the consuming code's perspective, there is nothing tying these values to any specific column family/qualifier; it just works!™

```java
final Key<Pokemon> bulbasaurKey = Pokemon.keyBuilder().id(1).build();
final Map<Key<Pokemon>, Pokemon> results = pokemonDao.getAll(Collections.singleton(bulbasaurKey));
final Pokemon bulbasaur = results.get(bulbasaurKey);
bulbasaur.getAttacks().add(new Attack("Razor Leaf"));
pokemonDao.saveAll(Collections.singletonMap(bulbasaurKey, bulbasaur));
```

#### Column Versioning

The `@Column` annotation supports defining an optional `versioned` boolean which, when true, generates additional getters and setters for the annotated column, providing control over that column's "version" (typically a timestamp).

##### Example

For instance, let's assume we have a table containing people, keyed by their social security numbers, of whom we want to track heights (in inches) over time:

```java
@Table("People")
class PeopleTableConfiguration {
    
    @Entity(keyComponents = {
            @KeyComponent(name = "ssn")
    })
    private class Person {
        
        @Column(family = "measurements", versioned = true)
        private int heightInches;
    }
}
```

To set the height for a person at a given timestamp, you can invoke the setter for the person's height, and provide the additional timestamp parameter:

```java
final Key<Person> jeffKey = Person.keyBuilder().ssn("000-00-0000").build();

final long timestamp = Instant.now().toEpochMilli();

final Person jeff = new Person();
jeff.setHeightInches(72, timestamp);

final Map<Key<Person>, Person> persisted = personDao.saveAll(Collections.singletonMap(jeffKey, jeff));
final Person persistedJeff = persisted.get(jeffKey);

assertEquals(72, (int) persistedJeff.getHeightInches());
assertEquals(timestamp, (long) persistedJeff.getHeightInchesTimestamp());
```

Additionally, you may omit the timestamp parameter, and a timestamp will be set and reflected by the entity returned by `save`.

Invoking the setter for a column without defining the timestamp parameter will set the timestamp to `null` on the Java Object. A `null` timestamp is interpreted as a signal to auto-generate the column's timestamp on `save`:

```java
final Person jeff = new Person();
jeff.setHeight(75);

assertNull(jeff.getHeightInchesTimestamp());

final long minimumExpectedTimestamp = Instant.now().toEpochMilli();

final Map<Key<Person>, Person> persisted = personDao.saveAll(Collections.singletonMap(jeffKey, jeff));
final Person persistedJeff = persisted.get(jeffKey);

assertEquals(75, (int) persistedJeff.getHeightInches());
assertTrue(persistedJeff.getHeightInchesTimestamp() >= minimumExpectedTimestamp);
```

It is important to note that only the column values are considered when checking for equality, not their associated timestamps. Therefore:

```java
assertEquals(jeff, persistedJeff); // True, even though we did not define the timestamp ourselves
```

### Unit Testing

You should use IoC/DI frameworks, and inject the `Dao<T extends Entity>` interface, typed to your entity, whenever possible.

When testing your code, the `Dao<T extends Entity>` interface can be effectively mocked to return expected values.

The `Key<T extends Entity>` interface can be mocked or constructed using the appropriate `KeyBuilder`, and used with mocked `Dao` instances.

All generated Entity models can be used directly from unit tests, but are left non-`final` to support mocking when desired. Generated models support `hashCode()`, `equals(Object o)`, and `toString()`, which means they will work just fine with assertions and hash-based algorithms (such as `HashMap`).

Contributing
------------

Contributions and feedback are welcome and encouraged!

For minor bug-fixes, simply submit a pull request with your changes and a short description of the problem being solved. For major/breaking changes, please open an issue for discussion prior to submitting code changes.

All changes are expected to be tested thoroughly prior to submission. Any untested code will simply be rejected.

History
-------

* **1.5.0**: Add asynchronous read/write/scan/delete operations via AsyncDao and AsyncDaoFactory.

* **1.4.0**: Add table scan capability, with starting key to start scan from and ending/last key to be scanned. Requires full key.

* **1.3.0**: Improve the extensibility of the library by allowing DAO creation from an `EntityConfiguration` to support dynamically generated entities.

* **1.2.1**: Implement `equals` and `hashCode` methods on `StringKey`.

* **1.2.0**: Add batch read/write/delete functionality. Deprecate single-row operations to encourage use of batch operations.

* **1.1.1**: Read non-existent cells as `null` values (and `null` timestamps, if `versioned = true`).

* **1.1.0**: Add optional column versioning capabilities. Timestamps currently only reflect the **latest** column value.

* **1.0.0**: Initial public release. Support for single-row read/write/delete operations, as well as automatic entity (de)serialization.

License
-------

The MIT License (MIT)

Copyright (c) 2019 BetterCloud

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.