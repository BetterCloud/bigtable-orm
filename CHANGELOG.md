History
-------

* **4.0.0**:
  - Updated Gradle to 6.9 
  - Updated Java to 11
  - Updated dependencies

* **3.3.0**: Updated hadoop-common from 2.x to 3.x to fix Snyk issues

* **3.2.0**: Updated dependencies to fix Snyk issues

* **3.1.0**: Bumps hbase-client version to 2.4.13

* **3.0.0**: Bumps jacksonVersion and bigtableVersion to address security vulnerabilities, removes hbase-shaded-client and pulls in the standard hbase-client

* **2.0.0**: Bumps jacksonVersion and bigtableVersion to address security vulnerabilities

* **1.6.0**: Bumps jacksonVersion to address security vulnerabilities

* **1.5.0**: Add asynchronous read/write/scan/delete operations via AsyncDao and AsyncDaoFactory.

* **1.4.0**: Add table scan capability, with starting key to start scan from and ending/last key to be scanned. Requires full key.

* **1.3.0**: Improve the extensibility of the library by allowing DAO creation from an `EntityConfiguration` to support dynamically generated entities.

* **1.2.1**: Implement `equals` and `hashCode` methods on `StringKey`.

* **1.2.0**: Add batch read/write/delete functionality. Deprecate single-row operations to encourage use of batch operations.

* **1.1.1**: Read non-existent cells as `null` values (and `null` timestamps, if `versioned = true`).

* **1.1.0**: Add optional column versioning capabilities. Timestamps currently only reflect the **latest** column value.

* **1.0.0**: Initial public release. Support for single-row read/write/delete operations, as well as automatic entity (de)serialization.
