# Change Log
All notable changes to this project will be documented in this file, which follows the guidelines
on [Keep a CHANGELOG](http://keepachangelog.com/). This project adheres to
[Semantic Versioning](http://semver.org/).

## [Unreleased]
### Changed
- Updated the parent `maven-framework-parent-pom` to 25.104.1 to take the changes from it
- Updated `cp-file-service`, `framework-libraries`, `microservice-framework` and `event-store` to 25.104.1

## [25.104.0] - 2026-09-07
First official (non-milestone) release of the Java 25 / WildFly 40 / Jakarta EE 11 line,
consolidating milestone `25.104.0-M1`. As the framework's reference implementation, this
is the first build to run end to end against the fully released `25.104.0` chain.

### Changed
- Upgraded to Java 25 / WildFly 40 / Jakarta EE 11 (25.104.x release line)
- Consumed the released framework chain at `25.104.0` throughout: parent `maven-framework-parent-pom`, `framework.version` (microservice-framework), `event-store.version`, `framework-libraries.version` and `file-service.version`. Brings Java 25 / Jakarta EE 11 targeting (`java.major.version=25`, `enforcer.java.version.range=[25,)`), the Jakarta EE 11 API set, Weld 6, RESTEasy 7, Hibernate ORM 6, Apache Artemis `2.54.0` under the new `org.apache.artemis` groupId, `liquibase.version=5.0.3`, Jackson `2.21.5` (**CVE-2026-54515**), the `org.junit:junit-bom` import, the relocated `persistence-jpa` module (event-stream self-healing `EntityManagerFlushInterceptor` + `EntityManagerProducer`), and the event-store `EntityManagerFlushInterceptorPresenceVerifier` deploy-guard
- `cakeshop-viewstore-persistence` now depends on `persistence-jpa`, delivering the flush interceptor and `EntityManager` producer into the service WARs
- WildFly upgraded to `40.0.0.Final`; the Docker image uses a multi-stage build on `eclipse-temurin:25-jdk-noble`
- Azure pipeline agent: `ubuntu-j21` → `ubuntu-j25-postgres`

### Removed
- Dead `wildfly.maven.plugin.version` (`4.2.2.Final`) property — nothing referenced `${wildfly.maven.plugin.version}`, and the WildFly plugin version comes from `maven-framework-parent-pom` as `plugins.maven.wildfly.version` (`6.0.0.Final`). The stale value was a leftover from the WildFly 32 era
- Local `jakarta.xml.bind-api.raml.version` property — the value (`2.3.2`) now comes from `maven-framework-parent-pom`, which centralised it so child projects could drop their copies. The explanatory comment stays, since the generator plugin configs below refer to it

### Fixed
- `StreamErrorHandlingIT` restored to assert the event-processing constraint violation is caught in-chain by `uk.gov.justice.services.persistence.EntityManagerFlushInterceptor` (Hibernate 6 surfaces `org.hibernate.exception.ConstraintViolationException` directly rather than wrapping it) — re-establishing coverage of the event-stream self-healing error capture that regressed when the flush interceptor was lost

## [21.0.0-M1] - 2026-06-02
### Changed
- Upgraded to Java 21 and Jakarta EE 10
- Updated WildFly from `26.1.2.Final` to `32.0.1.Final` and WildFly Maven plugin from `4.1.1.Final` to `4.2.2.Final`
- Removed `WEB-INF/web.xml` — JAX-RS Application subclasses are now discovered purely via `@ApplicationPath` annotations; WildFly's JAX-RS subsystem registers servlet mappings automatically
- Replaced `javax.persistence:javax.persistence-api:2.2` with `jakarta.persistence:jakarta.persistence-api:3.1.0`
- Replaced `javax.annotation:javax.annotation-api:1.3.2` with `jakarta.annotation:jakarta.annotation-api:2.1.1`
- Migrated `javax.persistence.metamodel.*` to `jakarta.persistence.metamodel.*` in `Recipe_.java` static metamodel
- Updated to framework `21.0.0-SNAPSHOT`; IT tests now run using the pull mechanism; pull mechanism now has retries for failed events
- Reset database state before each test through junit5 extension
### Fixed
- Replaced all occurrences of non-existent `javax:javaee-api:7.0` plugin dependency with `jakarta.platform:jakarta.jakartaee-api:10.0.0` across root `pom.xml`, `cakeshop-command-api`, and `cakeshop-event` POMs
- Fixed `ServiceConfigurationError: Provider javax.json.spi.JsonProvider not found` in all code generator plugins by excluding `org.glassfish:javax.json` and adding Jakarta JSON-P runtime stack (`parsson:1.1.0`, `org.glassfish:jakarta.json:2.0.1`, `jackson-datatype-jakarta-jsonp:2.14.2`) to each plugin classpath
- Replaced `javax.xml.bind:jaxb-api:2.3.1` with `jakarta.xml.bind:jakarta.xml.bind-api:4.0.0` in `raml-maven-plugin` classpath
- Disabled `maven-processor-plugin` annotation processing in `cakeshop-viewstore-persistence` (`hibernate-jpamodelgen:4.3.11.Final` does not support `@jakarta.persistence.Entity`); added manually maintained `Recipe_.java` static metamodel

## [17.104.0] - 2025-12-16
### Changed
- Integration tests now run against a standalone wildfly docker instance rather than using the maven-wildfly-plugin
- Integration tests no longer run as part of the default build.
- `runIntegrationTests.sh` script now deploys cake-shop into wildfly and runs the integration tests
- Deltaspike database tests now use an in memory database rather than using postgres
- Random ports for integration tests removed and now use the default wildfly ports
- Addition of secret scanning and dependabot to the project
- Update event-store for event-buffer refactor:
  - Run each event sent to the event listeners in its own transaction
  - Update the `stream_status` table with `latest_known_position`
  - Mark stream as 'up_to_date' when all events from event-buffer successfully processed
  - New column `latest_known_position` in `stream_status table`
  - New column `is_up_to_date` in `stream_status table`
  - New liquibase scripts to update stream_status table
  - New SubscriptionManager class `NewSubscriptionManager`to handle the new way of processing events
  - New replacement StreamStatusRepository class for data access of stream_status table
  - Change name of jndi value for self-healing from `event.error.handling.enabled` to `event.stream.self.healing.enabled`
- Jmx MBean `SystemCommanderMBean` now only takes basic Java Objects to keep the JMX handling interoperable

### Added
- Implemented error handling for events
- New tables in viewstore `stream_error` and `stream_error_hash` for storing errors on a stream 
- New column `buffered_at` on the stream_buffer tables to allow for monitoring of stuck stream_buffer events
- New integration test for event error handling
- The columns `stream_id`, `component_name` and `source` on the `stream_error` table are now unique when combined
- Inserts into `stream_error` now `DO NOTHING` if a row with the same `stream_id`, `component_name` and `source` on the `stream`error` already exists
- Inserts into `stream_error` are therefore idempotent
- No longer removing stream_errors before inserting a new error, as the insert is now idempotent
### Removed
- Removed `JmxCommandParameters` and `CommandRunMode` from JMX SystemCommanderMBean call
- Removed wildfly maven plugin and wildfly download, as integration tests now run separately using `runIntegrationTests.sh`
- Removed run of integration tests from default maven test phase; `mvn test` now solely runs unit tests 

## [17.100.1] - 2024-11-12
### Added
- Add jobstore usecase
- Add ITs for validating REPLAY_EVENT_TO_EVENT_LISTENER and REPLAY_EVENT_TO_EVENT_INDEXER command processing
- New Jndi value `java:global/catchup.event.source.whitelist` for a comma separated list of whitelisted event-sources for catchup.
- New parameter 'JmxCommandRuntimeParameters' to JMX commands
- New Jmx command `RebuildSnapshotCommand` and handler that can force hydration and generation of an Aggregate snapshot

### Changed
- All JmxCommandHandlers must now have `commandName` String, `commandId` UUID and JmxCommandRuntimeParameters in their method signatures
- Improve the fetching of jobs by priority from the jobstore by retrying with a different priority if the first select returns no jobs
- Update jobstore to process tasks with higher priority first
- Fix for Jackson single argument constructor issue inspired from  https://github.com/FasterXML/jackson-databind/issues/1498
- Update jobstore to process tasks with higher priority first
- Refactor of File Store to merge file store 'metadata' table into the 'content' table.
- File Store now only contains one table
- The catchup process can now whitelist event sources to catchup
- New Jndi value can be set to `ALLOW_ALL` to allow all
**- Split filestore `content` tables back into two tables of `metadata` and `content` to allow for backwards compatibility with liquibase**
### Fixed
- JdbcResultSetStreamer now correctly streams data using statement.setFetchSize(). The Default fetch size is 200. This can be overridden with JNDI prop jdbc.statement.fetchSize


## [17.0.1] - 2023-12-13
### Changed
- Updated to Junit 5
- Centralise all generic library dependencies and versions into maven-common-bom
- Update to Junit5 and surefire, failsafe plugin versions
- Add retry mechanism to jobstore via framework-libraries
### Fixed
- Fix Logging of missing event ranges to only log on debug
- Limit logging of MissingEventRanges logged to sensible maximum number.
### Added
- New JNDI value `catchup.max.number.of.missing.event.ranges.to.log`
- Add '-f' '--force' switch to the JmxCommandClient to bypass COMMAND_IN_PROGRESS check
### Removed
- Removed dependency on apache-drools as it's not used by any of the framework code
### Security
- Update common-bom to fix various security vulnerabilities in org.json, plexus-codehaus, apache-tika and google-guava


## [17.0.0] - 2023-02-07
### Changed
- Updated to Java 17
- Update common-bom to 17.0.0-M3 in order to:
  - Add byte-buddy 1.12.22 as a replacement for cglib
  - Downgrade h2 to 1.4.196 as 2.x.x is too strict for our tests
- Update framework-libraries to 17.0.0-M4 in order to:
  - Change 'additionalProperties' Map in generated pojos to HashMap to allow serialization
- Update framework-libraries to 17.0.0-M6 in order to:
  - Remove illegal-access argument from surefire plugin
  - Make pojo generator to perform null safe assignment of additionalProperties inside constructor

### Changed
- Update framework-libraries to 11.0.0 for:
    - A default name of `jms.queue.DLQ` rather than the original name of `DLQ`
    - A new constructor to pass the name in if you don't want the default name
    - New builder `MessageConsumerClientBuilder` that allows ActiveMQ connection parameters to be specified
- Updated to java 11 and OpenJdk
- Removed trigger from the event publishing process
- Updated wildfly to 20.0.1-Final  
- Reduced the maximum runtime for each iteration of the publishing beans in the IT tests to 450 milliseconds
- Update to maven-framework-parent-pom 11.0.0
- Update to framework 11.0.0
- Update to event-store 11.0.0
- Bumped the base version of the project to 11.0.0 to match the framework libraries and show java 11 change  
- Handled the move to the new Cloudsmith.io maven repository
- Updated slf4j/log4j bridge jar from slf4j-log4j12 to slf4j-reload4j
- Added Artemis healthcheck
- Downgraded maven minimum version to 3.3.9 until the pipeline maven version is updated
- Add cover all token to travis settings
- Update common bom in order to:
  - Update jboss-logging version to 3.5.0.Final
  - Update jackson libraries to 2.12.7
  - Update mockito version to 4.11.0
  - Update slf4j version to 2.0.6
  - Update hamcrest version to 2.2
   -Update slf4j version to 2.0.6

### Added
- Added support for feature toggling with an integration test showing it working
- Added healthcheck integration test

### Security
- Updates to various libraries to address security alerts:
  - wildfly to version 26.1.2.Final
  - artemis to version 2.20.0
  - resteasy-client to version 4.7.7.Final
  - Update hibernate version to 5.4.24.Final
  - Update jackson.databind version to 2.12.7.1

## [2.0.0] - 2019-08-19
### Added
- Update to framework 6.0.6
- Unified Search indexer module
- Integration test for event catchup
- Integration test for PublishedEvent rebuild.
- Update to event-store 2.0.6
- Update to framework-generators 2.0.4
- Update framework-api to 4.0.1
- Update file.service to 1.17.11
- Update common-bom to 2.4.1
- Update utilities to 1.20.2
- Update test-utils to 1.24.3
- Update json-schema-catalog to 1.7.4

### Changed
- Use a single event-source.yaml in cakeshop-event-source module

## [2.0.0-M3] - 2019-05-09

### Changed
- common-bom -> 2.0.2
- framework -> 6.0.0-M22
- event-store -> 2.0.0-M22
- framework-generators -> 2.0.0-M15
- file.service -> 1.17.7
- framework-api -> 4.0.0-M18
- generator-maven-plugin -> 2.7.0
- json-schema-catalog -> 1.7.0
- raml-maven-plugin -> 1.6.7
- test-utils -> 1.23.0
- utilities -> 1.18.0


### Added
- Integration Test for Event Catchup
### Changed
- Update Shuttering Integration Test
- Remove deprecated github_token entry from travis.yml


## [2.0.0-M2] - 2019-04-08

### Added
- Add Shuttering Integration Test
### Changed
- Update framework-api to 4.0.0-M5
- Update framework to 6.0.0-M10
- Update event-store to 2.0.0-M10
- Update framework-generators to 2.0.0-M8

## [2.0.0-M1] - 2019-03-25

### Changed
- Update framework-api to 4.0.0-M2
- Update framework to 6.0.0-M5
- Update event-store to 2.0.0-M7
- Update framework-generators to 2.0.0-M6
- Update plugins to use a single plugin declaration for each plugin rather that one large plugin with multiple configurations
- Removed framework-domain

## [1.1.0] - 2019-01-09

### Changed
- Update framework-api to 3.1.0
- Update framework to 5.1.0
- Update framework-domain to 1.1.0
- Update event-store to 1.1.0
- Update framework-generators to 1.1.0
- Update utilities to 1.16.2
- Update test-utils to 1.19.1
- Update file-service to 1.17.2
- Update json-schema-catalog to 1.4.3

### Added
- Liquibase script to add events into event_log before startup
- CakeShopReplayEvents IT to test the replaying of events on startup
- SubscriptionEventInterceptor into Event Listener to update Subscription event number

## [1.0.1] - 2018-12-11

### Changed
- Update framework-api to 3.0.1
- Update framework to 5.0.4
- Update framework-domain to 1.0.3
- Update event-store to 1.0.4
- Update framework-generators to 1.0.2
- Use new Enveloper in service components

## [1.0.0] - 2018-11-09

### Added
- Extracted project from cakeshop app in Microservices Framework 5.0.0-M1: https://github.com/CJSCommonPlatform/microservice_framework


