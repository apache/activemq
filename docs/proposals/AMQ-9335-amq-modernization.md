## Broker modernization 

[DRAFT] This document is a draft proposal and may be broken up into smaller sub-proposals

### Problem statement

Most ActiveMQ broker deployments rely on the Spring Framework (and xbean) for boot, configuration and integration of plugin extension. There are a number of limitations and strategic issues with continuing with this design as the primary approach.

### Rationale

Modernization of the ActiveMQ broker will provide new features, improved multi-tenancy and ability to operate in Kubernetes and other cloud-based environments. Additionally, reducing the number of dependencies will reduce the surface area for security vulnerabilities caused by 3rd party projects.

### Goals

1. ActiveMQ 7.0 will ship without requiring Spring dependencies for boot, configuration and plugin extension

2. ActiveMQ 7.0 will support more format options for configuration -- ex. xml, json and/or yaml file segments

3. ActiveMQ 7.0 will support environment, property and encryption interpolation of configuration values

4. ActiveMQ 7.0 will include a Builder API for constructing a broker 

### Proposal

Organize the ActiveMQ modernization into phases with target release versions. Adhere to SEMVER and provide experimental/technical preview abilities to end users without having to require them to build from git. (ie the project may release an alternative dist or secondary boot scripts)

NOTE: Version numbers are provided for conceptual purposes. The actual version numbers may change as required for release planning.

### Phases

#### Phase 0 - Dependency and shared library clean-up

1. Replace Spring MVC with simple Java-based controller handler

2. Adopt next-gen openwire module

3. Refactor/Re-home/Replace common data handling and buffer management classes related to streams, buffers, etc. into a single module to remove redundancy 

4. On-going: unit test modernization -- migrate to JUnit5, new JUnit5 Extension, fix broken tests, migrate junit 3 and junit 4 tests to junit 5.

#### Phase 1 - Separation of stat, statistics and config fields in classes. Move to config data model classes

In order to improve configuration, ActiveMQ should move to a full data-model approach for configuration vs class fields (aka bean properties) 

Examples:

- PolicyEntry
- NetworkBridgeConfiguration

*Benefits*

Configuration changes will be 'all-at-once' instead of field-at-a-time. This is required for supporting thread-safe proper runtime configuration change where messages may be flowing and multiple configuration properties need to be modified and race conditions may lead to undesirable results.

*Requirements*

A centralized Configuration Service will be used to load config file segments, interpolate environment, system and encrypted properties and configure a BrokerService.

Tooling: Updated tooling to support validating configuration file segments and previewing environment and encrypted property expansion.

#### Phase 2 - activemq-boot service

ActiveMQ Boot Service will be a generic boot lifecycle service that will delegate details about Broker booting to specific boot service provider implementations.

For example, in order to provide legacy support for Spring-bean based activemq.xml, a SpringBootService implementation will be needed.

Additional, Boot Service providers may include SPI-based, simple boot (user customized builder expresssion), etc. 

*Requirements*

A Jetty-boot handler will be required for enabling web-console, API and other servlets.

#### Phase 3 - Repackaging

Organizing the activemq modules will allow for reducing dependency leakage to clients. For example, the current activemq-http provides both client and server-side implementations.

*Common*
activemq-boot: Light-weight boot service for starting brokers, or running cmoplex client-side scenarios (performance testing, etc)
activemq-io: Buffer, Stream and common data handling classes used by clients and servers

*Broker*
activemq-broker-model: Data model classes, and utility classes for managing data model classes.
activemq-broker-api: API, SPI and JMX MXBean interface definitions. Depends on activemq-broker-model, but provides no implementations
activemq-broker-service: BrokerService classes and default implementations
activemq-broker-amqp: AMQP-based server-side transport (may depend on activemq-client-amqp)
activemq-broker-http: HTTP-based server-side transport (may depend on activemq-client-jms)
activemq-broker-jms: JMS-based server-side transport (may depend on activemq-client-jms)
activemq-broker-mqtt: MQTT-based server-side transport (may depend on activemq-client-mqtt)

*Client*
activemq-client-amqp: AMQP-based client support
activemq-client-http: HTTP-based client support
activemq-client-jms: JMS-based client support

*Storage*
activemq-store-kahadb: Pluggable storage backend KahaDB
activemq-store-jdbc: Pluggable storage backend JDBC


