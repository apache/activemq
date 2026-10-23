---
title: "How To Unit Test JMS Code"
slug: /how-to-unit-test-jms-code
---

When unit testing code with JMS you'll typically want to avoid the overhead of running separate proceses; plus you'll want to increase startup time as fast as possible as you tend to run unit tests often and want immediate feedback. Also persistence can often cause problems - as previous test case results can adversely affect future test case runs - so you often need to purge queues on startup.

So when unit testing JMS code we recommend the following

*   Use [an embedded broker](how-do-i-embed-a-broker-inside-a-connection) to avoid a separate broker process being required.
*   Disable [broker persistence](broker-configuration-uri) so that no queue purging is required before/after tests.
*   It's often simpler and faster to just use Java code to create the broker via an XML configuration file using Spring etc.

You can do all of this using the following Java code to create your JMS `ConnectionFactory` which will also automatically create an embedded broker
```
ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
```
For more configuration options see the [VM Transport Reference](vm-transport-reference) and the [Broker Configuration URI](broker-configuration-uri)

Or if you really would rather be more explicit you can create the broker first using the following Java code
```
BrokerService broker = new BrokerService();
broker.setPersistent(false);
broker.start();
```
or you could use [Spring Support.](spring-support)

### Using JNDI

If your application code is using JNDI to lookup the JMS `ConnectionFactory` and `Destination`'s to use, then you could use the [JNDI Support](jndi-support) in ActiveMQ.

Add the following `jndi.properties` to your classpath, e.g., in `src/test/resources`, if you are using maven:
```
java.naming.factory.initial = org.apache.activemq.jndi.ActiveMQInitialContextFactory
java.naming.provider.url = vm://localhost?broker.persistent=false
```
You should then consider using [Dynamic destinations in JNDI](jndi-support) so that your code looks up destinations via
```
context.lookup("dynamicQueues/FOO.BAR");
```

### Using The `EmbeddedActiveMQBroker` JUnit Rule (ActiveMQ 5.13)

If your test code is using JUnit, then you could use the `EmbeddedActiveMQBroker` JUnit Rule provided in the `activemq-junit` library. Add the `activemq-junit` library along with the `activemq-broker` libraries for the version of ActiveMQ you want to test with.  The rule will use whatever version of ActiveMQ it finds in the classpath, so the ActiveMQ libraries need to be specified if they are not already there.

If you are using Maven, add the following to your `pom.xml`:
```
<dependency>
    <groupId>org.apache.activemq.tooling</groupId>
    <artifactId>activemq-junit</artifactId>
    <version>${activemq-junit-version}</version>
    <scope>test</scope>
</dependency> 

<dependency>
    <groupId>org.apache.activemq</groupId>
    <artifactId>activemq-broker</artifactId>
    <version>${activemq-version}</version>
    <scope>test</scope>
</dependency>
```
 Then add the `EmbeddedActiveMQBroker` JUnit Rule to your test, and JUnit will start the embedded broker at the beginning of each test and stop the broker at the end of the test.

**Use The ActiveMQ JUnit Rule**
```
@Rule
public EmbeddedActiveMQBroker broker = new EmbeddedActiveMQBroker();
```
By default, the `EmbeddedActiveMQBroker` will configure the broker as non-persistent, and the only transport available will be the VM transport. To customize this configuration, either extend the `EmbeddedActiveMQBroker` class and override the `configure()` method, or use an XML configuration for the broker.  

**Note**: to configure an `EmbeddedActiveMQBroker` using XML configuration, you may need to add additional libraries to the classpath to support XBean configuration of ActiveMQ.

**Customizing An EmbeddedActiveMQBroker Using Java**
```
@Rule
EmbeddedActiveMQBroker customizedBroker = new EmbeddedActiveMQBroker() {
    @Override
    protected void configure() {
        // Perform additional configuration here...
    }
}
```  

**Customizing An EmbeddedActiveMQBroker Using XML Configuration**
```
@Rule
EmbeddedActiveMQBroker customizedBroker = new EmbeddedActiveMQBroker("bean:customize-activemq.xml");
```
Note that to use the XML configuration, you may need to add additional libraries on the classpath to support the XBean configuration of ActiveMQ.  The versions of the `spring-context` library should correspond with the version used by your selected version of ActiveMQ.

**Maven Configuration For XBean Configuration**
```
<dependency>
    <groupId>org.springframework</groupId>
    <artifactId>spring-context</artifactId>
    <version>Appropriate version for activemq-version</version>
</dependency>

<dependency>
    <groupId>org.apache.activemq</groupId>
    <artifactId>activemq-spring</artifactId>
    <version>${activemq-version></version>
</dependency>
```
Then you can use the VM URI to connect with the broker
```
ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://embedded-broker?create=false");
```
You can also get a connection factory from the `EmbeddedActiveMQBroker`:
```
ConnectionFactory connectionFactory = embeddedBroker.createConnectionFactory();
```
