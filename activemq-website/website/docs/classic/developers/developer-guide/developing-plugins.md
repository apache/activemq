---
title: "Developing Plugins"
slug: /developing-plugins
---


Apache ActiveMQ is based on the model of POJOs and _Dependency Injection_. If you are developing [Interceptors](interceptors) or additional components or plugins for ActiveMQ then the first thing you should do is develop the code as if you are writing any other Spring component, using dependency injection.

### Dependency Injection

Some folks favour using constructor based injection as it removes the need to have a separate lifecycle _start()_ method - others find using property based injection is a little more flexible and easier to map to XML configuration files. We'll leave that choice up to you. For complex to create objects you could consider writing a FactoryBean. For more details on writing POJOs with Spring see their [documentation](http://www.springframework.org/documentation).

### Custom XML

With ActiveMQ you can use regular Spring.xml syntax to configure things. However to produce neater XML that is easier to read and edit we use [XBean](http://geronimo.apache.org/xbean/) to autogenerate support for [Custom XML](http://geronimo.apache.org/xbean/custom-xml.html).

If you wish your POJO to have its own custom XML you may wish to follow the following source examples for working nicely with XBean. Basically you add an XBean annotation in the javadoc comments to tell XBean how to map the POJO to custom XML. This should look something like
```
/**
 * @org.apache.xbean.XBean element="foo"
 */
public class MyExtension {
...
}
```
You can omit the element configuration. For more details on the available annotation options see [here](http://geronimo.apache.org/xbean/xbean-ant-task.html)

If you are submitting your plugin to the ActiveMQ project then it will end up being included in the maven build step to create the XBean artifacts as part of the jar (in the META-INF/services area).

However if you are writing an external plugin to ActiveMQ then you will need to add the maven-xbean-plugin to your Maven 2 build. Refer to the [activemq-spring/pom.xml](https://github.com/apache/activemq/tree/main/activemq-spring/pom.xml) as an example of using this plugin.

### Configuring plugins without custom XML

If you want to configure plugins that does not implement custom XML, you can define plugins as "regular" Spring beans and reference them in broker's `plugins` attribute. For example,
```
<broker useJmx="true" xmlns="http://activemq.apache.org/schema/core" plugins="#loggingPlugin">
 ...
</broker>

<bean id="loggingPlugin" 
      class="org.apache.activemq.broker.util.LoggingBrokerPlugin"
/>
```
Not that this mechanism will not work in case that you have some XBean plugins configured inside the `<plugins/>` tag. In that case you must define the plugin inside that tag as well (with the appropriate schema definition). For example,
```
<plugins>
    <simpleAuthenticationPlugin>
      <users>
        <authenticationUser username="system" password="manager"
          groups="users,admins"/>
        <authenticationUser username="user" password="password"
          groups="users"/>
        <authenticationUser username="guest" password="password" groups="guests"/>
      </users>
    </simpleAuthenticationPlugin>  
    <bean xmlns="http://www.springframework.org/schema/beans" 
          id="loggingPlugin" 
          class="org.apache.activemq.broker.util.LoggingBrokerPlugin"
    />
</plugins>  
```
### Examples

The easiest way to get a feel for how to extend ActiveMQ is maybe to look at some concrete examples of features and how those are implemented and configured. Here are some examples

*   [XBeanBrokerService](https://github.com/apache/activemq/tree/main/activemq-spring/src/main/java/org/apache/activemq/xbean/XBeanBrokerService.java) deals with most of the core configuration of the <broker> tag in the XML
*   [Security](security) has an [example](http://svn.apache.org/repos/asf/activemq/trunk/activemq-unit-tests/src/test/resources/org/apache/activemq/security/jaas-broker.xml) XML configuration file using the [AuthorizationPlugin](http://svn.apache.org/repos/asf/activemq/trunk/activemq-broker/src/main/java/org/apache/activemq/security/AuthorizationPlugin.java)
*   The [Discarding DLQ Plugin](message-redelivery-and-dlq-handling) is used to discard messages from the DLQ.

