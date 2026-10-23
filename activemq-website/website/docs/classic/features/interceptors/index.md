---
title: "Interceptors"
slug: /interceptors
---

ActiveMQ has a sophisticated _interceptor stack_ so that you can attach whatever functionality you require into the broker in an easy way without complicating all of the other broker code. This has really helped us keep the code clean and modular while offering powerful extension points.

For an example of the kinds of things you can do with interceptors see the following pages

*   [Logging Interceptor](logging-interceptor)
*   [Security](../security)
*   [Visualisation](visualisation)
*   [TimeStamp on the Broker](timestampplugin)
*   [Get Statistics via Messages](statisticsplugin)
*   [Destinations Plugin](destinations-plugin)

### How plugins work

A plugin is an instance of the interface [BrokerPlugin](http://activemq.apache.org/components/classic/documentation/maven/apidocs/org/apache/activemq/broker/BrokerPlugin.html) which allows a plugin to add itself into the broker interceptor chain, typically using the [BrokerFilter](http://activemq.apache.org/components/classic/documentation/maven/apidocs/org/apache/activemq/broker/BrokerFilter.html) as a base class to allow only certain operations to be customized.

The object that implements the BrokerPlugin interface is called out as a plugin in the message broker's XML configuration file (see example below). Your plugin can then optionally reference other beans that are defined in the XML file.
```
<beans xmlns="http://www.springframework.org/schema/beans" xmlns:amq="http://activemq.org/config/1.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.springframework.org/schema/beans 
http://www.springframework.org/schema/beans/spring-beans-2.0.xsd http://activemq.org/config/1.0 
http://activemq.apache.org/schema/activemq-core.xsd http://activemq.apache.org/camel/schema/spring  http://activemq.apache.org/camel/schema/spring/camel-spring.xsd">

<!-- Allows us to use system properties as variables in this configuration file -->
<bean  class="org.springframework.beans.factory.config.PropertyPlaceholderConfigurer" />

  <broker xmlns="http://activemq.org/config/1.0" brokerName="localhost" dataDirectory="${activemq.base}/data" plugins="#myPlugin">

    <!-- The transport connectors ActiveMQ will listen to -->
    <transportConnectors>
      <transportConnector name="openwire" uri="tcp://localhost:61616" />
    </transportConnectors>

  </broker>

  <bean id="myPlugin" class="org.myorg.MyPlugin">
    <!-- You can reference one or more Spring beans in this file -->
    <property name="myMgr" ref="myManager"/>		 
  </bean>

  <bean id="myManager" class="org.myorg.MyManager">
    <property name="fooList">
      <list>
        <value>foo</value>
        <value>foo2</value>
      </list>
    </property>
  </bean>

</beans>
```
You can also define plugins from within the <plugin> element as this example illustrates.
```
<beans xmlns="http://www.springframework.org/schema/beans" xmlns:amq="http://activemq.org/config/1.0"
xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.springframework.org/schema/beans
http://www.springframework.org/schema/beans/spring-beans-2.0.xsd http://activemq.org/config/1.0 
http://activemq.apache.org/schema/activemq-core.xsd http://activemq.apache.org/camel/schema/spring
http://activemq.apache.org/camel/schema/spring/camel-spring.xsd">

 <!-- Allows us to use system properties as variables in this configuration file -->
 <bean  class="org.springframework.beans.factory.config.PropertyPlaceholderConfigurer" />

 <broker xmlns="http://activemq.org/config/1.0" brokerName="localhost" dataDirectory="${activemq.base}/data">

  <!-- The transport connectors ActiveMQ will listen to -->
  <transportConnectors>
     <transportConnector name="openwire" uri="tcp://localhost:61616" />
  </transportConnectors>

  <plugins>
      <bean xmlns="http://www.springframework.org/schema/beans" id="myPlugin" class="org.myorg.MyPlugin"/>    
  </plugins>

 </broker>
 
</beans>
```
At startup, the main or core broker calls your plugin's installPlugin() method. This method creates and returns an object that typically extends [BrokerFilter](http://activemq.apache.org/components/classic/documentation/maven/apidocs/org/apache/activemq/broker/BrokerFilter.html).
```
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerPlugin;

public class MyPlugin implements BrokerPlugin {	
        
        public Broker installPlugin(Broker broker) throws Exception {            
             return new MyBroker(broker);
        }	

}
```
The BrokerFilter class is a convenience class that implements the [Broker](http://activemq.apache.org/components/classic/documentation/maven/apidocs/org/apache/activemq/broker/Broker.html) interface. This interface defines all the main operations (e.g., addConnection, addSession, etc.) that your implementation can intercept. The class that extends BrokerFilter overrides any of the methods that are defined in the Broker interface so that it can intercept the corresponding core engine's operations. Here's an example of a class that extends BrokerFilter and intercepts/overrides the addConnection() and addSession() Broker methods/operations.
```
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ConnectionInfo;

public class MyBroker extends BrokerFilter {
    
     public MyBroker(Broker next) {
        super(next);                
    }

    public void addConnection(ConnectionContext context, ConnectionInfo info) 
            throws Exception {       
        
         // Your code goes here 

        // Then call your parent
        super.addConnection(context, info);
    }   

    public void addSession(ConnectionContext context, SessionInfo info) 
            throws Exception {       
        
         //  Your code goes here...

         // Then call your parent
         super.addSession(context, info);
    }	
}
```
For more details see [Developing Plugins](developing-plugins)

