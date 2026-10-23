---
title: "Complex Single Broker Configuration (STOMP only)"
description: "Locked-down STOMP-only ActiveMQ broker config with predefined queues, simple authentication and authorization, a Jetty JAAS secured Web Console and JMX."
slug: /complex-single-broker-configuration-stomp-only
---


Example of an ActiveMQ configuration with predefined queues, simple destination security (could easily update it to JAAS), complex Web Console security with Jetty JAAS, and JMX security too.

While this is a fairly detailed configuration, it locks down every ActiveMQ service. It would be ideal if ActiveMQ shipped with a default closed configuration like this.

ActiveMQ is assumed to be installed in /usr/local/activemq/ in this example.
```
<!--
  ActiveMQ activemq.xml configuration file (/usr/local/activemq/conf/activemq.xml)

  * ActiveMQ JVM Startup options are in /etc/activemq.conf

  * Uses the Sun JMX connector for remote management.  Point jconsole at:
    service:jmx:rmi:///jndi/rmi://myserver.domain.net:61616/jmxrmi

  * Uses Kaha persistence storage, stored in the "activemq-data" directory.
    "activemq-data" and "logs" sub-directories must be writable by the
    ActiveMQ user.

  * Also see conf/log4j.properties for logging configuration
-->

<beans>
    <!-- Enables system properties as variables in this configuration file -->
    <bean class="org.springframework.beans.factory.config.PropertyPlaceholderConfigurer"/>

    <broker xmlns="http://activemq.org/config/1.0" brokerName="SERVER1"
        populateJMSXUserID="true" useJmx="true" persistent="true">

    <!-- Queue setup.  Queues can be created on the fly by any user with
         admin rights, but it is not good to give every user admin rights.  -->
    <destinations>
        <queue physicalName="widgets" />
        <queue physicalName="spacecontrol" />
        <queue physicalName="displays" />
    </destinations>

    <!-- We only allow Stomp clients -->
    <transportConnectors>
        <transportConnector name="stomp" uri="stomp://localhost:61613"/>
    </transportConnectors>

    <!-- We don't have any other brokers to connect to -->
    <networkConnectors>
    </networkConnectors>

    <!-- Do not create an ActiveMQ JMX connector.  Use the Sun JMX connector
         instead, and hook ActiveMQ to it. -->
    <managementContext>
        <managementContext createConnector="false" />
    </managementContext>

    <plugins>
        <simpleAuthenticationPlugin>
            <users>
                <authenticationUser username="sa" password="manager" groups="producers,consumers,admins" />
                <authenticationUser username="frontend" password="manager" groups="producers,consumers" />
                <authenticationUser username="backend" password="manager" groups="consumers" />
            </users>
        </simpleAuthenticationPlugin>
        <authorizationPlugin>
            <map>
                <authorizationMap>
                    <authorizationEntries>
                        <authorizationEntry queue=">" write="producers" read="consumers" admin="admins" />
                    </authorizationEntries>
                </authorizationMap>
            </map>
        </authorizationPlugin>
    </plugins>

    </broker>

    <!-- Do not create ActiveMQ.Agent topic, as it does not work if
         destination security is enabled -->
    <!-- <commandAgent xmlns="http://activemq.org/config/1.0"/> -->

    <!-- Web Console.  Auth is via JAAS.  Beware: jetty-plus-6.1.4.jar contains the
         JAAS classes, and is not included with ActiveMQ.  You need to download
         separately.  Web Console queue browser will fail, as it tries to use JMS
         to browse the queue, and that requires a password.
    -->

    <jetty xmlns="http://mortbay.com/schemas/jetty/1.0">
        <connectors>
            <nioConnector port="8161" />
        </connectors>

        <userRealms>
            <!-- "name" must match the realm in web.xml, and "loginModuleName" must be defined in login.conf -->
            <jaasUserRealm name="ActiveMQ" loginModuleName="ActiveMQ"
                    callbackHandlerClass="org.mortbay.jetty.plus.jaas.callback.DefaultCallbackHandler" />
        </userRealms>

        <handlers>
            <webAppContext contextPath="/admin" resourceBase="${activemq.base}/webapps/admin" logUrlOnStart="true" />
        </handlers>
    </jetty>
</beans>
```
Add this XML snippet to the web.xml for the /admin/ app, in order to enable HTTP Authentication to match the activemq.xml configuration above.
```
<security-constraint>
    <web-resource-collection>
        <web-resource-name>Web Console</web-resource-name>
        <url-pattern>/*</url-pattern>
    </web-resource-collection>
    <auth-constraint>
        <role-name>admins</role-name>
    </auth-constraint>
</security-constraint>

<login-config>
    <auth-method>BASIC</auth-method>
    <realm-name>ActiveMQ</realm-name>
</login-config>
```
