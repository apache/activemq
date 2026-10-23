---
title: "WebLogic Integration"
slug: /weblogic-integration
---


ActiveMQ WebLogic Integration
=============================

ActiveMQ clients and brokers can be run in WebLogic Server or WebLogic Express. This is usually done for licensing reasons:

*   WebLogic Server includes JMS client and server features, but it may cost extra to use these JMS features to connect multiple servers together (as opposed to only using JMS internally within one server)
*   WebLogic Express disables the JMS features via the license, so while JMS classes are on the class path, there are no JMS features available.  
    An ActiveMQ broker may be run within WebLogic to provide JMS features while avoiding these issues. As well, an ActiveMQ client may be run within WebLogic to connect to either an ActiveMQ broker running in the same WebLogic instance, and ActiveMQ broker running in a remote WebLogic instance, or a standalone ActiveMQ broker running elsewhere on the network.

ActiveMQ Clients in WebLogic
----------------------------

An ActiveMQ client can be run in any recent version of WebLogic. However, the specifics depend on which revision of JMS is supported by your version of WebLogic.

### WebLogic Server/Express versions including JMS 1.0.x

This is known to include **WebLogic 8.1** and all previous versions.

**Note:** ActiveMQ cannot deliver messages to message-driven beans in this version of WebLogic. You will need to create JMS clients manually to send and receive JMS messages.

To work with ActiveMQ from one of these releases of WebLogic, you should not include any `javax.jms` classes with your application, but should instead use the JMS 1.0.x classes distributed with WebLogic. This means you should not use a fully-aggregated ActiveMQ JAR such as `activemq-4.0.1/incubator-activemq-4.0.1.jar` but instead, individual ActiveMQ JARs such as `activemq-4.0.1/lib/activeio-core-3.0-beta3.jar` and `activemq-4.0.1/lib/activemq-core-4.0.1.jar`.

Further, your code should use only JMS 1.0.x versions of the JMS interfaces (such as `javax.jms.Session`). Note that JMS 1.1 uses many of the same interfaces, but has added numerous methods to them, and if you use the new methods your code will not run in this version of WebLogic. Try to ensure you're compiling your client code against the `weblogic.jar` from the correct version of WebLogic, or else a dedicated JMS 1.0.x JAR such as the one you can download from Sun. _Do not compile against a JAR like `activemq-4.0.1/incubator-activemq-4.0.1.jar` because that JAR includes JMS 1.1!_

Finally, you can connect to ActiveMQ using the normal ActiveMQ JNDI settings including a connect URL. Do not attempt to use WebLogic JNDI to connect to ActiveMQ, and do not use the WebLogic host name or listen port in the ActiveMQ connect URL – even if the ActiveMQ broker is run within WebLogic it will listen on a separate network port from the one that WebLogic uses.

**TODO:** Add sample JMS 1.0.x code

### WebLogic Server versions including JMS 1.1

**WebLogic 9.2** and above are known to support JMS 1.1. (I don't know what JMS release WebLogic 9.0-9.1 support)

To work with ActiveMQ in one of these release of WebLogic Server, you can use the J2EE Connector included with ActiveMQ (known as ActiveMQ-RA). This will provide full ActiveMQ support to J2EE applications – ActiveMQ can be used to both send messages and to deliver incoming messages to message-driven beans. Your J2EE applications can map connection factory and destination references in JNDI.

_I don't have an example of using the J2EE Connector from a J2EE 1.4 application in WebLogic. If someone can contribute one that would be great!_

If you're using Spring or another non-J2EE architecture in your application, you may choose to skip the J2EE Connector and just connect directly to ActiveMQ using the technique described in the next section.

### WebLogic Express versions including JMS 1.1

**WebLogic 9.2** and above are known to support JMS 1.1. (I don't know what JMS release WebLogic 9.0-9.1 support)

J2EE Connectors are not supported in WebLogic Express, so that approach is out. Instead, any components that want to connect to ActiveMQ need to manually create the connection (or use a technique like Spring message-driven POJOs to manage it). However, this should be relatively trouble-free since ActiveMQ and WebLogic support the same JMS release.

**TODO:** Example of JMS 1.1 client and Spring Message-Driven POJO client for WebLogic

ActiveMQ Brokers in WebLogic
----------------------------

On the surface it's not obvious why you might want to run an ActiveMQ broker within WebLogic instead of standalone, but the considerations include:

*   When ActiveMQ is run within WebLogic, it can be deployed, started, stopped, and redeployed like other applications, using the WebLogic console or other tools, while the JVM and WebLogic are still running
*   ActiveMQ can hook into the WebLogic JMX management system (as well as e.g. JRockit) to provide a single management interface to the app server, applications, ActiveMQ, and even the JVM.
*   ActiveMQ can leverage WebLogic security realms for authentication and authorization – not only are there more security realm types available for WebLogic, but they're also easier to configure via the WebLogic console

If you decide to run ActiveMQ standalone, that's fine, you can skip this section. Otherwise, read on.

The specific integration techniques depend on whether you're using WebLogic Server or WebLogic Express, and whether your version supports JMS 1.0.x or JMS 1.1.

### WebLogic Server/Express versions including JMS 1.0.x

Deploying an ActiveMQ broker in a release of WebLogic Server or WebLogic Express that supports only JMS 1.0.x has not been tested. This is known to include **WebLogic 8.1** and all previous versions. It will probably have trouble due to conflicting versions of JMS used by ActiveMQ and by WebLogic.

_Please report your results if you try this_

### WebLogic Server versions including JMS 1.1

**WebLogic 9.2** and above are known to support JMS 1.1. (I don't know what JMS release WebLogic 9.0-9.1 support)

It is possible to deploy ActiveMQ in WebLogic Server in one of two ways:

*   **Using the ActiveMQ-RA J2EE Connector** – ActiveMQ provides a J2EE Connector that is normally used by clients to connect to a separate ActiveMQ broker. However, the J2EE Connector accepts arguments to run an embedded broker and provide the location of an XML config file for an embedded broker. This is probably the best way to deploy an ActiveMQ broker in WebLogic Server, because ActiveMQ then has access to WebLogic thread pools and other resources.
*   **Deploying an ActiveMQ Broker as a WebLogic Application** – This is the technique described below for WebLogic Express. It works equally well in WebLogic Server.

The J2EE Connector approach has not be specifically tested, but should work well. It would need some additional classes to support the security and management integration as described in the next section, so those would need to be added to a JAR which is in turn added to the J2EE Connector RAR file. _Please report your results if you try this_

### WebLogic Express versions including JMS 1.1

**WebLogic 9.2** and above are known to support JMS 1.1. (I don't know what JMS release WebLogic 9.0-9.1 support)

Since WebLogic Express does not support J2EE Connectors, it is necessary to deploy ActiveMQ wrapped in an application that is deployed to WebLogic. Note that ActiveMQ runs in the XBean container, so XBean must be pointed to an appropriate configuration file to start up and it will then configure and start all the ActiveMQ components. The first challenge is then to construct an application that locates the right config file(s) and starts the ActiveMQ broker when it is deployed.

#### ActiveMQ as a WebLogic Application

The easiest type of WebLogic application to configure with all the needed ActiveMQ libraries and configuration and not much else is a web application. The JARs go in `WEB-INF/lib/` and config files typically in `WEB-INF/`. The only necessary configuration for the web application itself is to install a listener that will start and stop ActiveMQ when the web application is started and stopped. There are also a couple optional classes that can be used to integrate ActiveMQ with WebLogic's security system. Additionally, in this example, a simple web page included in the WAR will be available whenever ActiveMQ is running, so a simple HTTP request can determine whether the ActiveMQ module has been started.

#### Management Options

ActiveMQ exposes a number of management and monitoring features via JMX. There are three ways to expose these JMX objects:

1.  Have ActiveMQ start its own JMX MBeanServer and network connector (typically on port 1099)
2.  Have ActiveMQ use the JVM's MBeanServer (for Java 5 and higher, either Sun VM or JRockit VM)
3.  Have ActiveMQ use the MBeanServer provided by a runtime environment such as WebLogic

Each approach has its advantages and disadvantages. A few things to note:

*   Make sure that each ActiveMQ instance on the same machine uses a separate network port to handle JMX requests. This is set in the config file when using the ActiveMQ MBeanServer, in a system property (command-line argument) when using the JVM's MBeanServer, and it uses the standard WebLogic listen port for WebLogic JMX
*   JRockit appears to require a runtime license to use the embedded MBeanServer, while the Sun JVM provides it for free

The sample below includes an optional configuration block that lets ActiveMQ hook into the WebLogic runtime MBeanServer. This means ActiveMQ MBeans will appear alongside WebLogic MBeans (and even JVM MBeans if they are enabled). With this approach, management clients will access ActiveMQ MBeans through the normal WebLogic listen port (e.g. 7001) rather than using a dedicated JMX port, though IIOP must be enabled for this to work. However, note that this is optional, and you can skip that configuration block and use one of the other approaches (JVM or embedded MBeanServer) to expose the ActiveMQ MBeans.

#### Security Options

ActiveMQ has optional authentication and authorization plugins, which are based on JAAS. Fortunately, WebLogic provides a JAAS LoginModule that performs the authentication against the default WebLogic security realm, and returns the appropriate principals for authorization. Unfortunately, by default, ActiveMQ cannot authorize access to specific users within the security realm based on WebLogic principals, meaning either there's no security or a login is required but any user with a valid login has full access to ActiveMQ. However, with custom security classes like the ones shown below, ActiveMQ can use the WebLogic security realm to process a login and then you can configure the WebLogic principals (users and/or groups) that should be allowed to perform read/write/create/remove actions on a per-broker or per-destination basis.

You may choose to use authentication only, in which case any user with a valid WebLogic login can access ActiveMQ, and no custom code is necessary. You may also add authorization to that using some custom code, to apply specific security constraints to specific users or destinations. For purposes of this example, we have only implemented an authorization approach that allows any member of a single specific WebLogic group to access all resources in ActiveMQ. Between the authorization plugin provided here and the default one provided with ActiveMQ, you should have the foundation to enhance this if more feature-rich authorization is required.

#### Persistence Options

ActiveMQ uses a combination of a local journal (files on the file system) and a backing database by default. In the standard configuration, an embedded Derby database is used. This runs fine in WebLogic, but it's also possible to have ActiveMQ use a database connection pool defined in WebLogic, rather than using a separate Derby database. The configuration files shown later have commented-out sections referring to a WebLogic database connection pool – if you want to use those, you'll just need to set the correct JNDI name that was used for the database connection pool.

Note that ActiveMQ will create the tables it needs the first time it connects to a database, so no particular preparation is required for the database.

#### ActiveMQ Integration Architecture

The ActiveMQ-in-WebLogic combination looks like this:

![](/img/activemq-in-weblogic.png)

To summarize this diagram:

*   ActiveMQ runs as a web application, which runs in the web container inside WebLogic Express
*   ActiveMQ reads two configuration files from a directory on the WebLogic class path – one to configure ActiveMQ itself, and one to configure its logging output
*   ActiveMQ uses two main resources from WebLogic Express – the JMX management server (MBeanServer) and the WebLogic security realm (in the form of a JAAS LoginModule).
*   In order to start ActiveMQ when the web application is deployed, the `web.xml` includes a context listener for Spring, and the Spring `applicationContext.xml` file loads the ActiveMQ broker and points it to the ActiveMQ configuration file (which as mentioned above, is stored in a directory on the class path).
*   Within ActiveMQ:
    *   A configuration block in the ActiveMQ configuration file causes ActiveMQ to register all its JMX MBeans with the WebLogic JMX MBeanServer
    *   A security plugin causes all clients to authenticate against the WebLogic security realm
    *   The security plugin also reads the name of a WebLogic group from the ActiveMQ configuration file, and requires that any client must be a member of that group (when WebLogic processed the login, it must have created a principal for that group for the user)
    *   The ActiveMQ configuration file also identifies a network listen port that ActiveMQ will listen on for JMS clients. Any clients must include the server's hostname and this listen port in their connect URL.
    *   The ActiveMQ logging system (Log4J) outputs log message in the format specified in and to the destinations specified in the logging configuration file
    *   Persistent messages are stored to the journal in the ActiveMQ Data Directory and periodically spooled to a remote database (database not shown in diagram)
*   Note that ActiveMQ uses its own threads; since WebLogic Express does not support J2EE Connectors, it is not possible for ActiveMQ to access a WebLogic thread pool without more extensive customization

#### Building the ActiveMQ to WebLogic Integration WAR

This section discusses the code, libraries, and configuration files necessary to build the ActiveMQ web application that will be deployed in WebLogic Express.

##### Starting and Stopping ActiveMQ

ActiveMQ needs to start when the web application is deployed or started, and stop when the web application is stopped or undeployed. The easiest way to do this is to a use the commonly available Spring listeners, which start a Spring context, which can point ActiveMQ to an XBean configuration file, which starts ActiveMQ. A little roundabout, but it works well in practice with very little configuration.

**J2EE WEB-INF/web.xml**
```xml
<?xml version="1.0" encoding="UTF-8"?>

<web-app version="2.4"
         xmlns="http://java.sun.com/xml/ns/j2ee"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://java.sun.com/xml/ns/j2ee http://java.sun.com/xml/ns/j2ee/web-app\_2\_4.xsd" >
    <listener>
        <listener-class>org.springframework.web.context.ContextLoaderListener</listener-class>
    </listener>
</web-app>
```
**Spring WEB-INF/applicationContext.xml**
```xml
<?xml version="1.0" encoding="UTF-8" ?>
<!DOCTYPE beans PUBLIC "-//SPRING//DTD BEAN//EN" "http://www.springframework.org/dtd/spring-beans.dtd">
<beans>
    <bean id="brokerService" class="org.apache.activemq.xbean.BrokerFactoryBean">
        <property name="config" value="classpath:activemq-config.xml"/>
    </bean>
</beans>
```
Again, the `web.xml` listener starts Spring and reads the Spring `META-INF/applicationContext.xml` file (the default location used by the `ContextLoaderListener`), and the `applicationContext.xml` indicates that the ActiveMQ configuration file should be read from the class path with the name `activemq-config.xml`. That file could be placed in `WEB-INF/classes` or in a directory on the file system that is added to the WebLogic class path (for ease of editing).

With this approach, the normal WebLogic deployment tools, admin console, etc. can be used to deploy, start, stop, and restart ActiveMQ (packaged into a web application WAR).

##### Required Libraries

ActiveMQ required a number of JARs which should be included in the web application `WEB-INF/lib` directory.

This list was generated for ActiveMQ 4.0.1:

*   `activemq-core` (basic ActiveMQ code)
*   `activemq-jaas` (ActiveMQ security code)
*   `activeio-core` (ActiveMQ networking layer)
*   `derby` (embedded DB for persistent messages)
*   `commons-logging` (logging abstraction library)
*   `log4j` (actual logging package used)
*   `backport-util-concurrent` (concurrency utility library)
*   `spring` (used to configure and start ActiveMQ)
*   `xbean-spring` (used to configure ActiveMQ)

Additionally, to build the custom security plugins, the WebLogic `server/lib/weblogic.jar` is presently required at compile time.

Of these, Derby could be omitted if ActiveMQ was configured to not use a database for persistence or to use a separate database (e.g. a WebLogic database pool) for persistence. The WebLogic JAR is needed only at build time (it's provided by the server at runtime). Spring could be omitted if a different strategy was used to start and stop ActiveMQ when the web app was started or stopped (a little custom code could replace this dependency). The rest are probably unavoidable, unless ActiveMQ changes its dependencies in a future version.

##### WebLogic Integration Code

There are two custom classes used for this example. We'll show in a minute how to configure ActiveMQ to use these. Note that these are optional – if you don't want to leverage WebLogic's security realm, you can skip these.

The first class makes ActiveMQ use the WebLogic security realm for authentication, and lets you specify a single WebLogic group to use for authorization (only members of that group can access ActiveMQ, though group members have full access to ActiveMQ).

**ActiveMQToWebLogicSecurity.java**

This class is an ActiveMQ "plugin", which installs two filters (authorization and authentication) which will be invoked on every request. This is similar to the default behavior provided by ActiveMQ's `JaasAuthenticationPlugin` and `AuthorizationPlugin`.
```
/**
 * An ActiveMQ security plugin that installs two security filters
 * (authentication and authorization) that use WebLogic security realms to
 * handle the login and provide user and group principals.
 */
public class ActiveMQToWebLogicSecurity implements BrokerPlugin {
    private String authorizedGroup;

    public Broker installPlugin(Broker broker) {
        // Install the first filter for authentication
        Broker first = new ActiveMQWebLogicAuthenticationFilter(broker);
        // Configure and install the second filter for authorization
        AuthorizationEntry entry = new AuthorizationEntry();
        Set acls = new HashSet();
        acls.add(new WLSGroupImpl(authorizedGroup));
        entry.setAdminACLs(acls);
        entry.setReadACLs(acls);
        entry.setWriteACLs(acls);
        DefaultAuthorizationMap map = new DefaultAuthorizationMap();
        map.setDefaultEntry(entry);
        //todo: if finer-grained access is required, add more entries to the authorization map
        Broker second = new AuthorizationBroker(first, map);
        return second;
    }

    public String getAuthorizedGroup() {
        return authorizedGroup;
    }

    /**
     * Called by XBean at configuration time to set the authorized group from a
     * property in the main ActiveMQ configuration file.
     */
    public void setAuthorizedGroup(String authorizedGroup) {
        this.authorizedGroup = authorizedGroup;
    }
}
```
The second class is the authentication filter used by the class above to authenticate all logins against the WebLogic default security realm.

**ActiveMQWebLogicAuthenticationFilter.java**
```
/**
 * A broker filter that authenticates callers against WebLogic security.
 * This is similar to the ActiveMQ JaasAuthenticationBroker except for two
 * things:
 * <ul>
 *   <li>Instead of reading a JAAS configuration file, it hardcodes the JAAS
 *     configuration to require authentication against WebLogic</li>
 *
 *   <li>The SecurityContext implementation overrides the method used to
 *     compare actual and eligible principals in order to handle the fact
 *     that WebLogic principals (WLSGroupImpl in particular) do not seem
 *     to match according to equals and hashCode even if the principal class
 *     and principal name are the same (perhaps having to do with the
 *     signature data on the WLSAbstractPrincipal).</li>
 * </ul>
 */
public class ActiveMQWebLogicAuthenticationFilter extends BrokerFilter {
    private final static Configuration WEBLOGIC\_JAAS\_CONFIGURATION = new Configuration() {
        public AppConfigurationEntry\[\] getAppConfigurationEntry(String name) {
            return new AppConfigurationEntry\[\]{
                    new AppConfigurationEntry("weblogic.security.auth.login.UsernamePasswordLoginModule",
                            AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, Collections.EMPTY_MAP)
            };
        }

        public void refresh() {
        }
    };
    private final CopyOnWriteArrayList securityContexts = new CopyOnWriteArrayList();

    public ActiveMQWebLogicAuthenticationFilter(Broker next) {
        super(next);
    }

    static class JaasSecurityContext extends SecurityContext {

        private final Subject subject;

        public JaasSecurityContext(String userName, Subject subject) {
            super(userName);
            this.subject = subject;
        }

        public Set getPrincipals() {
            return subject.getPrincipals();
        }

        /**
         * This is necessary because WebLogic uses extra logic when comparing principals,
         * probably to check whether they are cryptographically signed (which WebLogic
         * supports).  We skip that test because ActiveMQ does not sign the principals
         * it deals with.
         */
        public boolean isInOneOf(Set eligiblePrincipals) {
            for (Iterator it = getPrincipals().iterator(); it.hasNext();) {
                Principal test = (Principal) it.next();
                for (Iterator el = eligiblePrincipals.iterator(); el.hasNext();) {
                    Principal eligible = (Principal) el.next();
                    if(test.getName().equals(eligible.getName()) &&
                            test.getClass().getName().equals(eligible.getClass().getName())) {
                        return true;
                    }
                }
            }
            return false;
        }
    }

    public void addConnection(ConnectionContext context, ConnectionInfo info) throws Exception {
        if( context.getSecurityContext()==null ) {
            // Do the login.
            try {
                LoginContext lc = new LoginContext("ActiveMQ", new Subject(),
                        new URLCallbackHandler(info.getUserName(), info.getPassword()),
                        WEBLOGIC\_JAAS\_CONFIGURATION);
                lc.login();
                Subject subject = lc.getSubject();

                SecurityContext s = new JaasSecurityContext(info.getUserName(), subject);
                context.setSecurityContext(s);
                securityContexts.add(s);
            } catch (Exception e) {
                throw (SecurityException)new SecurityException("User name or password is invalid.").initCause(e);
            }
        }
        super.addConnection(context, info);
    }

    public void removeConnection(ConnectionContext context, ConnectionInfo info, Throwable error) throws Exception {
        super.removeConnection(context, info, error);
        if( securityContexts.remove(context.getSecurityContext()) ) {
            context.setSecurityContext(null);
        }
    }

    /**
     * Previously logged in users may no longer have the same access anymore.  Refresh
     * all the logged into users.
     */
    public void refresh() {
        for (Iterator iter = securityContexts.iterator(); iter.hasNext();) {
            SecurityContext sc = (SecurityContext) iter.next();
            sc.getAuthorizedReadDests().clear();
            sc.getAuthorizedWriteDests().clear();
        }
    }
}
```

##### Sample ActiveMQ Configuration Files

This section shows three sample ActiveMQ configuration files – one for a single broker with the security and management plugins listed above, and one for a network of two brokers with the security and management plugins listed above. If you want to skip the security and management plugins, you can remove those attributes from the main `<broker>` element and remove the `<bean>` definitions for them below.

It also shows a Log4J configuration file, which controls the ActiveMQ log output.

**Single Broker**
```xml
<?xml version="1.0" encoding="UTF-8"?>
<beans xmlns="http://activemq.org/config/1.0">
  <!-- Allows us to use system properties as variables in this configuration file -->
  <bean class="org.springframework.beans.factory.config.PropertyPlaceholderConfigurer"/>
  
  <broker useJmx="true" brokerName="MyBroker" useShutdownHook="false"
          plugins="#WebLogicSecurity">

    <!-- Register all ActiveMQ MBeans with the WebLogic runtime MBeanServer -->
    <managementContext>
      <managementContext>
          <MBeanServer>
             <bean class="org.springframework.jndi.JndiObjectFactoryBean" xmlns="">
                <property name="jndiName" value="java:comp/env/jmx/runtime" />
                <property name="lookupOnStartup" value="true" />
                <property name="expectedType" value="javax.management.MBeanServer" />
             </bean>
          </MBeanServer>
      </managementContext>
    </managementContext>
  
    <persistenceAdapter>
      <!-- By default, use an embedded Derby database -->
        <journaledJDBC journalLogFiles="5"
                       dataDirectory="/server/bea/weblogic920/domains/jms/activemq-data"/>
      <!-- Use this with the WebLogicDataSource below to use a WebLogic
           database connection pool instead of the embedded Derby database
        <journaledJDBC journalLogFiles="5"
                       dataDirectory="/server/bea/weblogic920/domains/jms/activemq-data"
                       dataSource="#WebLogicDataSource" />

      -->
    </persistenceAdapter>
  
    <transportConnectors>
        <transportConnector name="MyBrokerTCP" uri="tcp://0.0.0.0:61616" />
    </transportConnectors>
  </broker>

  <bean id="WebLogicSecurity"
        class="com.example.activemq.weblogic.ActiveMQToWebLogicSecurity">
      <property name="authorizedGroup" value="ActiveMQUsers" />
  </bean>

<!-- Uncomment and configure this if you want to use a WebLogic database
     connection pool for persistent messages
  <bean id="WebLogicDataSource"
        class="org.springframework.jndi.JndiObjectFactoryBean">
     <property name="jndiName" value="db/pool/jndi/name"/>
  </bean>
-->
</beans>
```

**Network of Brokers (Broker 1/2)**
```xml
<?xml version="1.0" encoding="UTF-8"?>
<beans xmlns="http://activemq.org/config/1.0">
  <!-- Allows us to use system properties as variables in this configuration file -->
  <bean class="org.springframework.beans.factory.config.PropertyPlaceholderConfigurer"/>
  
  <broker useJmx="true" brokerName="FirstBroker" useShutdownHook="false"
          plugins="#WebLogicSecurity">

    <!-- Register all ActiveMQ MBeans with the WebLogic runtime MBeanServer -->
    <managementContext>
      <managementContext>
          <MBeanServer>
             <bean class="org.springframework.jndi.JndiObjectFactoryBean" xmlns="">
                <property name="jndiName" value="java:comp/env/jmx/runtime" />
                <property name="lookupOnStartup" value="true" />
                <property name="expectedType" value="javax.management.MBeanServer" />
             </bean>
          </MBeanServer>
      </managementContext>
    </managementContext>
  
    <persistenceAdapter>
      <!-- By default, use an embedded Derby database -->
        <journaledJDBC journalLogFiles="5"
                       dataDirectory="/server/bea/weblogic920/domains/jms/activemq-b1-data"/>
      <!-- Use this with the WebLogicDataSource below to use a WebLogic
           database connection pool instead of the embedded Derby database
        <journaledJDBC journalLogFiles="5"
                       dataDirectory="/server/bea/weblogic920/domains/jms/activemq-b1-data"
                       dataSource="#WebLogicDataSource" />

      -->
    </persistenceAdapter>
  
    <transportConnectors>
        <transportConnector name="FirstBrokerTCP" uri="tcp://0.0.0.0:60010" />
    </transportConnectors>
    
    <networkConnectors>
        <networkConnector name="Broker1ToBroker2"
                          uri="static://(tcp://localhost:60011)" failover="true"
                          userName="fixme" password="fixme"/>
    </networkConnectors>
  </broker>

  <bean id="WebLogicSecurity"
        class="com.example.activemq.weblogic.ActiveMQToWebLogicSecurity">
      <property name="authorizedGroup" value="ActiveMQUsers" />
  </bean>

<!-- Uncomment and configure this if you want to use a WebLogic database
     connection pool for persistent messages
  <bean id="WebLogicDataSource"
        class="org.springframework.jndi.JndiObjectFactoryBean">
     <property name="jndiName" value="db/pool/jndi/name"/>
  </bean>
-->
</beans>
```

**Network of Brokers (Broker 2/2)**
```xml
<?xml version="1.0" encoding="UTF-8"?>
<beans xmlns="http://activemq.org/config/1.0">
  <!-- Allows us to use system properties as variables in this configuration file -->
  <bean class="org.springframework.beans.factory.config.PropertyPlaceholderConfigurer"/>
  
  <broker useJmx="true" brokerName="SecondBroker" useShutdownHook="false"
          plugins="#WebLogicSecurity">

    <!-- Register all ActiveMQ MBeans with the WebLogic runtime MBeanServer -->
    <managementContext>
      <managementContext>
          <MBeanServer>
             <bean class="org.springframework.jndi.JndiObjectFactoryBean" xmlns="">
                <property name="jndiName" value="java:comp/env/jmx/runtime" />
                <property name="lookupOnStartup" value="true" />
                <property name="expectedType" value="javax.management.MBeanServer" />
             </bean>
          </MBeanServer>
      </managementContext>
    </managementContext>
  
    <persistenceAdapter>
      <!-- By default, use an embedded Derby database -->
        <journaledJDBC journalLogFiles="5"
                       dataDirectory="/server/bea/weblogic920/domains/jms/activemq-b2-data"/>
      <!-- Use this with the WebLogicDataSource below to use a WebLogic
           database connection pool instead of the embedded Derby database
        <journaledJDBC journalLogFiles="5"
                       dataDirectory="/server/bea/weblogic920/domains/jms/activemq-b2-data"
                       dataSource="#WebLogicDataSource" />

      -->
    </persistenceAdapter>
  
    <transportConnectors>
        <transportConnector name="SecondBrokerTCP" uri="tcp://0.0.0.0:60011" />
    </transportConnectors>
    
    <networkConnectors>
        <networkConnector name="Broker2ToBroker1"
                          uri="static://(tcp://localhost:60010)" failover="true"
                          userName="fixme" password="fixme" />
    </networkConnectors>
  </broker>

  <bean id="WebLogicSecurity"
        class="com.example.activemq.weblogic.ActiveMQToWebLogicSecurity">
      <property name="authorizedGroup" value="activemq" />
  </bean>

<!-- Uncomment and configure this if you want to use a WebLogic database
     connection pool for persistent messages
  <bean id="WebLogicDataSource"
        class="org.springframework.jndi.JndiObjectFactoryBean">
     <property name="jndiName" value="db/pool/jndi/name"/>
  </bean>
-->
</beans>
```

**Log4J Configuration File (`log4j.properties`)**
```
# Can change this to "INFO, out" or "INFO, out, stdout"
# to enable logging to the file defined down below
log4j.rootLogger=INFO, stdout
log4j.logger.org.apache.activemq.spring=WARN
log4j.logger.org.springframework=WARN
log4j.logger.org.apache.xbean.spring=WARN

# Configuration to log to stdout
log4j.appender.stdout=org.apache.log4j.ConsoleAppender
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
log4j.appender.stdout.layout.ConversionPattern=%-5p %-30.30c{1} - %m%n
log4j.appender.stdout.threshold=INFO

# Configuration for a log file (in addition to or instead of stdout)
log4j.appender.out=org.apache.log4j.RollingFileAppender
log4j.appender.out.file=/server/bea/weblogic920/domains/test/activemq.log
log4j.appender.out.maxFileSize=1024KB
log4j.appender.out.maxBackupIndex=5
log4j.appender.out.append=true
log4j.appender.out.layout=org.apache.log4j.PatternLayout
log4j.appender.out.layout.ConversionPattern=%d \[%-15.15t\] %-5p %-30.30c{1} - %m%n
```

#### Installation Procedure

This procedure makes the following assumptions:

*   The ActiveMQ configuration file and Log4J configuration file will be stored in a subdirectory of the WebLogic domain directory for ease of editing. They may be packaged into `WEB-INF/classes` in the WAR for ease of distribution instead.
*   The ActiveMQ JMX management features will be exposed via the WebLogic runtime JMX MBeanServer. This is not required, but it should allow existing WebLogic management scripts to see the ActiveMQ MBeans. _NOTE: Additional configuration is required if the Sun JVM JMX server or JRockit management server will be used instead, or if the embedded ActiveMQ JMX server will be used instead and more than one ActiveMQ instance will be run on the same server._
*   ActiveMQ will be installed on a single WebLogic instance or on two WebLogic instances in a Network-of-Brokers configuration.
    *   In a (Network-of-Brokers) cluster, the two ActiveMQ instances will use specific URLs to locate each other, rather than attempting runtime discovery of arbitrary brokers that may be present on the network.
*   Either ActiveMQ security is disabled, or ActiveMQ logins are validated against the WebLogic default security realm and a login is required and any member of a designated WebLogic group can access ActiveMQ.

Prerequisites

*   WebLogic Express 9.2 has been installed
*   A simple WebLogic domain has been created
*   If the JRockit VM will be used on Linux, the file handle limit of the user running WebLogic may be important (test with `ulimit -n`). A recommended value would be at least twice the expected maximum number of JMS clients.
*   The ActiveMQ web application WAR (`activemq-4.0.1-web.war`)
*   The ActiveMQ configuration file (`activemq-config.xml`)
*   The Log4J configuration file (`log4j.properties`)

Installation Procedure

1.  Create a subdirectory of the WebLogic domain directory to hold ActiveMQ config files (e.g. `domain-dir/activemq/config`)
2.  Create a subdirectory of the WebLogic domain directory to hold ActiveMQ runtime data – persistent messages, etc. (e.g. `domain-dir/activemq/data`)
3.  Edit the `activemq-config.xml` to point to the correct runtime data directory created in step 2 (look for the part of the file that reads `dataDirectory="..."`)
4.  In `activemq-config.xml`, review the port for the current ActiveMQ server defined in the `<transportConnector .../>` line, if ActiveMQ clustering will be used, review the hostname and port for the other ActiveMQ server(s) in the cluster defined in the `<networkConnector .../>` line.
5.  If ActiveMQ clustering and security are both enabled, in `activemq-config.xml`, find (or add) the `userName` and `password` settings on the `<networkConnector .../>` and change them to valid values. (When security is enabled, the ActiveMQ brokers in a cluster require valid logins to connect to each other to form the cluster.)
6.  If security is enabled, in `activemq-config.xml`, find the `authorizedGroup` setting on the `WebLogicSecurity` bean and set that to the name of the WebLogic group whose members should be able to access ActiveMQ. Note that the account used by the network connectors in the previous step must be members of this group. (This entry in the config file is described in the previous section)
7.  (Optional) Update the `log4j.properties` file to save ActiveMQ logs to a file rather than just emitting to the WebLogic server's standard output (e.g. change the first line to `log4j.rootLogger=INFO, out, stdout` and change the destination file in the `log4j.appender.out.file=...` line)
8.  Save the `activemq-config.xml` and `log4j.properties` files to the ActiveMQ configuration directory created in step 1
9.  Edit the `startWebLogic.sh` file in the domain directory and add the following line to the top (immediately after the `DOMAIN_HOME=...` line), adjusting the line to use the directory created in step 1:
    ```
    export EXT_POST_CLASSPATH=${DOMAIN_HOME}/activemq/config
    ```
10.  Start WebLogic
11.  Deploy the `activemq-4.0.1-web.war` file, by copying it to the `domain-dir/autodeploy` directory or using the deployment tools in the WebLogic console.
12.  Verify that the WebLogic server standard output (or the ActiveMQ log file configured in step 5) shows that ActiveMQ started, with output like this:
     ```
     INFO  BrokerService                  - ActiveMQ 4.0.1 JMS Message Broker (localhost) is starting
     INFO  BrokerService                  - For help or more information please see:
                                            http://incubator.apache.org/activemq/
     INFO  JDBCPersistenceAdapter         - Database driver recognized: \[apache\_derby\_embedded\_jdbc\_driver\]
     INFO  JournalPersistenceAdapter      - Journal Recovery Started from: Active Journal: using 5 x 20.0 Megs at:
                                            /data/server/bea/weblogic920/domains/test/activemq-data/journal
     INFO  JournalPersistenceAdapter      - Journal Recovered: 0 message(s) in transactions recovered.
     INFO  TransportServerThreadSupport   - Listening for connections at: tcp://0.0.0.0:60010
     INFO  TransportConnector             - Connector default Started
     INFO  BrokerService                  - ActiveMQ JMS Message Broker
                                            (localhost, ID:remus-28763-1156532531109-1:0) started
     ```
13.  Test a management connection by running the `jconsole` tool distributed with Java 5, using an advanced connection, entering the URL `service:jmx:rmi:///jndi/iiop://localhost:7001/weblogic.management.mbeanservers.runtime` (adjusting `localhost:7001` to the WebLogic server hostname and port) and the WebLogic administrator username and password. Once connected, on the MBeans tab, there should be entries under `org/activemq`
14.  Test a security if desired by running the example producer and consumer from an ActiveMQ 4.0.1 distribution. Note that the connect URL is in `examples/build.xml` while the connect username and password need to be added to `examples/src/ConsumerTool.java` and `examples/src/ProducerTool.java` to connect successfully to a secure instance.
15.  If ActiveMQ clustering will be used, repeat this process for the other WebLogic/ActiveMQ instance in the cluster. This should set up the two ActiveMQ servers to connect to each other. Confirm this by looking for output like this once both brokers are running:
     ```
     INFO  DemandForwardingBridge         - Network connection between vm://FirstBroker#0 and
                                            tcp://localhost:63222(SecondBroker) has been established.
     ```

