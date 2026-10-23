---
title: "Security"
description: "Secure ActiveMQ Classic with JAAS or simple authentication, authorizationPlugin ACLs, LDAP, broker-to-broker credentials and message-level authorization."
slug: /security
---

ActiveMQ 4.x and greater provides pluggable security through various different providers.

The most common providers are

*   [JAAS](http://java.sun.com/products/jaas/) for authentication
*   a default authorization mechanism using a simple XML configuration file.

### Authentication

The default [JAAS](http://java.sun.com/products/jaas/) plugin relies on the standard JAAS mechanism for authentication. Refer to the [documentation](http://java.sun.com/products/jaas/reference/docs/index.html) for more detail.

Typically you configure JAAS using a config file like [this one](http://svn.apache.org/repos/asf/activemq/trunk/activemq-unit-tests/src/test/resources/login.config) and set the **java.security.auth.login.config** system property to point to it. If no system property is specified then by default the ActiveMQ JAAS plugin will look for **login.config** on the classpath and use that.

#### Authentication Example

Here is an example [login.config](http://svn.apache.org/repos/asf/activemq/trunk/activemq-unit-tests/src/test/resources/login.config) which then points to these files

*   [users.properties](http://svn.apache.org/repos/asf/activemq/trunk/activemq-unit-tests/src/test/resources/org/apache/activemq/security/users.properties)
*   [groups.properties](http://svn.apache.org/repos/asf/activemq/trunk/activemq-unit-tests/src/test/resources/org/apache/activemq/security/groups.properties)

**Note:** Until version 5.11.1, these property files got reloaded on every authentication request by default. So updates to users, password and groups were loaded immediately. From 5.12 onward they only get reloaded if reload=true is set in your LoginModule configuration, e.g.
```
activemq { org.apache.activemq.jaas.PropertiesLoginModule required org.apache.activemq.jaas.properties.user="users.properties" org.apache.activemq.jaas.properties.group="groups.properties" reload=true; };
```
If `reload=true` is not set, these property files get loaded on broker startup only!! See AMQ-5876 for details.

#### Simple Authentication Plugin

If you have modest authentication requirements (or just want to quickly set up your testing environment) you can use SimpleAuthenticationPlugin. With this plugin you can define users and groups directly in the broker's XML configuration. Take a look at the following snippet for example:

```
<simpleAuthenticationPlugin>
  <users>
    <authenticationUser username="system" password="manager" groups="users,admins"/>
    <authenticationUser username="user" password="password" groups="users"/>
    <authenticationUser username="guest" password="password" groups="guests"/>
  </users>
</simpleAuthenticationPlugin>
```

Users and groups defined in this way can be later used with the appropriate authorization plugin.

##### Anonymous access

From version 5.4.0 onwards, you can configure simple authentication plugin to allow anonymous access to the broker.

```
<simpleAuthenticationPlugin anonymousAccessAllowed="true"> <users> <authenticationUser username="system" password="manager" groups="users,admins"/> <authenticationUser username="user" password="password" groups="users"/> <authenticationUser username="guest" password="password" groups="guests"/> </users> </simpleAuthenticationPlugin>
```

To allow anonymous access to the broker, use `anonymousAccessAllowed` attribute and set it to `true` as shown above. Now, when the client connects without username and password provided, a default username (`anonymous`) and group (`anonymous`) will be assigned to its security context. You can use this username and password to authorize client's access to appropriate broker resources (see the next section). You can also change username and group that will be assigned to _anonymous_ users by using `anonymousUser` and `anonymousGroup` attributes.

### Authorization

In ActiveMQ we use a number of operations which you can associate with user roles and either individual queues or topics or you can use wildcards to attach to hierarchies of topics and queues.

Operation|Description
---|---
read|You can browse and consume from the destination
write|You can send messages to the destination
admin|You can lazily create the destination if it does not yet exist. This allows you fine grained control over which new destinations can be dynamically created in what part of the queue/topic hierarchy

Queues/Topics can specified using the ActiveMQ [Wildcards](wildcards) syntax.

#### Authorization Example

The following [example](https://github.com/apache/activemq/blob/main/activemq-unit-tests/src/test/resources/org/apache/activemq/security/jaas-broker.xml) shows these 2 plugins in operation. Though note its very easy to write your own plugin. 

**Note:** All users need permission to create ActiveMQ.Advisory destinations, which is given by the "admin" acl. This is to allow the creation of advisory destinations generated as a side effect of user operations. For example, sending a message for the first time may trigger producer advisory destination creation.
However, normal users should generally **not** be given access to read/write all Advisories because those are messages meant for admin users.

The following advisory permissions are required for proper operations:
* All users should be given access to advisories for temporary destinations because ActiveMQConnection uses those advisories to get early knowledge of temp destination creation and deletion. 
* In addition, dynamic network connectors use advisories to determine consumer demand so the users that will be used to create bridges need access to those advisories.  

If necessary, the use of advisories in this manner can be disabled via the _watchTopicAdvisories_ boolean attribute of ActiveMQConnectionFactory and for a networkConnector, via the network connector _staticBridge_(5.6) boolean attribute.

**Example configuration:**
```
<broker> 
  .. 
    <plugins> 
      .. 
      <authorizationPlugin> 
        <map> 
          <authorizationMap> 
            <authorizationEntries> 
              <!-- Some destination a user may access -->
              <authorizationEntry queue="TEST.Q" read="users" write="users" admin="users" /> 
              
              <!-- Grant all users permission to create advisory topics, but only admins to read/write -->
              <authorizationEntry topic="ActiveMQ.Advisory.>" read="admin" write="admin" admin="*"/> 
              
              <!-- Grant all users permission to read/write advisories for temporary destinations -->
              <authorizationEntry topic="ActiveMQ.Advisory.TempQueue" read="*" write="*" admin="*"/> 
              <authorizationEntry topic="ActiveMQ.Advisory.TempTopic" read="*" write="*" admin="*"/> 
              
              <!-- Grant users that will be used to create a network of brokers permission for consumer advisories -->
              <authorizationEntry topic="ActiveMQ.Advisory.Consumer.>" read="bridge-user,admin" write="bridge-user,admin" admin="bridge-user,admin"/> 
              <authorizationEntry topic="ActiveMQ.Advisory.VirtualDestination.Consumer.>" read="bridge-user,admin" write="bridge-user" admin="bridge-user,admin"/> 
            </authorizationEntries> 
          </authorizationMap> 
        </map> 
      </authorizationPlugin> 
      .. 
    </plugins> 
  .. 
</broker>
```

### Broker-to-Broker Authentication and Authorization

If you have enabled authentication for a particular message broker, then other brokers that wish to connect to that broker must provide the proper authentication credentials via their <networkConnector> element. For example, suppose that we have a network of brokers with the following configuration:

*   The network of brokers comprises two brokers (BrokerA and BrokerB)
*   Authentication for BrokerA has been enabled via the example `<simpleAuthenticationPlugin>` element.
*   Authentication for BrokerB has not been enabled.
*   BrokerA only listens for connections. In other words, BrokerA has a `<transportConnector>` element, but no `<networkConnector>` elements.

In order for BrokerB to connect to BrokerA, the corresponding <networkConnector> element in BrokerB's XML configuration file must be set up as follows.

```
<networkConnectors> 
  <networkConnector name="brokerAbridge" userName="user" password="password" uri="static://(tcp://brokerA:61616)"/> 
</networkConnectors>
```

Note how BrokerB's `<networkConnector>` element must provide the proper credentials in order to connect to BrokerA. If authorization has been enabled on BrokerA, then the userName assigned to the `<networkConnector>` element must also have the proper authorization credentials. Messages cannot be forwarded from BrokerB to BrokerA if BrokerA has authorization enabled and BrokerB's corresponding `<networkConnector>` element's userName has not been given the proper authorization credentials.

Also, if BrokerA is given a `<networkConnector>` element so that it can initiate a connection to BrokerB, then that `<networkConnector>` must be given a userName/password combination that is defined in the `<simpleAuthenticationPlugin>` element; this is required even though BrokerB does not have authentication services enabled.

### Controlling Access To Temporary Destinations

To control access to temporary destinations, you will need to add a `<tempDestinationAuthorizationEntry>` element to the `authorizationMap`. Through this element, you control access to all temporary destinations. If this element is not present, read, write, and admin privileges for temporary destinations will be granted to all. In the example below, read, write, and admin privileges for temporary destinations are only granted to those clients that have been assigned to the 'admin' group.

```
<broker> 
  .. 
    <plugins> 
      .. 
      <authorizationPlugin> 
        <map> 
          <authorizationMap> 
            <authorizationEntries> 
              <authorizationEntry queue="TEST.Q" read="users" write="users" admin="users" /> 
              <authorizationEntry topic="ActiveMQ.Advisory.>" read="admin" write="admin" admin="*"/> 
              <authorizationEntry topic="ActiveMQ.Advisory.TempQueue" read="*" write="*" admin="*"/> 
              <authorizationEntry topic="ActiveMQ.Advisory.TempTopic" read="*" write="*" admin="*"/> 
              <authorizationEntry topic="ActiveMQ.Advisory.Consumer.>" read="bridge-user,admin" write="bridge-user,admin" admin="bridge-user,admin"/> 
              <authorizationEntry topic="ActiveMQ.Advisory.VirtualDestination.Consumer.>" read="bridge-user,admin" write="bridge-user" admin="bridge-user,admin"/> 
            </authorizationEntries> 
            <tempDestinationAuthorizationEntry> 
              <tempDestinationAuthorizationEntry read="admin" write="admin" admin="admin"/> 
            </tempDestinationAuthorizationEntry> 
          </authorizationMap> 
        </map> 
      </authorizationPlugin> 
      .. 
    </plugins> 
  .. 
</broker>
```

### LDAP Authentication Using the JAAS Plugin

New module

A new/better ldap authorization module is available since 5.6. See [Cached LDAP Authorization Module](cached-ldap-authorization-module) for more info.

1. Configure the JAAS LDAPLoginModule and the LDAPAuthorizationMap in activemq.xml:

    ```
    <plugins> 
      <!-- use JAAS to authenticate using the login.config file on the classpath to configure JAAS --> 
      <jaasAuthenticationPlugin configuration="LdapConfiguration" /> 
      <!-- lets configure a destination based role/group authorization mechanism --> 
      <authorizationPlugin> 
        <map> 
          <bean xmlns="http://www.springframework.org/schema/beans" id="lDAPAuthorizationMap" class="org.apache.activemq.security.LDAPAuthorizationMap"> 
            <property name="initialContextFactory" value="com.sun.jndi.ldap.LdapCtxFactory"/> 
            <property name="connectionURL" value="ldap://ldap.acme.com:389"/> 
            <property name="authentication" value="simple"/> 
            <property name="connectionUsername" value="cn=mqbroker,ou=Services,dc=acme,dc=com"/> 
            <property name="connectionPassword" value="password"/> 
            <property name="connectionProtocol" value="s"/> 
            <property name="topicSearchMatching" value="cn={0},ou=Topic,ou=Destination,ou=ActiveMQ,ou=systems,dc=acme,dc=com"/> 
            <property name="topicSearchSubtreeBool" value="true"/> 
            <property name="queueSearchMatching" value="cn={0},ou=Queue,ou=Destination,ou=ActiveMQ,ou=systems,dc=acme,dc=com"/> 
            <property name="queueSearchSubtreeBool" value="true"/> 
            <property name="adminBase" value="(cn=admin)"/> 
            <property name="adminAttribute" value="member"/> 
            <property name="adminAttributePrefix" value="cn="/> 
            <property name="readBase" value="(cn=read)"/> 
            <property name="readAttribute" value="member"/> 
            <property name="readAttributePrefix" value="cn="/> 
            <property name="writeBase" value="(cn=write)"/> 
            <property name="writeAttribute" value="member"/> 
            <property name="writeAttributePrefix" value="cn="/> 
          </bean> 
        </map> 
      </authorizationPlugin> 
    </plugins>
    ```

2. Configure the JAAS login.config (I haven't de-duplicated the config yet):
    ```
    LdapConfiguration { 
       org.apache.activemq.jaas.LDAPLoginModule required 
       initialContextFactory=com.sun.jndi.ldap.LdapCtxFactory 
       connectionURL="ldap://ldap.acme.com:389" 
       connectionUsername="cn=mqbroker,ou=Services,dc=acme,dc=com" 
       connectionPassword=password connectionProtocol=s 
       authentication=simple 
       userBase="ou=User,ou=ActiveMQ,ou=systems,dc=acme,dc=com" 
       userRoleName=dummyUserRoleName 
       userSearchMatching="(uid={0})" 
       userSearchSubtree=false 
       roleBase="ou=Group,ou=ActiveMQ,ou=systems,dc=acme,dc=com" 
       roleName=cn 
       roleSearchMatching="(member:=uid={1})" 
       roleSearchSubtree=true 
       ; 
    };
    ```
3. Import the following LDIF file into the LDAP server:
    ```
    version: 1 
    # 
    # Sample LDIF for ActiveMQ LDAP authentication and authorisation 
    # Passwords are defaulted to "password" - it is your responsibility to change them! 
    # 
    # Sets up: 
    # 1. Bind user 
    # 2. A sample queue with admin,read,write permission assignments 
    # 3. ActiveMQ advisory topics 
    # 4. Two groups - admin and webapp 
    # 5. Two users - admin and webapp 
    # 6. Role assignments - admin->admin, webapp->webapp 
    # 
    # (c) Robin Bramley 2008 # Provided as is without any warranty of any kind 
    # 
    dn: dc=acme,dc=com 
    dc: acme 
    objectClass: domain 
    objectClass: top 
    
    dn: ou=Services,dc=acme,dc=com 
    ou: Services 
    objectClass: organizationalUnit 
    objectClass: top 
    
    dn: cn=mqbroker,ou=Services,dc=acme,dc=com 
    cn: mqbroker 
    objectClass: organizationalRole 
    objectClass: top 
    objectClass: simpleSecurityObject 
    userPassword: {SSHA}j0NpveEO0YD5rgI5kY8OxSRiN5KQ/kE4 
    description: Bind user for MQ broker 
    
    dn: ou=systems,dc=acme,dc=com 
    ou: systems 
    objectClass: organizationalUnit 
    objectClass: top 
    
    dn: ou=ActiveMQ,ou=systems,dc=acme,dc=com 
    objectClass: organizationalUnit 
    objectClass: top 
    ou: ActiveMQ 
    
    dn: ou=Destination,ou=ActiveMQ,ou=systems,dc=acme,dc=com 
    objectClass: organizationalUnit 
    objectClass: top 
    ou: Destination 
    
    dn: ou=Queue,ou=Destination,ou=ActiveMQ,ou=systems,dc=acme,dc=com 
    objectClass: organizationalUnit 
    objectClass: top 
    ou: Queue 
    
    dn: cn=com.acme.myfirstrealqueue,ou=Queue,ou=Destination,ou=ActiveMQ,ou=syst ems,dc=acme,dc=com 
    cn: com.acme.myfirstrealqueue 
    description: A queue 
    objectClass: applicationProcess 
    objectClass: top 
    
    dn: cn=admin,cn=com.acme.myfirstrealqueue,ou=Queue,ou=Destination,ou=ActiveM Q,ou=systems,dc=acme,dc=com 
    cn: admin 
    description: Admin privilege group, members are roles 
    member: cn=admin 
    member: cn=webapp 
    objectClass: groupOfNames 
    objectClass: top 
    
    dn: cn=read,cn=com.acme.myfirstrealqueue,ou=Queue,ou=Destination,ou=ActiveMQ ,ou=systems,dc=acme,dc=com 
    cn: read member: cn=webapp 
    objectClass: groupOfNames 
    objectClass: top 
    
    dn: cn=write,cn=com.acme.myfirstrealqueue,ou=Queue,ou=Destination,ou=ActiveM Q,ou=systems,dc=acme,dc=com 
    cn: write 
    objectClass: groupOfNames 
    objectClass: top member: cn=webapp 
    
    dn: ou=Topic,ou=Destination,ou=ActiveMQ,ou=systems,dc=acme,dc=co m 
    objectClass: organizationalUnit 
    objectClass: top 
    ou: Topic 
    
    dn: cn=ActiveMQ.Advisory.Consumer,ou=Topic,ou=Destination,ou=ActiveMQ,ou=sys tems,dc=acme,dc=com 
    cn: ActiveMQ.Advisory.Consumer 
    objectClass: applicationProcess 
    objectClass: top description: Advisory topic about consumers 
    
    dn: cn=read,cn=ActiveMQ.Advisory.Consumer,ou=Topic,ou=Destination,ou=ActiveM Q,ou=systems,dc=acme,dc=com 
    cn: read member: cn=webapp 
    objectClass: groupOfNames 
    objectClass: top 
    
    dn: cn=ActiveMQ.Advisory.TempQueue,ou=Topic,ou=Destination,ou=ActiveMQ,ou=sy stems,dc=acme,dc=com 
    cn: ActiveMQ.Advisory.TempQueue 
    description: Advisory topic about temporary queues 
    objectClass: applicationProcess 
    objectClass: top 
    
    dn: cn=read,cn=ActiveMQ.Advisory.TempQueue,ou=Topic,ou=Destination,ou=Active MQ,ou=systems,dc=acme,dc=com 
    cn: read member: cn=webapp 
    objectClass: groupOfNames 
    objectClass: top 
    
    dn: cn=ActiveMQ.Advisory.TempTopic,ou=Topic,ou=Destination,ou=ActiveMQ,ou=sy stems,dc=acme,dc=com 
    cn: ActiveMQ.Advisory.TempTopic 
    objectClass: applicationProcess 
    objectClass: top 
    description: Advisory topic about temporary topics 
    
    dn: cn=read,cn=ActiveMQ.Advisory.TempTopic,ou=Topic,ou=Destination,ou=Active MQ,ou=systems,dc=acme,dc=com 
    cn: read 
    member: cn=webapp 
    objectClass: groupOfNames 
    objectClass: top 
    
    dn: ou=Group,ou=ActiveMQ,ou=systems,dc=acme,dc=com 
    objectClass: organizationalUnit 
    objectClass: top 
    ou: Group 
    
    dn: cn=admin,ou=Group,ou=ActiveMQ,ou=systems,dc=acme,dc=com 
    cn: admin 
    member: uid=admin 
    objectClass: groupOfNames 
    objectClass: top 
    
    dn: cn=webapp,ou=Group,ou=ActiveMQ,ou=systems,dc=acme,dc=com 
    cn: webapp 
    member: uid=webapp 
    objectClass: groupOfNames 
    objectClass: top 
    
    dn: ou=User,ou=ActiveMQ,ou=systems,dc=acme,dc=com 
    objectClass: organizationalUnit 
    objectClass: top 
    ou: User 
    
    dn: uid=admin,ou=User,ou=ActiveMQ,ou=systems,dc=acme,dc=com 
    uid: admin 
    userPassword: {SSHA}j0NpveEO0YD5rgI5kY8OxSRiN5KQ/kE4 
    objectClass: account 
    objectClass: simpleSecurityObject 
    objectClass: top 
    
    dn: uid=webapp,ou=User,ou=ActiveMQ,ou=systems,dc=acme,dc=com 
    uid: webapp 
    userPassword: {SSHA}j0NpveEO0YD5rgI5kY8OxSRiN5KQ/kE4 
    objectClass: account 
    objectClass: simpleSecurityObject 
    objectClass: top
    ```
4. Start up ActiveMQ

5. Test it out

### Security and ActiveMQ Components

Along with the message broker, you can optionally execute several additional "components", such as Camel and/or the Web console. These components establish connections with the broker; therefore, if you have secured your broker (i.e., enabled authentication), you will have to configure these components in order to have them provide the required security credentials (username, password) when they connect to the broker.

#### Camel

You may have the following Camel context defined in your broker's XML configuration file.

```
<!-- ** Lets deploy some Enterprise Integration Patterns inside the ActiveMQ Message Broker ** For more details see ** ** http://activemq.apache.orgFeatures/enterprise-integration-patterns.md -->
<camelContext id="camel" xmlns="http://activemq.apache.org/camel/schema/spring">
  <package>org.foo.bar</package>
  <route> 
    <from uri="activemq:example.A"/>
    <to uri="activemq:example.B"/>
  </route>
</camelContext>
```

The above configuration is not set up to work within a secure environment.

If the application is running in an OSGi container, add the following line before the CamelContext definition:

```
<osgi:reference id="activemq" interface="org.apache.camel.Component" />
```

This allows any pre-configured instance of the ActiveMQComponent deployed in the container to take precedence on the default ActiveMQComponent.

That is, with the above configuration, Camel will establish a connection with ActiveMQ, but will not provide a username and password. Therefore, when ActiveMQ security is enabled, the above configuration results in a security exception. The exception will be thrown multiple times, because Camel will continue to retry the connection. If you're not using Camel, comment out the above XML code. If you are using Camel, add the following bean definition to your broker's XML configuration:

```
<!-- configure the camel activemq component to use the current broker -->
<bean id="activemq" class="org.apache.activemq.camel.component.ActiveMQComponent" >
  <property name="connectionFactory"> 
    <bean class="org.apache.activemq.ActiveMQConnectionFactory">
      <property name="brokerURL" value="vm://localhost?create=false&amp;waitForStart=10000" />
      <property name="userName" value="system"/> 
      <property name="password" value="manager"/>
    </bean>
  </property>
</bean>
```

With the above bean definition, Camel will pass the specified security credentials when it connects to the broker.

If the broker is running in an OSGi container, add the following line after the ActiveMQComponent bean definition:

```
<service ref="activemq" interface="org.apache.camel.Component"/>
```

#### Web Console

If you want to use the Web Console with a secured broker, you have to change `connectionFactory` bean in your `webapps/admin/WEB-INF/webconsole-embeded.xml` to something like this:

```
<bean id="connectionFactory" class="org.apache.activemq.ActiveMQConnectionFactory">
  <property name="brokerURL" value="vm://localhost"/>
  <property name="userName" value="system"/>
  <property name="password" value="manager"/>
</bean>
```

#### Default Credentials

Starting with version 5.3, all of the above configuration details are included in the default ActiveMQ configuration. Also, there is a central place where you can set credentials that these components will use to connect to the broker. Just set your desired username and password in the `conf/credentials.properties` file, which by default looks like this:
```
activemq.username=system activemq.password=manager
```

#### Encrypted Passwords

As of version 5.4.1 you can also use [Encrypted passwords](encrypted-passwords) with your broker

### Message level Authorization

It's also possible to authorize each single message using some content based authorization policy of your choosing. In comparison to the other security options described before, Message level Authorization requires a bit more than just some configuration. You have to start with creating a new maven project and add the [activemq-all](https://mvnrepository.com/artifact/org.apache.activemq/activemq-all) maven dependency (in the same version as your activemq installation) to the pom.xml of your new project.  
In the next step you have to create a new Java class and let it implement the org.apache.activemq.security.MessageAuthorizationPolicy interface. After that, simply add a method with signature:
```
public boolean isAllowedToConsume(ConnectionContext context, Message message){...}
```
to the new Java class. For usage of your own Message level Authorization policy, the Java class has to be packaged as jar and added to the /lib folder of ActiveMQ to make it available. In the last step, it has to be configured on the broker directly by using the * messageAuthorizationPolicy* property or add it to the XML as follows

```
<broker>
  ..
  <messageAuthorizationPolicy>
    <bean class="com.acme.MyMessageAuthorizationPolicy" xmlns=""/>
  </messageAuthorizationPolicy>
  ..
</broker>
```

Implementing your own custom Security Plugin
--------------------------------------------

All of the various security implementations are implemented as [Interceptors](interceptors) so its very easy to add your own custom implementation. Its probably easier to start with one of the [simple implementations](http://activemq.apache.org/components/classic/documentation/maven/apidocs/org/apache/activemq/security/package-summary.html) though if you are using JAAS you could derive from the [JAAS implementation](http://activemq.apache.org/components/classic/documentation/maven/apidocs/org/apache/activemq/jaas/package-summary.html).
