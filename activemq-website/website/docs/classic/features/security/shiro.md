---
title: "Shiro"
description: "Secure ActiveMQ Classic with the Apache Shiro plugin: ini users and roles, hashed passwords, a custom Environment or SecurityManager, and the filter design."
slug: /shiro
---


ActiveMQ 5.10 and later provides a fully customizable security experience using [Apache Shiro](http://shiro.apache.org).

The ActiveMQ Shiro plugin can secure the ActiveMQ broker, from authenticating transport connections to authorizing behavior with topics and queues and everything in between.

Quickstart
----------

The fastest/simplest way to enable the ShiroPlugin is to define it as a Spring bean in the `broker` `plugins` section and embed [Shiro ini configuration](http://shiro.apache.orgCommunity/FAQ/configuration):
```
<beans xmlns="http://www.springframework.org/schema/beans"
       xmlns:amq="http://activemq.apache.org/schema/core"
       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
       xsi:schemaLocation="
       http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans-2.0.xsd
       http://activemq.apache.org/schema/core http://activemq.apache.org/schema/core/activemq-core.xsd">

    <broker xmlns="http://activemq.apache.org/schema/core" ... other attributes here ...>
        <plugins>
            <bean id="shiroPlugin" class="org.apache.activemq.shiro.ShiroPlugin" xmlns="http://www.springframework.org/schema/beans">
                <property name="iniConfig"><value>

                [main]
                # Shiro object graph configuration here if desired/necessary

                [users]
                # users section format:
                #
                # username = password [, assignedRole1, assignedRole2, ..., assignedRoleN]
                #
                # for example:
                #
                # scott = tiger, advisory, users, administrators
                #
                # Roles and permissions assigned to roles are defined in the [roles] section
                # below. By transitive association, any user assigned a role is granted the
                # role's permissions.
                
                # ActiveMQ System User
                # needed for in-VM/local connections when authentication is enabled:
                system = manager, system
                
                # Other users here.  You should almost always add the \`advisory\` role for each
                # user to make your life easy!  See the [roles] comments below for more info.
                # jsmith = jsmithsPassword, advisory
                # djones = djonesPassword, advisory, ...
                # etc.

                [roles]
                # roles section format:
                #
                # roleName = wildcardPermission1, wildcardPermission2, ..., wildcardPermissionN
                #
                # The 'system' role is assigned all permissions (*).  Be careful when assigning
                # this to actual users other than then system user:
                system = *

                # Full access rights should generally be given to the ActiveMQ.Advisory.*
                # destinations because by default an ActiveMQConnection uses advisory topics to
                # get early knowledge of temp destination creation and deletion. For more info:
                #
                #   http://activemq.apache.org/Features/security.md
                #
                # So we create an 'advisory' role here with a wildcard/catch-all permissions
                # for all advisory topics.  To make your life easy, ensure you assign this to
                # any/all users in the [users] section above, e.g.
                #
                #   jsmith = jsmithsPassword, advisory, ...

                advisory = topic:ActiveMQ.Advisory*

                </value></property>
            </bean>
        </plugins>
    </broker>
</beans>
```
This config assumes you have a simple/small set of static users that access your ActiveMQ broker. We'll cover enabling more advanced user repositories later.

#### Encrypted Passwords

The above example uses plaintext passwords, which is simple to set up and easy to use for testing, but not really secure. Most production deployments will likely want to use encrypted passwords. For example:
```
<bean id="shiroPlugin" class="org.apache.activemq.shiro.ShiroPlugin" xmlns="http://www.springframework.org/schema/beans">
    <!-- enabled by default.  To disable, uncomment:
    <property name="iniConfig"><value>

    [main]
    # Shiro object graph configuration here
    passwordMatcher = org.apache.shiro.authc.credential.PasswordMatcher
    iniRealm.credentialsMatcher = $passwordMatcher
 
    [users]
    scott = $shiro1$SHA-256$500000$eWpVX2tGX7WCP2J+jMCNqw==$it/NRclMOHrfOvhAEFZ0mxIZRdbcfqIBdwdwdDXW2dM=, advisory
    system = $shiro1$SHA-256$500000$eUyGwMGr9GYzB/gg/MoNgw==$WGc0yWFWv8+hLqjzVLgW7Hat2FQTywDXBl5izpqaLSY=, system

    [roles]
    system = *
    advisory = topic:ActiveMQ.Advisory*
    </value></property>
</bean>
```
As you can see, two things are different than the simpler/default configuration:

1.  The `[main]` section configured a `PasswordMatcher` on the implicit `iniRealm`. This indicates that all `.ini`-configured users are expected to have proper hashed/secure passwords.
2.  The `[users]` lines now have hash values in the `password` location instead of plaintext values.

To get the hashed password text values, you will want to [Download Shiro's Command Line Hasher](http://search.maven.org/remotecontent?filepath=org/apache/shiro/tools/shiro-tools-hasher/1.2.2/shiro-tools-hasher-1.2.2-cli.jar) from Maven Central. Once downloaded, you can use it to create secure password hashes that you can safely copy-and-paste in to the `[users]` section:

$ java -jar shiro-tools-hasher-X.X.X-cli.jar -p

It will then ask you to enter the password and then confirm it:
```
Password to hash:
Password to hash (confirm):
```
When this command executes, it will print out the securely-salted-iterated-and-hashed password. For example:
```
$shiro1$SHA-256$500000$eWpVX2tGX7WCP2J+jMCNqw==$it/NRclMOHrfOvhAEFZ0mxIZRdbcfqIBdwdwdDXW2dM=
```
Take this value and place it as the password in the user definition line (followed by any desired roles, such as the `advisory` role). For example:
```
[users]
scott = $shiro1$SHA-256$500000$eWpVX2tGX7WCP2J+jMCNqw==$it/NRclMOHrfOvhAEFZ0mxIZRdbcfqIBdwdwdDXW2dM=, advisory
system = $shiro1$SHA-256$500000$eUyGwMGr9GYzB/gg/MoNgw==$WGc0yWFWv8+hLqjzVLgW7Hat2FQTywDXBl5izpqaLSY=, system
```

Configuration
-------------

The ActiveMQ Shiro plugin can be configured in a number of ways. For example, with Java:
```
BrokerService brokerService = new BrokerService();

ShiroPlugin shiroPlugin = new ShiroPlugin();
//configure shiroPlugin via getters/setters here

broker.setPlugins(new BrokerPlugin[]{shiroPlugin});
//continue configuring the brokerService as necessary ...
```
Or, if using traditional ActiveMQ xml, as a Spring bean in the `broker` `plugins` section. For example:
```
<beans xmlns="http://www.springframework.org/schema/beans"
       xmlns:amq="http://activemq.apache.org/schema/core"
       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
       xsi:schemaLocation="
       http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans-2.0.xsd
       http://activemq.apache.org/schema/core http://activemq.apache.org/schema/core/activemq-core.xsd">

    <broker xmlns="http://activemq.apache.org/schema/core" ... other attributes here ...>

        <plugins>
    
            <bean id="shiroPlugin" class="org.apache.activemq.shiro.ShiroPlugin" xmlns="http://www.springframework.org/schema/beans">
                <!-- Config properties via getters/setters as necessary: -->
            </bean>

        </plugins>

    </broker>
</beans>
```
The remaining configuration examples on this page will be shown as bean XML, but know that the same configuration can be done in Java as standard JavaBeans-compatible getter and setter methods.

### Enabling/Disabling

You can enable or disable the ShiroPlugin entirely without having to remove it from your configuration. This is convenient when testing, or when you want to enable or disable it based on a configuration parameter at startup.
```
<bean id="shiroPlugin" class="org.apache.activemq.shiro.ShiroPlugin" xmlns="http://www.springframework.org/schema/beans">
    <!-- enabled by default.  To disable, uncomment:
    <property name="enabled" value="false"/> -->
</bean>
```
A nice technique is to use Spring's [PropertySourcesPlaceholderConfigurer](http://static.springsource.org/spring/docs/3.2.x/javadoc-api/org/springframework/context/support/PropertySourcesPlaceholderConfigurer.html) and placeholder tokens (set `shiro.enabled = true` in one of your placeholder property files):
```
<beans ...>

    <bean class="org.springframework.context.support.PropertySourcesPlaceholderConfigurer">
       ...
    </bean>

    <broker ...>
        <plugins ...>

            <bean id="shiroPlugin" class="org.apache.activemq.shiro.ShiroPlugin" xmlns="http://www.springframework.org/schema/beans">
                <property name="enabled" value="${shiro.enabled}"/>
            </bean>
 
        </plugins>
    </broker>
</beans>
```
This allows you to enable or disable the Shiro plugin by simply setting a property in a `.properties` file without having to change your XML config.

### Shiro Environment

The `shiroPlugin` requires a Shiro [Environment](http://shiro.apache.org/static/current/apidocs/org/apache/shiro/env/Environment.html) to function. You must either configure the plugin with:

*   an `Environment` instance (or a Shiro `SecurityManager` instance) that you instantiate and configure elsewhere - e.g. in Java code or elsewhere in the Spring XML config, or
*   specify some Shiro [.ini configuration](http://shiro.apache.orgCommunity/FAQ/configuration), either as a direct String, an [Ini](http://shiro.apache.org/static/current/apidocs/org/apache/shiro/config/Ini.html) instance, or a [resource path](http://shiro.apache.org/static/current/apidocs/org/apache/shiro/io/ResourceUtils.html#getInputStreamForPath(java.lang.String)) where your `shiro.ini` file is located. The plugin will load the ini config and create an `Environment` automatically.

#### Custom Environment

A Shiro `Environment` object contains everything that Shiro needs to operate, and this encapsulates the Shiro `SecurityManager` as well. If you want to construct and configure an Environment instance yourself:
```
<beans ...>
    <broker ...>
        <plugins>
        
            <bean id="shiroPlugin" class="org.apache.activemq.shiro.ShiroPlugin" xmlns="http://www.springframework.org/schema/beans">
                <property name="environment" ref="shiroEnvironment"/>
            </bean>
 
        </plugins>
    </broker>

    <bean id="shiroEnvironment" class="..">
        ... config here ...
    </bean>
    <bean class="org.apache.shiro.spring.LifecycleBeanPostProcessor"/>

</beans>
```

#### Custom SecurityManager

Instead of configuring an `Environment` instance, you can construct a `SecurityManager` instead:
```
<beans ...>
    <broker ...>
        <plugins>
        
            <bean id="shiroPlugin" class="org.apache.activemq.shiro.ShiroPlugin" xmlns="http://www.springframework.org/schema/beans">
                <property name="securityManager" ref="shiroSecurityManager"/>
            </bean>
 
        </plugins>
    </broker>

    <bean id="shiroSecurityManager" class="org.apache.shiro.mgt.DefaultSecurityManager">
        <property name="realms">
            <list>
                <bean id="myRealm" class="...">
                    ...
                </bean>
                ... maybe more Realm beans ...
            </list>
        </property>
    </bean>
    <bean class="org.apache.shiro.spring.LifecycleBeanPostProcessor"/>

</beans>
```
If specifying a `SecurityManager` instead of the `Environment` property, an `Environment` will be created automatically that wraps the configured `SecurityManager`.

#### shiro.ini File

If you don't want to construct a `SecurityManager` or `Environment` in code or xml, you can easily specify a [shiro.ini](http://shiro.apache.orgCommunity/FAQ/configuration) file instead and an Environment/SecurityManager will automatically be created based on that:
```
<beans ...>
    <broker ...>
        <plugins>

            <bean id="shiroPlugin" class="org.apache.activemq.shiro.ShiroPlugin" xmlns="http://www.springframework.org/schema/beans">
                <property name="iniResourcePath" value="classpath:myshiro.ini"/>
            </bean>

        </plugins>
    </broker>
</beans>
```
This allows you to keep your Shiro config separate from your ActiveMQ broker configuration if you prefer.

#### shiro.ini Embedded

If you want to use ini configuration and you would prefer to have all configuration in one place, you can embed the ini config instead:
```
<beans ...>
    <broker ...>
        <plugins ...>
        
            <bean id="shiroPlugin" class="org.apache.activemq.shiro.ShiroPlugin" xmlns="http://www.springframework.org/schema/beans">
                <property name="iniConfig">
                    <value>
                    [main]

                    # Shiro object graph configuration here if desired/necessary

                    [users]
                    system = manager, system

                    [roles]
                    system = *
                    advisory = topic:ActiveMQ.Advisory*
                    </value>
                </property>
            </bean>

        </plugins>
    </broker>
</beans>
```

Design
------

The Shiro plugin is a [BrokerPlugin](http://activemq.apache.org/components/classic/documentation/maven/apidocs/org/apache/activemq/broker/BrokerPlugin.html) that inserts 3 [BrokerFilter](http://activemq.apache.org/components/classic/documentation/maven/apidocs/org/apache/activemq/broker/BrokerFilter.html)s in the broker filter chain: the `SubjectFilter`, the `AuthenticationFilter` and the `AuthorizationFilter`

**SubjectFilter**

The `SubjectFilter` exists before all other Shiro-related broker filters in the broker filter chain. It constructs a Shiro [Subject](http://shiro.apache.org/subject.html) instance reflecting the broker client and ensures the `Subject` instance is available for all downstream broker filters that may need to use the `Subject` to perform security operations.

**AuthenticationFilter**

The `AuthenticationFilter` exists immediately after the `SubjectFilter` in the broker filter chain. It ensures that the broker client `Subject` is authenticated if necessary before allowing the chain to continue. If authentication is required and the `Subject` is not authenticated, the broker filter chain will not be executed, ensuring only verified identities may perform further behavior.

**AuthorizationFilter**

The `AuthorizationFilter` exists immediately after the `AuthenticationFilter` in the broker filter chain. It ensures that the `Subject` associated with the filter chain is authorized (permitted) to perform the action being attempted before allowing the action to execute.

For example, it would ensure that the `Subject` is allowed to send a message to a particular topic before allowing the send operation to execute. If authorization is enabled and the `Subject` is not authorized to perform the desired action, the broker filter chain will not be executed.

SubjectFilter
-------------

The ShiroPlugin installs and executes the `SubjectFilter` before all other Shiro-related broker filters in the broker filter chain. The `SubjectFilter` constructs a Shiro [Subject](http://shiro.apache.org/subject.html) instance reflecting the broker client and ensures the `Subject` instance is available for all downstream broker filters that may need to use the `Subject` to perform security operations.

The `SubjectFilter` is mostly a 'behind the scenes' component of the SubjectFilter, but it does offer some customization for advanced use cases:

*   the ability to customize exactly how broker clients' `Subject` instances are created via a `ConnectionSubjectFactory` and
*   the ability to customize how the ActiveMQ ConnectionContext's [SecurityContext](http://activemq.apache.org/components/classic/documentation/maven/apidocs/org/apache/activemq/security/SecurityContext.html) is constructed.

Unless you're deeply familiar with ActiveMQ's security model, you can safely skip to **Authentication** below.

### ConnectionSubjectFactory

A `ConnectionSubjectFactory` creates a `Subject` instance that represents the broker client's identity. The `SubjectFilter`'s default instance is a `DefaultConnectionSubjectFactory`

Most `ConnectionSubjectFactory` implementations will simply use Shiro's `Subject.Builder` to create an anonymous Subject instance and let the downstream `AuthenticationFilter` authenticate the Subject based on any credentials associated with the connection. After authentication, the Subject will have an identity, and this is the expected flow for most connection clients.

However, if there is some other data associated with the connection that can be inspected to create a Subject instance beyond what the `DefaultConnectionSubjectFactory`, you can implement the `ConnectionSubjectFactory` interface and plug it in to the `SubjectFilter`:
```
<bean id="shiroPlugin" class="org.apache.activemq.shiro.ShiroPlugin" xmlns="http://www.springframework.org/schema/beans">
    <property name="subjectFilter.connectionSubjectFactory">
        <bean class="com.my.ConnectionSubjectFactory" .../>
    </property>
</bean>
```

### SecurityContextFactory

The ActiveMQ `ConnectionContext` associated with broker client connections utilizes a `SecurityContext` object. When the `SubjectFilter` executes, it needs to create a Shiro-specific `SecurityContext` and associate it with the `ConnectionContext` so the Subject may be accessed downstream for all subsequent security operations.

The `SubjectFilter` delegates `SecurityContext` creation to a `SecurityContextFactory` instance. The `DefaultSecurityContextFactory` implementation returns `SubjectSecurityContext` instances based on the connection's associated `Subject`. It should be an extremely rare thing to change, but if you must configure a custom `SecurityContextFactory`, you can do as follows:
```
<bean id="shiroPlugin" class="org.apache.activemq.shiro.ShiroPlugin" xmlns="http://www.springframework.org/schema/beans">
    <property name="subjectFilter.securityContextFactory">
        <bean class="com.my.SecurityContextFactory" .../>
    </property>
</bean>
```
Note however that much of the plugin's functionality and downstream filters expect created `SecurityContext` instances to be `SubjectSecurityContext` instances.

Authentication
--------------

The ShiroPlugin installs the `AuthenticationFilter` immediately after the `SubjectFilter` in the broker filter chain. The `AuthenticationFilter` ensures that the broker client `Subject` is authenticated if necessary before allowing the chain to continue. If authentication is required and the `Subject` is not authenticated, the broker filter chain will not be executed, ensuring only verified identities may perform further behavior.

WORK IN PROGRESS - STILL AUTHORING

