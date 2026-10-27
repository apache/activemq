---
title: "How do I use SSL"
slug: /how-do-i-use-ssl
---

### Setting up the Key and Trust Stores

Also see [Tomcat's SSL instructions](http://jakarta.apache.org/tomcat/tomcat-5.5-doc/ssl-howto.html) for more info. The following was provided by Colin Kilburn. Thanks Colin!

> ActiveMQ uses dummy credentials by default
> 
> ActiveMQ includes key and trust stores that reference a dummy self signed cert. When you create a broker certificate and stores for your installation, either overwrite the values in the conf directory or delete the existing dummy key and trust stores so they cannot interfere) 

1.  Using keytool, create a certificate for the broker:
    ```
    keytool -genkey -alias broker -keyalg RSA -keystore broker.ks
    ```
2.  Export the broker's certificate so it can be shared with clients:
    ```
    keytool -export -alias broker -keystore broker.ks -file broker_cert
    ```
3.  Create a certificate/keystore for the client:
    ```
    keytool -genkey -alias client -keyalg RSA -keystore client.ks
    ```
4.  Create a truststore for the client, and import the broker's
    certificate. This establishes that the client "trusts" the broker:
    ```
    keytool -import -alias broker -keystore client.ts -file broker_cert
    ```

### Starting the Broker

#### Using the javax.net.ssl.* System Properties

Before starting the broker's VM set the ACTIVEMQ_SSL_OPTS environment variable so that it knows to use the broker keystore. (note that in previous versions of ActiveMQ this property was called SSL_OPTS in some scripts. As of v5.12.0 all scripts use ACTIVEMQ_SSL_OPTS)

```
export ACTIVEMQ_SSL_OPTS = -Djavax.net.ssl.keyStore=/path/to/broker.ks -Djavax.net.ssl.keyStorePassword=password
```

#### Using Spring to configure SSL for a Broker instance

Sometimes the use of javax.net.ssl.* system properties is not appropriate as they effect all SSL users in a JVM. ActiveMQ 5.2.x adds an element to the that allows a broker specific set of SSL properties to be configured.

The SslContext [test case](https://github.com/apache/activemq/tree/main/activemq-unit-tests/src/test/java/org/apache/activemq/transport/tcp/SslContextBrokerServiceTest.java) validates starting an SSL transport listener using the configuration specified in the broker Xbean. The SslContext element is added to the broker as follows:

The SslContext is used to configure the [SslTransportFactory](https://github.com/apache/activemq/tree/main/activemq-client/src/main/java/org/apache/activemq/transport/tcp/SslTransportFactory.java) for that broker. Full details of the configuration options available can be seen in the [schema definition](http://activemq.apache.org/schema/core/activemq-core-5.2-SNAPSHOT.xsd) or in the accessors of [org.apache.activemq.spring.SpringSslContext](https://github.com/apache/activemq/tree/main/activemq-spring/src/main/java/org/apache/activemq/spring/SpringSslContext.java)

### Starting the Client

When starting the client's VM, specify the following system properties:
```
javax.net.ssl.keyStore=/path/to/client.ks
javax.net.ssl.keyStorePassword=password
javax.net.ssl.trustStore=/path/to/client.ts
```

In Linux, do not use absolute path to keystore. By default, keytool uses `~/.keystore`, but in some setups passing `-Djavax.net.ssl.keyStore=/home/account/.keystore` to Java VM does not work. This is not specific to ActiveMQ but good to keep in mind anyway.

### Client certificates

If you want to verify client certificates, you need to take a few extra
steps:

1.  Export the client's certificate so it can be shared with broker:
    ```
    keytool -export -alias client -keystore client.ks -file client_cert
    ```
2.  Create a truststore for the broker, and import the client's certificate. This establishes that the broker "trusts" the client:
    ```
    keytool -import -alias client -keystore broker.ts -file client_cert
    ```
3.  Add
    ```
    -Djavax.net.ssl.trustStore=/path/to/broker.ts
    ```
    to `ACTIVEMQ_SSL_OPTS`

4.  Instruct ActiveMQ to require client authentication by setting the following in activemq.xml:

### Certificate revocation

Starting with version **5.12**, you can define certificate revocation list (CRL) path on ssl context, so that invalid certificates can revoked

```
<sslContext>
    <sslContext keyStore="org/apache/activemq/security/broker1.ks"
                keyStorePassword="password"
                trustStore="org/apache/activemq/security/activemq-revoke.jks"
                trustStorePassword="password"
                crlPath="org/apache/activemq/security/activemq-revoke.crl"/>
</sslContext>
```

This list is static and loaded on broker startup.

Starting with version **5.14.0**, you can also enable more advanced Online Certificate Status Protocol (OCSP) protocol. For that you need to configure a location for the `java.security` configuration extension by setting appropriate system properties (in `${ACTIVEMQ_HOME}/bin/env`) like

```
ACTIVEMQ_SSL_OPTS="-Djava.security.properties=$ACTIVEMQ_CONF/java.security"
```

Then you need to configure OCSP responder properties in `java.security` file like
```
ocsp.enable=true ocsp.responderURL=<http://ocsp.example.net:80>
```

A demo of the broker configuration working with OCSP responder can be found at <https://github.com/dejanb/sslib>

### Working Around Java 7 SSL Bugs

As noted by issue AMQ-5970, it seems some versions of Java 7 have problems with SSL sessions that need to use the Diffie-Hellman cypher suite. If you run into this issue, just copy the Bouncy Castle bcprov-jdk15on-148.jar to ActiveMQ's lib directory and restart your broker.

### Useful links

These links might also help

-   [Sun's JSSE guide](http://java.sun.com/j2se/1.4.2/docs/guide/security/jsse/JSSERefGuide.html#CreateKeystore)
-   [Thawte SSL Troubleshooting Tips](https://search.thawte.com/support/ssl-digital-certificates/index?page=content&id=SO10061)
