---
title: "Integrating Apache ActiveMQ with Glassfish"
slug: /integrating-apache-activemq-classic-with-glassfish
---


General
=======

See this article for a description of how to connect Glassfish 3 to an ActiveMQ 5 broker, and consume messages using a Message Driven Bean: [http://geertschuring.wordpress.com/2012/04/20/how-to-connect-glassfish-3-to-activemq-5/](http://geertschuring.wordpress.com/2012/04/20/how-to-connect-glassfish-3-to-activemq-5/)

Glassfish 4.1
=============

1.  Add resource adapter  
    1.  Download the resource-adapter file suitable to your broker version: [http://repo1.maven.org/maven2/org/apache/activemq/activemq-rar/](http://repo1.maven.org/maven2/org/apache/activemq/activemq-rar/)
    2.  Deploy the  resource adapter with the name "activemq-rar"
        ```
        bin/asadmin deploy --type rar --name activemq-rar /tmp/activemq-rar-5.11.1.rar
        ```
    3.  Configure resource adapter (Failover Setup)
        ```
        bin/asadmin create-resource-adapter-config --property ServerUrl='failover:(tcp://broker-c1.foobar.local:61616,tcp://broker-c2.foobar.local:61616,tcp://broker-c3.foobar.local:61616)?initialReconnectDelay=2000&jms.useCompression=true':UserName='admin':Password='admin' activemq-rar
        ```
2.  Create connector connection pool
    ```
    bin/asadmin create-connector-connection-pool \
      --raname activemq-rar \
      --connectiondefinition javax.jms.ConnectionFactory \
      --ping true --isconnectvalidatereq true \
      jms/myConnectionPool
    ```
3.  Create a connector resource (JNDI-Mapping for connection pool)
    ```
    bin/asadmin create-connector-resource \
      --poolname jms/myConnectionPool \
      jms/myConnectionFactory
    ```
4.  Configure JNDI mapping for a queue
    ```
    bin/asadmin create-admin-object \
    	--raname activemq-rar \
      --restype javax.jms.Queue \
      --property PhysicalName=MY.MAGIC.OUT \
      jms/queue/MY.MAGIC.OUT
    ```
5.  Deploy your message driven beans and use the configured jndi names

