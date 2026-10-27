---
title: "What are administered objects"
slug: /what-are-administered-objects
---

What are administered objects?
------------------------------

Administered objects refers to objects that are configured in [JNDI](jndi-support) and then accessed by a JMS client. So they are simply client-side objects typically either a ConnectionFactory or a Destination (such as a Queue or Topic).

Note that administered objects are only used for JNDI. JNDI can then be used as a level of indirection between the JNDI API and the concrete API of the JMS provider. So looking up objects in JNDI avoids you having a runtime dependency on ActiveMQ. Given that we are only talking about one ConnectionFactory object and a few Destination objects, this is not a huge big deal though

Often folks get very confused with JNDI. e.g. in RMI / EJB scenarios JNDI provides client side proxies; this is not the case with JMS, as the JMS client is the client side proxy to the broker.

An alternative approach to creating administered objects in JNDI is to just use the [Spring Support](spring-support) and let dependency injection be an alternative to JNDI.

### See Also

*   [How do I create new destinations](how-do-i-create-new-destinations)
*   [How do I embed a Broker inside a Connection](how-do-i-embed-a-broker-inside-a-connection)
*   [How does ConnectionFactory relate to the Broker](how-does-connectionfactory-relate-to-the-broker)

