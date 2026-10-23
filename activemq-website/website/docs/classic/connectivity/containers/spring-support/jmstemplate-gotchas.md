---
title: "JmsTemplate Gotchas"
slug: /jmstemplate-gotchas
---


The thing to remember is JmsTemplate is designed for use in EJBs using the EJB containers JMS pooling abstraction. So every method will typically create a connection, session, producer or consumer, do something, then close them all down again. The idea being that this will use the J2EE containers pooling mechanism to pool the JMS resources under the covers. Without using a pooled JMS provider from the EJB container this is the worst possible way of working with JMS; since typically each create/close of a connection, producer/consumer results in a request-response with the JMS broker.

> User Story
> 
> We had a bug once in ActiveMQ where if you created 65535 MessageProducer instances within the space of a few seconds, we'd get an exception thrown in the broker. Its a kinda silly thing to do, to create that many producers in a small space of time (one for each message to be sent) - JMS is designed for resources like producers and consumers to be created up front and reused across many message exchanges. The bug was highlighted by a user using JmsTemplate without a JMS pool underneath. Well at least it helped find our bug which I'm glad to say is now fixed, but it didn't help their performance too much.

You should only use JmsTemplate with a pooled JMS provider. In J2EE 1.4 or later that typically means a JCA based JMS ConnectionFactory. If you are in an EJB then make sure you use your J2EE containers ConnectionFactory, never a plain-old-connection factory. If you are not inside an EJB Then you should use our [PooledConnectionFactory](http://activemq.apache.org/maven/activemq-core/apidocs/org/apache/activemq/pool/PooledConnectionFactory.html), then things will be nicely pooled. If you need to take part in XA transactions then look into our spring based [JCA Container](jca-container).

Another gotcha I've seen folks do is to create a MessageConsumer inside one of the SessionCallback methods then wonder why messages are not being received. After the SessionCallback is called, the session will be closed; which will close your consumers too. So if you want to create a MessageConsumer you should create a connection, session and consumer yourself.

Another problem I've seen is folks using the JmsTemplate.receive() method; as I've said above if you're not in an EJB using the J2EE containers ConnnectionFactory, a connection, session & consumer will be create and closed for each receive() method. This is all fine and well - if painfully slow unless you are using pooling - but be aware that this mechanism, without pooling, may well miss messages. If you are consuming on a topic which has messages sent with NON_PERSISTENT delivery mode then chances are you will miss messages, since each receive() call is a brand new consumer which will not receive any messages sent before the consumer existed. To receive messages efficiently you should use Spring's [MessageListenerContainer](http://static.springsource.org/spring/docs/2.5.x/reference/FAQ/jms.md#Community/FAQ/jms.mdp).

> New in 4.x
> 
> In ActiveMQ [4.x](changes-in-40) we have a new feature called [Subscription Recovery Policy](subscription-recovery-policy) which even in non-durable delivery mode allows a new consumer to go back in time and receive messages delivered within a window (a fixed amount of RAM or time window). e.g. if your broker dies you have 2 minutes to reconnect to another broker and not miss any messages - even without durable delivery.

### Recommendations for using JmsTemplate

*   Never use a regular ConnectionFactory unless you are totally sure it does all the pooling you need
*   If using in an EJB ensure you use the EJB containers ConnectionFactory
*   If you are only publishing messages and you are not in an EJB container and you are using ActiveMQ, then you can use either the [PooledConnectionFactory](http://activemq.codehaus.org/maven/apidocs/org/activemq/pool/PooledConnectionFactory.html) or the [Spring CachingConnectionFactory](http://static.springsource.org/spring/docs/2.5.x/api/org/springframework/jms/connection/CachingConnectionFactory.html)
*   If you are consuming messages its probably simpler & more efficient & less likely to lose messages to avoid using the receive() method and use Spring's [MessageListenerContainer](http://static.springsource.org/spring/docs/2.5.x/reference/FAQ/jms.md#Community/FAQ/jms.mdp) instead
*   See also [Using Spring to Send JMS Messages](http://bsnyderblog.blogspot.com/2010/02/using-spring-jmstemplate-to-send-/FAQ/jms) and [Using Spring to Receive JMS Messages](http://bsnyderblog.blogspot.com/2010/02/using-spring-to-receive-jms-messages.html)

