---
title: "java.lang.OutOfMemory"
slug: /javalangoutofmemory
---

Ok, this is manageable. It is possible to configure just about all of the memory utilisation of ActiveMQ. The first thing to determine is what part of the system is running out of memory. Is it the JVM, the broker, the consumers or the producers?

This entry refers to ActiveMQ versions 5.1.x and greater.

#### Memory

##### JVM Memory

Try running the broker in a standalone JVM using `bin/activemq`. Note the default JVM heap size option that is passed to the Java executable by the script (the exact options may depend upon the JVM that you are using, the examples are for the Sun JVM).

*   -Xmx: If your OS has more available memory, consider increasing the total heap memory available to the broker JVM. Note that the JVM will require more memory than the -Xmx value. Thread stacks and the JMVs internal classes will consume additional memory.
*   -Xss: If you have [massive numbers of threads](javalangoutofmemory) in the Broker JVM, consider reducing the default JVM stack size of each thread with the -Xss option.

If you are running an embedded broker or in a third party container, ensure that the hosting JVM has appropriate values for the maximum heap and stack sizes.

##### Broker Memory

The memory that the broker is allowed to use is not determined by the amount of memory allocated to the JVM. Although the broker is constrained by the amount of memory given to the JVM, the broker manages its memory independently. That is, the broker does not just simply use up all of the memory in the JVM and then die with an OutOfMemory exception. This is where you need to understand the [systemUsage](producer-flow-control) memory limit and the per destination memory limit.

The memory in ActiveMQ works in a tiered fashion that flows from the JVM -> Broker -> broker features. E.g., the total amount of destination memory limits placed cannot exceed the memory limit of the broker.

#### Consumer

Along with message size, the [prefetch limit](what-is-the-prefetch-limit-for) is main reason a consumer will [run out of memory](what-is-the-prefetch-limit-for). Reducing the prefetch value will reduce the amount of messages queued/stored in memory on the Consumer.

#### Producer

Unless message size exceeds resource limits, a producer should not run out of memory. A producer may notice the effect of memory limit enforcement by the broker in the form of [blocking](my-producer-blocks).

#### Other

##### Spooling Messages to Disk

Fast dispatch of messages is only possible when messages are stored in memory. When consumers are slow or absent, memory can quickly become exhausted.  
The Broker (using [Message Cursors](message-cursors)) will spool non-persistent messages to disk when the default memory usage threshold for a destination is reached. This threshold value is specified to the Broker via the <memoryUsage> section of the <systemUsage> configuration in [activemq.xml](xml-configuration). This feature allows producers to continue sending messages when there are slow consumers without exhausting available memory or reverting to [producer flow control](producer-flow-control). In the case of multiple destinations, the combined default memory thresholds may be excessive and they may exceed available memory. In such a case it may make sense to reduce the memory usage 'limit' threshold at which messages are spooled to disk. An alternative option is to configure the 'precentUsage' rather than the absolute usage 'limit'. In this way, memory usage can be confined to a fixed percentage of available memory.

More specific per destination memoryUsage limits can be specified in [activemq.xml](xml-configuration) using [Per Destination Policies](per-destination-policies). Some further examples of <destinationPolicy> map entries can be found in the [Message Cursors](message-cursors) reference.

##### Number of Threads

By default, ActiveMQ uses a dedicated thread per destination. If there are large numbers of Destinations there will be a large number of threads and their associated memory resource usage. ActiveMQ can be configured to use a thread pool through the use of the system property: -Dorg.apache.activemq.UseDedicatedTaskRunner=false. This is currently specified in the activemq start script via ACTIVEMQ_OPTS. Using a thread pool can restrict the number of threads required by ActiveMQ and hence reduce memory usage.

##### Really Large Messages

When your message are really large such that you can only allow a few messages in memory at at time, the [Per Destination Policies](per-destination-policies) maxPageSize and lazyDispatch can help. maxPageSize controls the amount of messages that are paged into memory for dispatch while lazyDispatch augments that value using the prefetch capacity of the current consumer list. With a prefetch of 1, a single consumer and lazyDispatch, only one message at a time would be loaded into memory at a time.

##### Leaking JMS resources

This is the most obvious one for Consumers or Producers; repeatedly obtaining a Session or MessageProducer or MessageConsumer and not closing it. With a java.lang.OutOfMemory, verifying (again) that all not in use JMS resources are released, is worth the time. If you have multiple threads in your application, consider using the [PooledConnectionFactory](http://activemq.apache.org/maven/activemq-core/apidocs/org/apache/activemq/pool/PooledConnectionFactory.html) as this will allow JMS resource to be safely shared among threads that follow the pattern:
```
obtainJmsResource(); 

try 
{ 
    useJmsResource() 
} finally { 
    releaseJmsResource(); 
} 
```
If you are using ActiveMQ via [Spring Support](spring-support) or with JMSTemplates, be sure to check you are not falling for any of the [JmsTemplate Gotchas](jmstemplate-gotchas). It may also be worth recapping on [How do I use JMS efficiently](how-do-i-use-jms-efficiently).

