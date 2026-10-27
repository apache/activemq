---
title: "Virtual Threads"
description: "Enable virtual threads in ActiveMQ Classic 6.3.0+ on JDK 25 with the virtualThreadTaskRunner broker attribute, and see the tech preview's benefits and status."
slug: /virtual-threads
---

### Virtual Threads support 

ActiveMQ support for Virtual Threads is in Technology Preview stage. Virtual Thread support is implemented using multi-release jar, so no special changes to the distribution are required.

### Virtual Threads benefits

 * Performance on-par (or better) than reactive coding approaches
 * JDK-based option for peak concurrency performance
 * Automatically scales based on need and detected CPU cores
 * Less configuration tuning required for administrators
 * Easier to code extensions
 * Scales connection count
 * Scales destination count

### Configuration

To enable Virtual Threads in ActiveMQ 6.3.x (and higher)

1. Run Apache ActiveMQ 6.3.0 (or higher)
2. Run with Java JDK 25 (or newer)
3. Edit conf/activemq.xml

    <broker ... virtualThreadTaskRunner="true" .. >

4. Run ActiveMQ and confirm with the following log message:

    INFO | VirtualThreadExecutor initialized name:ActiveMQ BrokerService[localhost] Task

NOTE: The log message may be removed in the future

### ActiveMQ Virtual Thread Implementation Progress 

Virtual Thread support was released with ActiveMQ v6.3.0. User feedback is welcome! Please open a GitHub issue with questions and comments.

JIRA|Status|Target Version|Completed Version|Feature|Notes
---|---|---
[AMQ-9394](https://issues.apache.org/jira/browse/AMQ-9394) | PR [#1172](https://github.com/apache/activemq/pull/1172) | 6.2.0 | 6.3.0 | Task Runner Factory | Tech Preview support to allow Virtual Thread Executor Service

### Reference: Running with Virtual Threads

JDK 25 (or newer) should always be used when running Virtual Threads to avoid thread pinning.

Technical factors:

 * ActiveMQ currently (6.1.x) uses virtual thread-friendly locking for key data structures in the Queue class
 * ActiveMQ currently (6.1.x) uses virtual thread-unfriendly _synchronized_ code blocks in many areas in the client and broker code
 * JDK 23 is slated to address the thread pinning in _synchronized_ code blocks
 * JDK 25 is the next planned LTS release after JDK 21
 * Areas of ActiveMQ source code will need to be modernized to be more virtual thread friendly
 * New virtual-thread first implementations may be added using extension points in the broker, vs refactoring the existing stable components 

JDK thread pinning improvement:
ref:  https://mail.openjdk.org/pipermail/loom-dev/2024-May/006632.html
