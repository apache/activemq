---
title: "Should I deploy the broker inside my JVM or AppServer"
slug: /should-i-deploy-the-broker-inside-my-jvm-or-appserver
---

You can deploy the ActiveMQ Broker inside your JVM or Application Server. Whether you do or not has pros and cons depending on how many JVMs you have and what your backup & recovery policy is.

### Advantages of embedding the broker

*   embedding a broker means you can use the VM transport which avoids the use of sockets and serialization. Instead ActiveMQ can pass around messages by value.
    *   the slight exception to this is ObjectMessage; the JMS specification says you must serialize the body of the ObjectMessage whenever you send it. However you can [disable this feature](how-should-i-use-the-vm-transport) if you want really high performance when using VM transport
*   its only 1 single deployment unit/JVM rather than 2 coupled processes.

### Disadvantages of embedding the broker

If you have lots of JVMs (say 100s of them) and have very stringent requirements; you don't ever want to loose a message and so you want to backup and manage the persistent ActiveMQ database (and/or files) very carefully.

Having an embdded broker inside each JVM could result in large number of sets of files to manage. So it may be simpler to separate the two out, so that your 100s of JVMs talk to a relatively small cluster of brokers (say 2-10 of them) so that you can avoid worrying about persistent state in each JVM and just focus on backing up and managing the files/databases used by the brokers.

Also its typically easier to run and manage ActiveMQ brokers when they are separate; you can have some ClassLoader issues when trying to embed ActiveMQ inside an application server. e.g. you can sometimes get [ClassPath Errors](javalangnosuchmethoderror)

### See

*   [How do I embed a Broker inside a Connection](how-do-i-embed-a-broker-inside-a-connection)
*   [Run Broker](run-broker)

