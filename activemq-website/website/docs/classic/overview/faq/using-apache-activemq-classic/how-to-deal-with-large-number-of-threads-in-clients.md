---
title: "How to deal with large number of threads in clients"
slug: /how-to-deal-with-large-number-of-threads-in-clients
---

If you study thread allocation in ActiveMQ clients, you'll notice that by default there is one thread allocated by every session. This basically means that session will use its [ThreadPoolExecutor](http://docs.oracle.com/javase/6/docs/api/java/util/concurrent/ThreadPoolExecutor.html) to execute its tasks. Up until version 5.7 this executor was unbound which could lead to OOM problems in rare case where are a large number of busy sessions in the same JVM could cause uncontrollable spikes in thread creation.

In 5.7 we bounded this executor to a maximum of 1000 threads by default, which we believe should be enough for most use cases. In case of large number of busy sessions, each of them could end up using large number of threads and eventually OOM your application. There are couple of things you can do. The first obvious thing is to decrease the max thread limit a session can use, like this:
```
ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
connectionFactory.setMaxThreadPoolSize(10);
Connection conn = connectionFactory.createConnection();
conn.start();
```
On the other hand this can lead to the scenario where a single session exaust its max thread number. A default behavior of `ThreadPoolExecutor` in this case is to throw an exception, so you'll notice it. A workaround for this scenario is to provide a rejected task handler that will synchronize the execution with the calling thread, like this
```
ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
connectionFactory.setRejectedTaskHandler(new ThreadPoolExecutor.CallerRunsPolicy());
Connection conn = connectionFactory.createConnection();
conn.start();
```
This will prevent the `ThreadPoolExecutor` from throwing an exception when it reaches its bound. Instead it will execute the rejected task in the calling thread.

Finally, you can eliminate these threads completly, you can do that by setting `alwaysSessionAsync` property to false
```
ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
connectionFactory.setAlwaysSessionAsync(false);
Connection conn = connectionFactory.createConnection();
conn.start();
```
However you need to have in mind that this approach can affect performance of the whole system.

So there you go. In the unlikely scenario where your application has trouble with the number of threads created by JMS session, you can use one of the shown techniques to deal with it. As always there's no silver bullet for all scenarios, so measure and tune for your system.

