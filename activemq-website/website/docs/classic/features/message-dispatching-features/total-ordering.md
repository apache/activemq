---
title: "Total Ordering"
slug: /total-ordering
---


Sometimes it can be useful to ensure that every topic consumer sees messages arriving on the topic in exactly the same order. Normally the broker will guarantee the order of all messages sent by the same producer. However, owing to the broker's use of multiple threads and asynchronous processing, messages from different producers could arrive in different consumers in different orders.

For example, if we have producers **`P`** and **`Q`** sending messages such that at about the same time **`P`** sends **`P1`, `P2`**, **`P3`** and **`Q`** sends **`Q1`, `Q2`**. Therefore, two different consumers _could_ see messages arrive in the following order:

consumer1: P1 P2 Q1 P3 Q2
consumer2: P1 Q1 Q2 P2 P3

In this example each producer's messages are in self-relative order. However, the streams of messages across producers can get intermixed.

_Total Ordering_ of a destination in ActiveMQ ensures that each consumer will see the same total order on that topic. This has a performance cost, since greater synchronization is required. This can be useful, particularly when very fast optimistic transactions are required. With total ordering the messages would arrive like this:

consumer1: P1 P2 Q1 P3 Q2
consumer2: P1 P2 Q1 P3 Q2

Configuring Total Ordering
--------------------------

Enable the **`<strictOrderDispatchPolicy/>`** on the [Per Destination Policies](per-destination-policies). Here's an example

<destinationPolicy>
  <policyMap>
    <policyEntries>
      <policyEntry topic=">">
        <dispatchPolicy>
          <strictOrderDispatchPolicy/>
        </dispatchPolicy>
      </policyEntry>
    </policyEntries>
  </policyMap>
</destinationPolicy>

