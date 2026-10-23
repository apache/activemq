---
title: "How do Message Groups compare to Selectors"
slug: /how-do-message-groups-compare-to-selectors
---

Selectors are just filters. Message Groups are a way of grouping messages together to the same consumer to partition your application or insure ordering is maintained.

Now you could implement message groups by hand, by having each consumer use its own selector for a specific header. e.g. if you had consumers A, B, C; you could use the values "A", "B", "C" as values of the JMSXGroupID header (or any other header name you like).

Then if consumer A used `JMXGroupID = 'A'` as a selector it would be the only consumer getting the A messages; ditto for B and C.

So that kinda simulates message groups a little. The downside is

*   you have to know the set of consumers that are running to know what values of A, B or C you can put on the header. So if you start up a new consumer D, you have to change the producer to now be aware of D. Wtih message groups you can use any string whatsoever - typically the string will come from your business such as a product code, a customer ID, a stock ticker name or something (or some combination of data such as IBM and NASDAQ and Tuesday).

*   if for whatever reason consumer B stops running, noone consumes any messages for B - you have to know to restart B manually. With message groups things auto-partition and load balance for you with immediate failover.

*   there's nothing to stop you accidentally creating 2 threads with the same selector - or 2 processes on the network starting up with the same selector and accidentally consuming from the same group of messages which completely breaks ordering; with message groups you don't have to worry about this - since it guarrentees that only 1 thread in your entire system will process messages from one group at once in order - irrespective how many consumers you start.

So in general; you can manually partition yourself using selectors. Message Groups however are a self-partitioning and auto-failvoer mechanism which also guarrentees that a single thread will process a specific message group at once, in order.

