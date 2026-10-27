---
title: "How does JMS compare with email"
slug: /how-does-jms-compare-with-email
---

Certainly they both do similar things. The main difference between them is their history and design criteria.

JMS has a bunch of different qualities of service (durable v non-durable, queue v topic) designed for very high performance messaging with low latency together with reliability. Email is designed for connectivitiy on the web. So they have different design trade offs. e.g. its not a big deal to get multiple copies of an email (which I get fairly often if I loose a connection with an email server over POP for example). It would be a major disaster to get 2 copies of the "remove $1000 from my bank account" messages

The main reasons for using JMS is performance (throughput & latency), together with support for transactions & XA together with avoiding duplicates and message loss.

