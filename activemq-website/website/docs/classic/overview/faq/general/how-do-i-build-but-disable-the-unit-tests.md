---
title: "How do I build but disable the unit tests"
slug: /how-do-i-build-but-disable-the-unit-tests
---

How do I build but disable the unit tests
-----------------------------------------

The test cases in ActiveMQ can take a very long time to run!

To disable this you can try the following
```
mvn install -Dmaven.test.skip=true
```