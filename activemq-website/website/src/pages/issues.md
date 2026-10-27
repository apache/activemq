---
title: Reporting Issues
---

# Reporting Issues

If you find a bug please raise a new issue in the appropriate tracker. If you're not quite sure you're hitting a bug consider [contacting us](contact) to get some clarity.

Whenever you are reporting a problem please be sure to include as much information as possible; the more we know the more chance of a quicker resolution. Here's a few items to consider:

* Which version of the component are you using on which Java platform?
* Any particular application/web server or container being used? If so what version?
* Stack traces generally really help! If in doubt include the whole thing. Often exceptions get wrapped in other exceptions and the exception right near the bottom explains the actual error, not the first few lines at the top.
* Log output can be useful too. Sometimes enabling `DEBUG` logging can help.
* Your code & configuration files are often useful.

We can help you much quicker if you try the following:

* Provide us with a JUnit test case that demonstrates your issue. For example, if you think you've found a bug can you create a test case to demonstrate the bug?
* [Send a PR](contributing) to fix the bug!
* For memory leaks or performance related issues, if you can run a profiler on your test case and attach the output as a file (or zipped file if it's huge) to the issue we can normally fix things much faster. e.g. you could run [jmap](https://docs.oracle.com/javase/7/docs/technotes/tools/share/jmap.html)/[jhat](https://docs.oracle.com/javase/7/docs/technotes/tools/share/jhat.html), JProfiler or YourKit on your code and send us the output. To find memory leaks it's quicker to resolve if you can tell us what classes are taking up all of the RAM; we can normally figure out what's wrong from that.

|Component|Issue Tracker|
|---|---|
|ActiveMQ|[https://github.com/apache/activemq/issues](https://github.com/apache/activemq/issues)|
|NMS|[https://issues.apache.org/jira/projects/AMQNET](https://issues.apache.org/jira/projects/AMQNET)|
|CMS|[https://issues.apache.org/jira/projects/AMQCPP](https://issues.apache.org/jira/projects/AMQCPP)|

#### Using the issue tracker

If you're not quite sure you're hitting an actual bug or you're not sure if the feature you need is valid, etc. please [contact us on the users mailing list](contact) to get some clarity.

For issues related to the ActiveMQ broker, you can open a GitHub issue using your own GitHub account.

For issues related to NMS and CMS you need to [request an account](https://selfserve.apache.org/jira-account.html) before you can create an issue in the issue tracker. When requesting the account please **be specific** about the reason you need it. Please give a brief synopsis of the bug you're encountering or feature you need. Account requests with vague reasons **will be rejected**. If you want to be the "assignee" of an issue you need to be in the list of `activemq-developers` on the issue tracker. To join the group please mail the [dev mailing list](mailto:dev@activemq.apache.org) with the email address you used to register with the issue tracker and we'll add you to the group.

#### Contributing Your Own Fixes

Want to do some hacking on ActiveMQ? Try surfing to our issue trackers for open issues or features that need to be implemented. Take ownership of an issue and try fix it. See the article on [contributing](contributing) for more details on how to get the code.

If you can create a JUnit test case then your issue is more likely to be resolved quicker. Then we can add your issue to the test suite and then we'll know when its really fixed and we can ensure that the problem **stays fixed** in future releases.

#### ActiveMQ Issues Mailing List

This mailing list is automatically notified when JIRA issues are created and updated. You can also watch the [GitHub repository](https://github.com/apache/activemq/) for ActiveMQ updates.

|  |  |
|---|---|
|Subscribe|[issues-subscribe@activemq.apache.org](mailto:issues-subscribe@activemq.apache.org)|
|Unsubscribe|[issues-unsubscribe@activemq.apache.org](mailto:issues-unsubscribe@activemq.apache.org)|
|Web Interface|[https://lists.apache.org/list.html?issues@activemq.apache.org](https://lists.apache.org/list.html?issues@activemq.apache.org)|
|Apache Archive|[http://mail-archives.apache.org/mod_mbox/activemq-issues/](http://mail-archives.apache.org/mod_mbox/activemq-issues/)|
|Mark Mail Archive|[http://activemq.markmail.org/search/list:org.apache.activemq.issues](http://activemq.markmail.org/search/list:org.apache.activemq.issues)|

#### Artemis

:::warning
For Artemis, consult the [Apache Artemis](https://artemis.apache.org) website.
:::
