---
title: "RSS and Atom"
slug: /rss-and-atom
---


We have added support for the [REST](rest)ful browsing of message queues in the **activemq-web** module. To try out this feature try the [Web Samples](web-samples).

Browsing of queues is implemented by a servlet, **QueueBrowseServlet** which allows queues to be browsed using pluggable views. The current views supported are

View name|Description
---|---
simple|Renders a list of <message> elements with an id attribute
xml|Uses XStream to render the entire messages as XML
rss|Uses [Rome](https://rome.dev.java.net/) to render the messages as an RSS 0.9*, 1.0, 2.0 or Atom 0.3 feed. You can configure the type of feed using the **feedType**

Any properties on the view objects can be configured via reflection using query parameters. e.g. you can specify these common properties

Parameter|Description
---|---
contentType|Override the MIME content type of the view
maxMessages|The maximum number of messages to render

### Examples

*   [http://localhost:8080/queueBrowse/FOO/BAR?view=xml](http://localhost:8080/queueBrowse/FOO/BAR?view=xml) will browse the queue as XML
*   [http://localhost:8080/queueBrowse/FOO/BAR?view=rss&feedType=rss_2.0](http://localhost:8080/queueBrowse/FOO/BAR?view=rss&feedType=rss_2.0) will browse the FOO.BAR queue using RSS 2.0
*   [http://localhost:8080/queueBrowse/FOO/BAR?view=rss&feedType=atom_0.3](http://localhost:8080/queueBrowse/FOO/BAR?view=rss&feedType=atom_0.3) will browse the FOO.BAR queue using Atom

