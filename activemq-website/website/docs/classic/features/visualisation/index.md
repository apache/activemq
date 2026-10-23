---
title: "Visualisation"
slug: /visualisation
---


Inspired greatly by [Gregor and Erik's great talk at TSSJS 2006](http://www.enterpriseintegrationpatterns.com/talks.html) we've started to add some visualisation plugins into Apache ActiveMQ.

### Connection visualisation

There is a `<connectionDotFilePlugin/>` broker plugin which generates a [DOT file](http://www.graphviz.org/) such as [this example](pathname:///assets/ActiveMQConnections.dot) representing the connections, sessions, producers, consumers and the destinations they are using.

![](/img/ActiveMQConnections.png)

### Destination visualisation

There is a `<destinationDotFilePlugin/>` broker plugin which generates a [DOT file](http://www.graphviz.org/) such as [this example](pathname:///assets/ActiveMQDestinations.dot) whenever a destination is created or destroyed in the broker. You can then turn this dot file into a nicely rendered image such as the one below.

![](/img/ActiveMQDestinations.png)

A note for OS X users
---------------------

If you are using OS X then you can open the DOT file using [graphviz](http://www.pixelglow.com/graphviz/) which will then automatically re-render if it changes, so you end up with a real time graphical representation of the topic and queue hierarchies.

Also if you want to edit the layout a little before adding it to a wiki to distribute to your team, open the DOT file with [OmniGraffle](http://www.omnigroup.com/applications/omnigraffle/) then just edit away.

Visualisation Wish List
-----------------------

There are so many different things we could visualise; our only limitation is our imagination. So if you have any ideas for what we could visualise please add something to our [Visualisation Wish List](visualisation-wish-list)

External Open Source Tools
==========================

*   [hawtio](http://hawt.io/) is an open source HTML5 web application for working with Apache ActiveMQ and Apache Camel. It lets you create topics/queues, browse queues or send messages to destinations as well as visualising Camel routes and working with Camel endpoints along with viewing logs and metric charting etc.
*   [Fuse IDE](https://github.com/fusesource/fuseide) is an open source set of eclipse plugins for working with Apache ActiveMQ and Apache Camel. Its got tooling for runtime browsing of queues, creating topics/queues. You can drag and drop messages to/from ActiveMQ destinations and Camel endpoints

