---
title: "How can I monitor ActiveMQ"
slug: /how-can-i-monitor-activemq-classic
---


How can I monitor ActiveMQ
----------------------------------

ActiveMQ comes bundled with [Jolokia](http://www.jolokia.org/) which provides a RESTful interface to ActiveMQ's JMX capabilities. An open source third party tool that connects via Jolokia is [hawt.io](http://hawt.io/), an HTML-5 web console with an ActiveMQ plugin that is at least worth being familiar with as ActiveMQ and hawt.io share developers.

In ActiveMQ you can monitor the broker to see what destinations are being used, their activity along with connections and subscriptions using the following tools

*   [JMX](jmx) and a JMX console such as [jConsole](http://java.sun.com/developer/technicalArticles/J2SE/jconsole.html)
*   The [Web Console](web-console)
*   the [Advisory Message](advisory-message) feature (using JMS messages to monitor the system)
*   The [Command Agent](command-agent); ActiveMQ.Agent topic that you query for status
*   The [Visualisation](visualisation) plug-in
*   The [Statistics](statisticsplugin) plug-in

### Third Party Tools

*   [ActiveMQ Monitor (AMon)](http://www.ttmsolutions.com/Transactional_Software_Solutions/Active_Monitor_AMon.php)
*   [Apache ActiveMQBrowser](http://sourceforge.net/projects/activemqbrowser/)
*   [FuseHQ](http://fusesource.com/products/fuse-hq/) (based on Hyperic HQ Enterprise)
*   [Geronimo Administration Console](https://cwiki.apache.org/GMOxDOC11/geronimo-administration-console.html#GeronimoAdministrationConsole-JMSServer) (JMS Resources)
*   [Hyperic HQ](http://www.hyperic.com/products/open-source-systems-monitoring) and [Hyperic HQ Enterprise](http://www.hyperic.com/products/enterprise-systems-monitoring)
*   [iTKO LISA Test](http://www.itko.com/products/jms.jsp)
*   [jmxtrans](https://github.com/jmxtrans/jmxtrans) which can be used to continuously query ActiveMQ via JMX and write to a chosen output. For instance, write to a graphite database file which can then be used to graph trends over time and be queried by Nagios to alarm should your thresholds be exceeded.
*   [Media Driver Integrated Console](https://mediadriver.com/software/) can manage and monitor ActiveMQ, IBM MQ and Tibco EMS
*   [New Relic ActiveMQ Integration](https://newrelic.com/instant-observability/activemq) Get real-time Apache ActiveMQ performance metrics with New Relic APM.

### See Also

*   [How can I monitor the connection with the broker](how-can-i-monitor-the-connection-with-the-broker)

