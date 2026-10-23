---
title: "Old OSGi Integration"
slug: /old-osgi-integration
---


> **This article applies to versions of ActiveMQ prior to 5.4.0. If you use version 5.4.0 or newer proceed to the [following article](osgi-integration).**

Introduction
============

All ActiveMQ modules are packaged as OSGi bundles and can be used in any OSGi container. This article will provide more details on how to use ActiveMQ in [Apache Karaf](http://felix.apache.org/site/apache-felix-karaf.html), small OSGi based runtime. Apache Karaf was previously know as _ServiceMix kernel_, so informations found here are applicable to [Apache ServiceMix Enterprise Service Bus](http://servicemix.apache.org/home.html) as well.

ActiveMQ
========

ActiveMQ comes as a feature in ServiceMix (Karaf). So instead of installing all necessary bundles one by one, you can simply install it using a single command. But first of all, you need to setup ServiceMix features url.
```
smx@root:/> features/addUrl mvn:org.apache.servicemix/apache-servicemix/4.0.0/xml/features
```
Now you can install ActiveMQ like this
```
smx@root:/> features/install activemq
```
This command will install all necessary ActiveMQ bundles and if you type
```
smx@root:/> osgi/list
```
you should see something like this
```
[  50] [Active     ] [       ] [   60] geronimo-jta\_1.1\_spec (1.1.1)
[  51] [Active     ] [       ] [   60] geronimo-j2ee-connector\_1.5\_spec (2.0.0)
[  52] [Active     ] [       ] [   60] Apache ServiceMix Bundles: howl-1.0.1-1 (1.0.1.1_1)
[  53] [Active     ] [       ] [   60] Geronimo TxManager :: Transaction (2.2.0.r634076)
[  54] [Active     ] [       ] [   60] Spring Transaction (2.5.6)
[  55] [Active     ] [Started] [   60] Apache ServiceMix Transaction (1.0.0)
[  56] [Active     ] [       ] [   60] Geronimo TxManager :: Connector (2.2.0.r634076)
[  57] [Active     ] [       ] [   60] geronimo-jms\_1.1\_spec (1.1.1)
[  58] [Active     ] [       ] [   60] Apache ServiceMix Bundles: jencks-2.1 (2.1.0.1)
[  59] [Active     ] [       ] [   60] geronimo-j2ee-management\_1.1\_spec (1.0.1)
[  60] [Active     ] [       ] [   60] Apache Commons Pool Bundle (1.4)
[  61] [Active     ] [       ] [   60] xbean-spring (3.5)
[  62] [Active     ] [       ] [   60] activemq-core (5.2.0)
[  63] [Active     ] [       ] [   60] activemq-ra (5.2.0)
[  64] [Active     ] [       ] [   60] activemq-console (5.2.0)
[  65] [Active     ] [       ] [   60] activemq-pool (5.2.0)
[  66] [Active     ] [Started] [   60] Apache ServiceMix Features :: ActiveMQ Commands (4.0.0)
```
Once installed, this feature offers a special set of commands which enables you easily use the broker.

This feature comes pre-installed with the ServiceMix ESB so previous steps are not necessary.

The following example shows how to create and destroy the broker using the `create-broker` and `destroy-broker` commands.
```
smx@root:/> activemq/create-broker
Creating file: /Users/dejanb/workspace/apache-servicemix-kernel-1.1.0/deploy/localhost-broker.xml
```
Default ActiveMQ Broker (localhost) configuration file created at: /Users/dejanb/workspace/apache-servicemix-kernel-1.1.0/deploy/localhost-broker.xml
Please review the configuration and modify to suite your needs.  
```
smx@root:/> activemq/destroy-broker
```
Default ActiveMQ Broker (localhost) configuration file created at: /Users/dejanb/workspace/apache-servicemix-kernel-1.1.0/deploy/localhost-broker.xml removed.

For more information on ActiveMQ feature take a look at the [ServiceMix ActiveMQ integration guide](http://servicemix.apache.org/SMX4/activemq-integration.html)

Web console
===========

[Web console](web-console) is included in ActiveMQ distribution and you'll probably want to have it to monitor your broker. First of all you'll have to install the `web` feature, so that you have a web container ready to deploy our web application. To do that you should first add the ServiceMix features url, like we did above (if you didn't already). Now, you can just run
```
smx@root:/> features/install web
```
to grab all necessary bundles. If you type
```
smx@root:/> osgi/list
```
you'll see all installed bundles (such as)
```
[  43] [Active     ] [       ] [   60] Apache ServiceMix Bundles: jetty-6.1.14 (6.1.14.1)
[  44] [Active     ] [       ] [   60] OPS4J Pax Web - Jsp Support (0.5.2)
[  45] [Active     ] [       ] [   60] OPS4J Pax Web - Web Container (0.5.2)
[  46] [Active     ] [       ] [   60] OPS4J Pax Web Extender - WAR (0.5.0)
[  47] [Active     ] [       ] [   60] OPS4J Pax Web Extender - Whiteboard (0.5.0)
[  48] [Active     ] [       ] [   60] OPS4J Pax Url - war:, war-i: (0.3.3)
[  49] [Active     ] [Started] [   60] Apache ServiceMix WAR Deployer (4.0.0)
```
This feature comes pre-installed with the ServiceMix ESB so previous steps are not necessary.

The important thing is that we need to use proper versions of Pax web bundles to make all this work, and those are:

*   pax-web-bundle version 0.6.0
*   pax-web-jsp version 0.6.0
*   pax-web-ex-war version 0.5.1
*   pax-web-ex-whiteboard version 0.5.1

So if you have older bundles, like it is the case in our example, you'll need to unistall them and install new ones. To uninstall old bundles, simply look at their id and execute
```
smx@root:/> osgi/uninstall [id]
```
In this example it would be
```
smx@root:/> osgi/uninstall 44
smx@root:/> osgi/uninstall 45
smx@root:/> osgi/uninstall 46
smx@root:/> osgi/uninstall 47
```
Now install new ones
```
smx@root:/> osgi/install mvn:org.ops4j.pax.web/pax-web-bundle/0.6.0
smx@root:/> osgi/install mvn:org.ops4j.pax.web/pax-web-jsp/0.6.0
smx@root:/> osgi/install mvn:org.ops4j.pax.web-extender/pax-web-ex-war/0.5.1
smx@root:/> osgi/install mvn:org.ops4j.pax.web-extender/pax-web-ex-whiteboard/0.5.1
```
After successful installation, you should start your newly added bundles. You can do that by executing
```
smx@root:/> osgi/start [id]
```
It's best to restart your container at this point and afterwards we're finally ready to deploy our war. To do that we'll use the Pax Web War Extender. The following example will use the latest `5.3-SNAPSHOT` build and deploy it in the OSGi runtime
```
smx@root:/> osgi/install war:mvn:org.apache.activemq/activemq-web-console/5.3-SNAPSHOT/war?Webapp-Context=activemqweb
```
You have to use the latest 5.3-SNAPSHOT web console, since this doesn't work right with previous versions.

Also, if you have problems installing the war with the previous method, download it manually from [https://repository.apache.org/content/repositories/snapshots/org/apache/activemq/activemq-web-console/5.3-SNAPSHOT/](https://repository.apache.org/content/repositories/snapshots/org/apache/activemq/activemq-web-console/5.3-SNAPSHOT/) and install using
```
smx@root:/> osgi/install war:file:///path\_to\_the_war/activemq-web-console-5.3-SNAPSHOT.war?Webapp-Context=activemqweb
```
Unfortunately the command
```
osgi/install \
war:mvn:https://repository.apache.org/content/repositories/snapshots@snapshots!org.apache.activemq/activemq-web-console/5.3-SNAPSHOT/war?Webapp-Context=activemqweb
```
does not work with pax web war extender 0.5.1, but you try it if you're using the newer one.

If you execute
```
smx@root:/> osgi/list
```
you'll see the newly added bundle here.
```
[  84] [Resolved   ] [       ] [   60] mvn\_org.apache.activemq\_activemq-web-console\_5.3-SNAPSHOT\_war (0)
```
It's time t start it now
```
smx@root:/> osgi/start 84
```
Use the id of your bundle

The important thing to note here is that by default, web console will start an embedded broker and monitor it. So if you have a broker already running it will fail to start.

Note that we have used `activemqweb` as a `WebApp-Context` while deploying the war, so the console will be available at [http://localhost:8080/activemqweb](http://localhost:8080/activemqweb). Visit this URL and administer your embedded broker.

There a big chance that you don't want to monitor an embedded broker, but the broker you have started separately (in the OSGi runtime or elsewhere). To configure your web console to do that, you have to do the following. First, exit your OSGi runtime. Now you can edit the `etc/system.properties` file and add some configuration details. The following configuration snippet
```
webconsole.type=properties
webconsole.jms.url=tcp://localhost:61616
webconsole.jmx.url=service:jmx:rmi:///jndi/rmi://localhost:1099/karaf-root
webconsole.jmx.user=karaf
webconsole.jmx.password=karaf
```
should work fine with default broker started in the Karaf (ServiceMix). Adapt it to your specific needs.

Now if you run the runtime (and the console bundle), you'll notice that the console will not start the broker and will try to use one defined with the system properties.

Visit [http://localhost:8080/activemqweb](http://localhost:8080/activemqweb) and enjoy.

