---
title: "Java Service Wrapper"
slug: /java-service-wrapper
---


This page provides some useful information on running the ActiveMQ broker as a windows-NT service or a daemon thread in Linux or Unix systems.

64bit 4gb memory limit

The ActiveMQ distribution uses an older and free community release of the service wrapper library. This wrapper has a memory limit of 4gb when running on 64bit platforms. You can manually [download a newer release](http://wrapper.tanukisoftware.com/doc/english/download.jsp) of the wrapper library that does not have this limit, from the vendors website at. It us not possible for Apache ActiveMQ to update and distribute a newer release of the service wrapper, as the vendor changed licensing terms, which does not comply with ASF licensing terms. Therefore out of the box the service wrapper is stuck on that older release.

However you can manually upgrade the library.

### Windows-NT Service

Here are the steps to install the ActiveMQ broker as an NT service:

1.  Download the windows binary distribution (i.e. apache-activemq-5.9.0.zip)
2.  Go to ACTIVEMQ_HOME/bin/win32
3.  Run InstallService.bat

After running InstallService.bat, the ActiveMQ service should be added to the list of NT services. It is not started by default.

To verify, go to control panel -> administrative tools -> services and look for the ActiveMQ service. Here you can start the ActiveMQ service or configure it to start automatically every time the system boots.

To remove the ActiveMQ service:

1.  Run UninstallService.bat

### Linux/Mac OS X Daemon

Here are the steps to run the ActiveMQ broker as a daemon in Linux or Unix systems:

1.  Download the linux/unix binary distribution (i.e. apache-activemq-5.9.0.tar.gz)
2.  Go to ACTIVEMQ\_HOME/linux for linux systems or ACTIVEMQ\_HOME/macosx for Mac OS X systems.
3.  Run ./activemq start

To stop the daemon process:

1.  Go to ACTIVEMQ\_HOME/linux for linux systems or ACTIVEMQ\_HOME/macosx for Mac OS X systems.
2.  Run ./activemq stop

###  Configuring the Java Service Wrapper

ActiveMQ uses the [Java Service Wrapper](http://wrapper.tanukisoftware.org/doc/english/introduction.html) to run the broker. To configure how the Java Service Wrapper starts up, you can edit the wrapper.conf located in the bin/win32, bin/linux, or bin/macosx depending on which system you are running it. For more information on the different properties of the Java Service Wrapper, refer to this [page](http://wrapper.tanukisoftware.org/doc/english/properties.html)

###  Troubleshooting

If you are having problems running the service, turn on the wrapper debugging in the corresponding wrapper.conf file via (wrapper.debug=TRUE)

Each time you run the wrapper, a log file will be created in ACTIVEMQ_HOME/data/wrapper.log 

