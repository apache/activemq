---
title: "Getting Started"
description: "Step-by-step ActiveMQ Classic install on Windows and Unix from binary, source or snapshot, then start, test on port 61616, monitor and stop the broker."
slug: /version-5-getting-started
---


Introduction
------------

This document describes how to install and configure ActiveMQ for both Unix and Windows' platforms.


Pre-Installation Requirements
-----------------------------

**Hardware:**

*   60 MB of free disk space for the ActiveMQ binary distribution.
*   200 MB of free disk space for the ActiveMQ source or developer's distributions.

**Operating Systems:**

*   Windows: Windows XP SP2, Windows 2000.
*   Unix: Ubuntu Linux, Powerdog Linux, MacOS, AIX, HP-UX, Solaris, or any Unix platform that supports Java.

**Environment:**

*   Java Developer Kit (JDK) 1.7.x or greater for deployment and 1.7.x (Java 7) for compiling/building.
*   The JAVA_HOME environment variable must be set to the directory where the JDK is installed, e.g., `c:\Program Files\jdk.1.7.0_xx_xx`.
*   Maven 3.0 or greater (required when installing source or developer's releases).
*   [JARs](http://cvs.apache.org/repository/geronimo-spec/jars/) that will be used must be added to the classpath.

Installation Procedure for Windows
----------------------------------

This section of the Getting Started Guide explains how to install binary and source distributions of ActiveMQ on a Windows system.

#### Windows Binary Installation

This procedure explains how to download and install the binary distribution on a Windows system.

1.  From a browser, navigate to [activemq.apache.org/](http://activemq.apache.org/).
2.  Click the [Download](/components/classic/download) link in the navigation pane (the left pane).
3.  Select the latest distribution (for older releases, click the link to the archives).  
    For a binary distribution, the filename will be similar to: `activemq-x.x.x.zip`.
4.  Extract the files from the ZIP file into a directory of your choice.
5.  Proceed to the [#Starting ActiveMQ](version-5-getting-started) section of this document.
6.  Following start-up, go to the [#Testing the Installation](version-5-getting-started) section of this document.

#### Windows Source Installation

This procedure explains how to download and install the source distribution on a Windows system.

**NOTE:** ActiveMQ requires Java 7 to run and to build

1.  From a browser, navigate to [activemq.apache.org/](http://activemq.apache.org/).
2.  Click the [Download](/components/classic/download) link in the navigation pane (the left pane).
3.  Select the latest distribution (for older releases, click the link to the archives).  
    For a source distribution, the filename will be similar to: `activemq-x.x-src.zip`.
4.  Extract ActiveMQ from the ZIP file into a directory of your choice.
5.  Build ActiveMQ using Maven 2.1 or greater and Java 1.7.

The recommended method of building ActiveMQ is the following:
```
cd [activemq_install_dir]
mvn clean install
```
where `[activemq_install_dir]` is the directory in which ActiveMQ was installed.

If the above build fails on some tests, type the following:
```
cd [activemq_install_dir]
mvn clean install -Dmaven.test.skip=true 
```
1.  If you prefer to use an IDE, then you can auto-generate the IDE's project file using maven plugins:
    ```
    mvn eclipse:eclipse
    ```
    or
    ```
    mvn idea:idea
    ```
    Feel free to use any other applicable IDE. Please refer to the [plugin reference](http://maven.apache.org/plugins/index.html) for more details.
    
2.  Start ActiveMQ from the target directory, for example:
    ```
    cd [activemq_install_dir]\assembly\target
    unzip activemq-x.x-SNAPSHOT.zip
    cd activemq-x.x-SNAPSHOT
    bin\activemq
    ```
    > **NOTE:** Working directories get created relative to the current directory. To create the working directories in the proper place, ActiveMQ must be launched from its home/installation directory.
    
3.  Proceed to the [#Testing the Installation](version-5-getting-started) section.

> **Warning**
> 
> If you are building ActiveMQ 5.x under Windows using Cygwin there is a path name length limitation. If the path name length is exceeded, you may see build errors. To correct this, move the ActiveMQ source directory higher in the file system tree, e.g., /cygdrive/c/d/sm.

#### Windows Developer's Release

This procedure explains how to download and install the latest developer's snapshot.

**NOTE:** ActiveMQ requires Java 7 to run and to build

1.  From a browser, navigate to [activemq.apache.org/](http://activemq.apache.org/).
2.  Click the [Download](/components/classic/download) link in the navigation pane (the left pane).
3.  Click the Maven ActiveMQ SNAPSHOT link.
4.  Select the version of ActiveMQ to download
5.  Extract the files from the ZIP file into a directory of your choice.
6.  If a binary snapshot was downloaded, proceed to the [#Starting ActiveMQ](version-5-getting-started) section of this document.  
    If a source snapshot was downloaded, perform step 6 and step 7 of the [#Windows Source Installation](version-5-getting-started) procedure.
7.  Following start-up, proceed to the [#Testing the Installation](version-5-getting-started) section.

Installation Procedure for Unix
-------------------------------

#### Unix Binary Installation

This procedure explains how to download and install the binary distribution on a Unix system.  

**NOTE:** There are several alternative ways to perform this type of installation.

1.  Download the activemq gzip file to the Unix machine, using either a browser or a tool, i.e., wget, scp, ftp, etc. for example:
    ```
    \> wget http://activemq.apache.org/path/tofile/apache-activemq-5.8-tar.gz
    ```
2.  Extract the files from the gzip file into a directory of your choice. For example:
    ```
    \> tar zxvf activemq-x.x.x.tar.gz
    ```
3.  If the `activemq` start-up script is not executable, change its permissions. The `activemq` script is located in the `bin` directory. For example:
    ```
    \> cd [activemq_install_dir]/bin
    \> chmod 755 activemq
    ```
4.  Proceed to the [#Starting ActiveMQ](version-5-getting-started) section of this document.
5.  Following start-up, go to the [#Testing the Installation](version-5-getting-started) section.

#### Unix Source Installation

This procedure explains how to download and install the source distribution on a Unix system. This procedure assumes the Unix machine has a browser. Please see the previous [#Unix Binary Installation](version-5-getting-started) section for details on how to install ActiveMQ without a browser.

**NOTE:** ActiveMQ requires Java 7 to run and to build

1.  From a browser, navigate to [activemq.apache.org/](http://activemq.apache.org/).
2.  Click the [Download](/components/classic/download) link in the navigation pane (the left pane).
3.  Click the Maven ActiveMQ SNAPSHOT link.
4.  Select the version of ActiveMQ to download  
    For a source distribution, the filename will be similar to: `activemq-x.x-src.tar.gz`.
5.  Extract the files from the ZIP file into a directory of your choice. For example:
    ```
    tar zxvf activemq.x.x-src.tar.gz
    ```
6.  Build ActiveMQ using Maven 3.0 or greater and Java 7:  
    The preferred method of building ActiveMQ is the following:
    ```
    cd [activemq_install_dir]
    mvn clean install
    ```
    If Maven crashes with a java.lang.OutOfMemoryError, you you need to do this first (assuming a Bourne-like shell):
    ```
    export MAVEN_OPTS=-Xmx1024M
    ```
    If the above build fails on some tests, do the following:
    ```
    cd [activemq_install_dir]
    mvn clean install -Dmaven.test.skip=true 
    ```
    If you prefer to use an IDE then you can auto-generate the IDE's project file using maven plugins:
    ```
    mvn eclipse:eclipse
    ```
    or
    ```
    mvn idea:idea
    ```
    Feel free to use any other applicable IDE. Please refer to the [plugin reference](http://maven.apache.org/reference/plugins/plugins.html) for more details.  
    **NOTE:** Working directories get created relative to the current directory. To create working directories in the proper place, ActiveMQ must be launched from its home/installation directory.
    
7.  Proceed to the [#Starting ActiveMQ](version-5-getting-started) section of this document.
8.  Proceed to [#Testing the Installation](version-5-getting-started) section.

#### Unix Developer's Release

This procedure explains how to download and install the latest developer's snapshot.

**NOTE:** ActiveMQ requires Java 7 to run and to build

1.  From a browser, navigate to [activemq.apache.org/](http://activemq.apache.org/).
2.  Click the [#Download](version-5-getting-started) link in the navigation pane (the left pane).
3.  Click the Maven ActiveMQ SNAPSHOT link.
4.  Select the version of ActiveMQ to download
5.  Extract the files from the gzip file into a directory of your choice. For example:  
    For a binary developer's snapshot:
    ```
    tar zxvf activemq-x.x.x.tar.gz
    ```
    For a source developer's snapshot:
    ```
    tar zxvf activemq-x.x.x-src.tar.gz
    ```
6.  If a binary snapshot was downloaded, to make it executable, the `activemq` script may need its permissions changed:
    ```
    cd [activemq_install_dir]/bin
    chmod 755 activemq
    ```
7.  For a binary snapshot, proceed to the [#Starting ActiveMQ](version-5-getting-started) section of this document.
8.  If a source snapshot was downloaded perform steps 6 - 8 of the [#Unix Source Installation](version-5-getting-started) procedure.
9.  Proceed to the [#Testing the Installation](version-5-getting-started) section.

Starting ActiveMQ
-----------------

There now follows instructions on how to [run the ActiveMQ Message Broker](run-broker).

See also [Starting ActiveMQ with a different configuration file](#)

### On Windows

From a console window, change to the installation directory and run `activemq`:
```
cd [activemq_install_dir]
```
where `activemq_install_dir` is the directory in which ActiveMQ was installed, e.g., `c:\Program Files\ActiveMQ-5.x`.

Then type (depending on ActiveMQ version):

**ActiveMQ 5.10 onwards**
```
bin\activemq start
```
`ActiveMQ 5.9 or older**
```
bin\activemq
```
**NOTE:** Working directories get created relative to the current directory. To create working directories in the proper place, ActiveMQ must be launched from its home/installation directory.

### On Unix

From a command shell, change to the installation directory and run `activemq`:
```
cd [activemq_install_dir]
```
where `activemq_install_dir` is the directory in which ActiveMQ was installed, e.g., `/usr/local/activemq-5.x`.  
Then type:
```
bin/activemq start
```
OR
```
bin/activemq start > /tmp/smlog  2>&1 &;
Note: /tmp/smlog may be changed to another file name.
```
**NOTE:** Working directories get created relative to the current directory. To create working directories in the proper place, ActiveMQ must be launched from its home/installation directory.

> **Warning**
> 
> Do NOT close the console or shell in which ActiveMQ was started, as that will terminate ActiveMQ (unless ActiveMQ was started with nohup).
```
nohup bin/activemq > /tmp/smlog 2>&1 &
```

### More help

For other ways of running the broker see [Here](run-broker). For example you can run an [embedded broker](how-do-i-embed-a-broker-inside-a-connection) inside your JMS Connection to avoid starting a separate process.

Testing the Installation
------------------------

If ActiveMQ is up and running without problems, the Window's console window or the Unix command shell will display information similar to the following log line:
```
INFO  ActiveMQ JMS Message Broker (ID:apple-s-Computer.local-51222-1140729837569-0:0) has started
```
  
ActiveMQ's default port is 61616. From another window run netstat and search for port 61616.

From a Windows console, type:
```
netstat -an|find "61616"
```
**OR**

From a Unix command shell, type:
```
netstat -an|grep 61616
```

Monitoring ActiveMQ
-------------------

You can monitor ActiveMQ using the [Web Console](web-console) by pointing your browser at

[http://localhost:8161/admin](http://localhost:8161/admin)

From ActiveMQ 5.8 onwards the web apps is secured out of the box.  
The default username and password is admin/admin. You can configure this in the conf/jetty-real.properties file.

Or you can use the [JMX](jmx) support to view the running state of ActiveMQ.

For more information see the file `docs/WebConsole-README.txt` in the distribution.

Stopping ActiveMQ
-----------------

For both Windows and Unix installations, terminate ActiveMQ by typing "CTRL-C" in the console or command shell in which it is running.

If ActiveMQ was started in the background on Unix, the process can be killed, with the following:
```
cd [activemq_install_dir]
```
where `activemq_install_dir` is the directory in which ActiveMQ was installed, e.g., `/usr/local/activemq-5.x`.  
Then type:
```
bin/activemq stop
```
Or you can do the following:
```
ps -ef|grep activemq
kill [PID]
```
where `[PID]` is the process id of the ActiveMQ process.

Starting ActiveMQ with a different configuration file
-----------------------------------------------------

By default ActiveMQ uses the `conf/activemq.xml` as the main configuration file when starting. If you want to use a different configuration file you can specify this as a parameter.

#### ActiveMQ 5.8 or older

For example to use `conf/activemq-demo.xml` which includes the web demos, you do as follows:
```
bin/activemq console xbean:conf/activemq-demo.xml
```
To start Active in console mode and load the `conf/activemq-demo.xml` file. Yes you need the `xbean:` as prefix.

And to launch as a background process you can do:
```
bin/activemq start xbean:conf/activemq-demo.xml
```

#### ActiveMQ 5.9 onwards

For example to use `examples/conf/activemq-demo.xml` which includes the web demos, you do as follows:
```
bin/activemq console xbean:examples/conf/activemq-demo.xml
```
To start Active in console mode and load the `examples/conf/activemq-demo.xml` file. Yes you need the `xbean:` as prefix.

And to launch as a background process you can do:
```
bin/activemq start xbean:examples/conf/activemq-demo.xml
```

Configuring ActiveMQ
--------------------

The ActiveMQ broker should now run. You can configure the broker by specifying an [Xml Configuration](xml-configuration) file as a parameter to the _activemq_ command. An alternative is to use the [Broker Configuration URI](broker-configuration-uri) to configure things on the command line in a concise format (though the configuration options are not as extensive as if you use Java or XML code). You can also

Also see [Configuring Transports](configuring-transports) to see how you can configure the various connection, transport and broker options using the connection URL in the ActiveMQConnectionFactory.

See the [Initial Configuration](initial-configuration) for details of which jars you need to add to your classpath to start using ActiveMQ in your Java code

If you want to use JNDI to connect to your JMS provider then please view the [JNDI Support](jndi-support). If you are a Spring user you should read about [Spring Support](spring-support)

After the installation, ActiveMQ is running with a basic configuration. For details on configuring options, please see refer to the [Configuration](/components/classic/documentation/overview/faq/configuration) section.

Additional Resources
--------------------

If you are new to using ActiveMQ, running the [Web Samples](web-samples) or the [Examples](examples) is a good next step to learn more about ActiveMQ.

