---
title: "Getting Started with ActiveMQ"
description: "Install ActiveMQ Classic 5.x on Windows or Unix from binary, source or Homebrew, then start, test, monitor and stop the broker with the activemq script."
slug: /getting-started
---

Introduction
------------

This document describes how to install and configure ActiveMQ 5.x for both Unix and Windows' platforms.

Document Organization
---------------------

The Getting Started Guide for ActiveMQ 5.x document contains the following sections:

*   [Introduction](#introduction)
*   [Document Organization](#document-organization)
*   [Pre-Installation Requirements](#pre-installation-requirements)
*   [Installation Procedure for Windows](#installation-procedure-for-windows)
    *   [Windows Binary Installation](#windows-binary-installation)
    *   [Windows Source Installation](#windows-source-installation)
    *   [Windows Developer's Release](#windows-developers-release)
*   [Installation Procedure for Unix](#installation-procedure-for-unix)
    *   [Unix Binary Installation](#unix-binary-installation)
    *   [Using Homebrew installer on OSX](#using-homebrew-installer-on-osx)
    *   [Unix Source Installation](#unix-source-installation)
    *   [Unix Developer's Release](#unix-developers-release)
*   [Starting ActiveMQ](#starting-activemq)    
    *   [On Windows:](#on-windows)
    *   [On Unix:](#on-unix)    
    *   [More help](#more-help)
*   [Testing the Installation](#testing-the-installation)
    *   [Using the administrative interface](#using-the-administrative-interface)
    *   [Logfile and console output](#logfile-and-console-output)
    *   [Listen port](#listen-port)
*   [Monitoring ActiveMQ](#monitoring-activemq)
*   [Stopping ActiveMQ](#stopping-activemq)
*   [Configuring ActiveMQ](#configuring-activemq)
*   [Additional Resources](#additional-resources)
*   [Docker Image](docker-image)

Pre-Installation Requirements
-----------------------------

**Hardware:**

*   ~ 60 MB of free disk space for the ActiveMQ 5.x binary distribution.  
    (you need additional disk space for storing persistent messages to disk)
*   ~ 300 MB of free disk space for the ActiveMQ 5.x source or developer's distributions.

**Operating Systems:**

*   Windows: Windows XP SP2, Windows 2000, Windows Vista, Windows 7.
*   Unix: Ubuntu Linux, Powerdog Linux, MacOS, AIX, HP-UX, Solaris, or any Unix platform that supports Java.

**Environment:**

*   [Java Runtime Environment (JRE)](http://www.oracle.com/technetwork/java/javase/downloads/index.html)  **JRE 1.8** or greater (1.7 for version <=5.15.0, 1.6 for version <=5.10.0)  
    (a JDK is required if you plan to recompile source code)
*   The JAVA_HOME environment variable must be set to the directory where the JRE is installed  
    (Unix: the binary "java" has to be resolvable by the PATH variable; execute "which java" to verify)
*   [Maven](http://maven.apache.org/) 3.0.0 build system  
    (only if you plan to recompile source code)
*   [JARs](http://cvs.apache.org/repository/geronimo-spec/jars/) that will be used must be added to the classpath.  
    (only if you plan to recompile source code)

Installation Procedure for Windows
----------------------------------

This section of the Getting Started Guide explains how to install binary and source distributions of ActiveMQ on a Windows system.

#### Windows Binary Installation

This procedure explains how to download and install the binary distribution on a Windows system.

1.  Download the latest release  
    (see [Download](/components/classic/download) -\> "The latest stable release" -> "apache-activemq-x.x.x-bin.zip")
2.  Extract the files from the ZIP file into a directory of your choice.
3.  Proceed to the [Starting ActiveMQ](#starting-activemq) section of this document.
4.  Following start-up, go to the [Testing the Installation](#testing-the-installation) section of this document.

#### Windows Source Installation

This procedure explains how to download and install the source distribution on a Windows system.

1.  Download the latest release  
    (see [Download](/components/classic/download) -\> "The latest stable release" -> "apache-activemq-x.x.x-source-release.zip")
2.  Extract ActiveMQ from the ZIP file into a directory of your choice.
3.    
    The recommended method of building ActiveMQ is the following:
    ```
    REM add "-Dmaven.test.skip=true" if tests are failing on your system (should not happen)
    mvn clean install 
    ```
    where `[activemq_install_dir]` is the directory in which ActiveMQ was installed.
    
4.  If you prefer to use an IDE, then you can auto-generate the IDE's project file using maven plugins:
    ```
    mvn eclipse:eclipse
    ```
    or
    ```
    mvn idea:idea
    ```
    Feel free to use any other applicable IDE. Please refer to the [plugin reference](http://maven.apache.org/reference/plugins/plugins.html) for more details.
    

1.  Start ActiveMQ from the target directory, for example:
    ```
    cd [activemq_install_dir]\assembly\target
    unzip activemq-x.x-SNAPSHOT.zip
    cd activemq-x.x-SNAPSHOT
    bin\activemq
    ```

    > **NOTE:** Working directories get created relative to the current directory. To create the working directories in the proper place, ActiveMQ must be launched from its home/installation directory.
    
2.  Proceed to the [Testing the Installation](#testing-the-installation) section.
    
    > **Warning**
    > 
    > If you are building ActiveMQ 4.x under Windows using Cygwin there is a path name length limitation. If the path name length is exceeded, you may see build errors. To correct this, move the ActiveMQ source directory higher in the file system tree, e.g., /cygdrive/c/d/sm.
    

#### Windows Developer's Release

This procedure explains how to download and install the latest developer's snapshot.

1.  Open the release archive: [https://repository.apache.org/content/repositories/snapshots/org/apache/activemq/apache-activemq/](https://repository.apache.org/content/repositories/snapshots/org/apache/activemq/apache-activemq/)  
    (open one of the SNAPSHOT directories)
2.  Select the version of ActiveMQ to download (if necessary, scroll down to see the ActiveMQ snapshots).
3.  Extract the files from the ZIP file into a directory of your choice.
4.  If a binary snapshot was downloaded, proceed to the [Starting ActiveMQ](#starting-activemq) section of this document.  
    If a source snapshot was downloaded, perform step 6 and step 7 of the [Windows Source Installation](#windows-source-installation) procedure.
5.  Following start-up, proceed to the [Testing the Installation](#testing-the-installation) section.

Installation Procedure for Unix
-------------------------------

#### Unix Binary Installation

This procedure explains how to download and install the binary distribution on a Unix system.  
**NOTE:** There are several alternative ways to perform this type of installation.

1.  Download the activemq zipped tarball file to the Unix machine, using either a browser or a tool, i.e., wget, scp, ftp, etc. for example:  
    (see [Download](/components/classic/download) -\> "The latest stable release")
    ```
    wget http://activemq.apache.org/path/tofile/apache-activemq-x.x.x-bin.tar.gz
    ```
2.  Extract the files from the zipped tarball into a directory of your choice. For example:
    ```
    cd [activemq_install_dir]
    tar zxvf activemq-x.x.x-bin.tar.gz
    ```
3.  Proceed to the [Starting ActiveMQ](#starting-activemq) section of this document.
    
4.  Following start-up, go to the [Testing the Installation](#testing-the-installation) section.

#### Using Homebrew installer on OSX

If you use OSX as your platform, you can use [Homebrew](http://mxcl.github.com/homebrew/) package manager to easily install Apache ActiveMQ.

1.  After installing Homebrew package manager successfully, just run
    ```
    $ brew install apache-activemq
    ```
    You can expect the following output:
    ```
    ==\> Downloading http://www.gossipcheck.com/mirrors/apache/activemq/apache-activemq/x.x.x/apache-activemq-x.x.x-bin.tar.gz
    ######################################################################## 100.0%
    ==\> Caveats
    Software was installed to:
      /usr/local/Cellar/apache-activemq/x.x.x/libexec
    ==\> Summary
    /usr/local/Cellar/apache-activemq/x.x.x: 406 files, 35M, built in 2 seconds
    ```

ActiveMQ will be installed in `/usr/local/Cellar/apache-activemq/x.x.x/` directory (where `x.x.x` denotes the actual version being installed).

Now you can proceed to [Starting ActiveMQ](#starting-activemq) and [Testing the Installation](#testing-the-installation) sections.

#### Unix Source Installation

This procedure explains how to download and install the source distribution on a Unix system. This procedure assumes the Unix machine has a browser. Please see the previous [Unix Binary Installation](#unix-binary-installation) section for details on how to install ActiveMQ without a browser.

1.  Download the latest source release  
    (see [Download](/components/classic/download) -\> "The latest stable release" -> "activemq-parent-x.x.x-source-release.zip")
2.  Extract the files from the ZIP file into a directory of your choice. For example:
    ```
    tar zxvf activemq.x.x-src.tar.gz
    ```
3.  Build ActiveMQ using Maven:  
    The preferred method of building ActiveMQ is the following:
    ```
    cd [activemq_install_dir]
    mvn clean install # add "-Dmaven.test.skip=true" if tests are failing on your system (should not happen)
    ```
    If Maven crashes with a java.lang.OutOfMemoryError, you you need to do this first (assuming a Bourne-like shell):
    ```
    export MAVEN_OPTS="-Xmx512M"
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
    
4.  Proceed to the [Starting ActiveMQ](#starting-activemq) section of this document.
5.  Proceed to [Testing the Installation](#testing-the-installation) section.

#### Unix Developer's Release

This procedure explains how to download and install the latest developer's snapshot.

1.  Open the release archive: [https://repository.apache.org/content/repositories/snapshots/org/apache/activemq/apache-activemq/](https://repository.apache.org/content/repositories/snapshots/org/apache/activemq/apache-activemq/)  
    (open one of the SNAPSHOT directories)
2.  Select the version of ActiveMQ to download (you may have to scroll down to see the ActiveMQ snapshots).  
    The filename will be similar to: `activemq-x.x.x-tar.gz`.
3.  Extract the files from the gzip file into a directory of your choice. For example:  
    For a binary developer's snapshot:
    ```
    tar zxvf activemq-x.x.x.tar.gz
    ```
    For a source developer's snapshot:
    ```
    tar zxvf activemq-x.x.x-src.tar.gz
    ```
4.  If a binary snapshot was downloaded, to make it executable, the `activemq` script may need its permissions changed:
    ```
    cd [activemq_install_dir]/bin
    chmod 755 activemq
    ```
5.  For a binary snapshot, proceed to the [Starting ActiveMQ](#starting-activemq) section of this document.
6.  If a source snapshot was downloaded perform steps 6 - 8 of the [Unix Source Installation](#unix-source-installation) procedure.
7.  Proceed to the [Testing the Installation](#testing-the-installation) section.

Starting ActiveMQ
-----------------

There now follows instructions on how to [run the ActiveMQ Message Broker](run-broker).

#### On Windows:

From a console window, change to the installation directory and run `activemq`:
```
cd [activemq_install_dir]
```

where `[activemq_install_dir]` is the directory in which ActiveMQ was installed, e.g., `c:\Program Files\ActiveMQ-5.x`.  
Then type:
```
bin\activemq start
```

`NOTE:** Working directories get created relative to the current directory. To create working directories in the proper place, ActiveMQ must be launched from its home/installation directory.

#### On Unix:

From a command shell, change to the installation directory and run `activemq` as a **foregroud** process:
```
cd [activemq_install_dir]/bin
./activemq console
```

From a command shell, change to the installation directory and run `activemq` as a **daemon** process:
```
cd [activemq_install_dir]/bin
./activemq start
```
### More help

For other ways of running the broker see [Here](run-broker). For example you can run an [embedded broker](how-do-i-embed-a-broker-inside-a-connection) inside your JMS Connection to avoid starting a separate process.

Testing the Installation
------------------------

### Using the administrative interface

*   Open the administrative interface
*   *   URL: [http://127.0.0.1:8161/admin/](http://127.0.0.1:8161/admin/)
    *   Login: admin
    *   Passwort: admin
*   Navigate to "Queues"
*   Add a queue name and click create
*   Send test message by klicking on "Send to"

### Logfile and console output

If ActiveMQ is up and running without problems, the Window's console window or the Unix command shell will display information similar to the following log line (see stdout output or "[activemq_install_dir]/data/activemq.log"):
```
Apache ctiveMQ 5.11.1 (localhost, ID:ntbk11111-50816-1428933306116-0:1) started | org.apache.activemq.broker.BrokerService | main
```
### Listen port

ActiveMQ's default port is 61616. From another window run netstat and search for port 61616.

From a Windows console, type:
```
netstat -an|find "61616"
```
**OR**

From a Unix command shell, type:
```
netstat -nl|grep 61616
```

Monitoring ActiveMQ
-------------------

You can monitor ActiveMQ using the [Web Console](web-console) by pointing your browser at `http://localhost:8161/admin`.

From ActiveMQ 5.8 onwards the web apps is secured out of the box.  
The default username and password is admin/admin. You can configure this in the conf/jetty-real.properties file.

Or you can use the [JMX](jmx) support to view the running state of ActiveMQ.

For more information see the file `docs/WebConsole-README.txt` in the distribution.

Stopping ActiveMQ
-----------------

For both Windows and Unix installations, terminate ActiveMQ by typing "CTRL-C" in the console or command shell in which it is running.

If ActiveMQ was started in the background on Unix, the process can be killed, with the following:
```
cd [activemq_install_dir]/bin
./activemq stop
```

Configuring ActiveMQ
--------------------

The ActiveMQ broker should now run. You can configure the broker by specifying an [Xml Configuration](xml-configuration) file as a parameter to the _activemq_ command. An alternative is to use the [Broker Configuration URI](broker-configuration-uri) to configure things on the command line in a concise format (though the configuration options are not as extensive as if you use Java or XML code). You can also

Also see [Configuring Transports](configuring-transports) to see how you can configure the various connection, transport and broker options using the connection URL in the ActiveMQConnectionFactory.

See the [Initial Configuration](initial-configuration) for details of which jars you need to add to your classpath to start using ActiveMQ in your Java code

If you want to use JNDI to connect to your JMS provider then please view the [JNDI Support](jndi-support). If you are a Spring user you should read about [Spring Support](spring-support)

After the installation, ActiveMQ is running with a basic configuration. For details on configuring options, please see refer to the [Configuration](configure-version-5-brokers) section.

Additional Resources
--------------------

If you are new to using ActiveMQ, running the [Web Samples](web-samples) or the [Examples](examples) is a good next step to learn more about ActiveMQ.

The commercial providers listed on the [issues](/support#commercial-support) page may also have additional documentation, examples, tutorials, etc... that can help you get started.

