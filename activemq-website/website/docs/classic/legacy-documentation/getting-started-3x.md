---
title: "Getting Started 3.x"
slug: /getting-started-3x
---

Introduction
------------

This document describes how to install and configure ActiveMQ 3.x for both Unix and Windows' platforms.

Pre-Installation Requirements
-----------------------------

**Hardware:**

*   33 MB of free disk space for the ActiveMQ 3.x binary distribution.
*   18 MB of free disk space for the ActiveMQ 3.x source or developer's distributions.

**Operating Systems:**

*   Windows: Windows XP SP2, Windows 2000.
*   Unix: Ubuntu Linux, Powerdog Linux, MacOS, AIX, HP-UX, Solaris, or any Unix platform that supports Java.

**Environment:**

*   Java Developer Kit (JDK) 1.4.x or greater for deployment and 1.5.x (Java 5) for compiling/building.
*   The JAVA_HOME environment variable must be set to the directory where the JDK is installed, e.g., c:\Program Files\jsdk.1.4.2-09.
*   Maven 1.0.2 or greater (required when installing source or developer's releases).
*   [JARs](http://cvs.apache.org/repository/geronimo-spec/jars/) that will be used must be added to the classpath.

Installation Procedure for Windows
----------------------------------

This section of the Getting Started Guide explains how to install binary and source distributions of ActiveMQ on a Windows system.

#### Windows Binary Installation

This procedure explains how to download and install the binary distribution on a Windows system.

1.  From a browser, navigate to [ActiveMQ.apache.org](http://ActiveMQ.apache.org).
2.  Click the Download link in the navigation pane (the left pane).
3.  Click the ActiveMQ 3.x Release link under the "Latest Releases" section. This brings up a new page.
4.  Under the Download Here section, select the desired distribution (you may have to scroll down to see the "Download Here" section).  
    For a binary distribution, the filename will be similar to: activemq-x.x.x.zip.
5.  Extract the files from the ZIP file into a directory of your choice.
6.  Proceed to the Starting ActiveMQ section of this document.
7.  Following start-up, go to the Testing the Installation section of this document.

#### Windows Source Installation

This procedure explains how to download and install the source distribution on a Windows system.

**NOTE:** ActiveMQ can be run on a Java 1.4.x system, however, Java 1.5 is required to compile/build ActiveMQ.

1.  From a browser, navigate to [ActiveMQ.apache.org](http://ActiveMQ.apache.org).
2.  Click the Download link in the navigation pane (the left pane).
3.  Click the ActiveMQ 3.x Release link under the "Latest Releases" section. This brings up another page.
4.  Under the Download Here section, select the desired distribution (if necessary, scroll down to see the "Download Here" section).  
    For a source distribution, the filename will be similar to: activemq-x.x-src.zip.
5.  Extract ActiveMQ from the ZIP file into a directory of your choice.
6.  Build ActiveMQ using Maven 1.0.2 or greater and Java 1.5.
    
    ```
    The recommended method of building ActiveMQ is the following: cd [activemq_install_dir] where [activemq_install_dir] is the directory in which ActiveMQ was installed. maven clean test If the above build fails on some tests, type the following: `cd [activemq_install_dir] maven -Dmaven.test.skip=true`
    ```
    
7.  If you prefer to use an IDE, then you can auto-generate the IDE's project file using maven plugins:
    
    ```
    maven eclipse or maven idea
    ```
    
    Feel free to use any other applicable IDE. Please refer to the [plugin reference](http://maven.apache.org/reference/plugins/plugins.html) for more details.
    
8.  Start ActiveMQ from the target directory, for example:
    
    ```
    cd [activemq_install_dir]\activemq-assembly\target\activemq-x.x-SNAPSHOT\ bin\activemq
    ```
    
    **NOTE:** Working directories get created relative to the current directory. To create the working directories in the proper place, ActiveMQ must be launched from its home/installation directory.
    
9.  Proceed to the [Testing the Installation](#testing-the-installation) section.

**NOTE:** Working directories get created relative to the current directory. To create the working directories in the proper place, ActiveMQ must be launched from its home/installation directory.


> **Warning**  
> 
> If you are building ActiveMQ 3.x under Windows using Cygwin there is a path name length limitation. If the path name length is exceeded, you may see build errors. To correct this, move the ActiveMQ source directory higher in the file system tree, e.g., /cygdrive/c/d/sm.

#### Windows Developer's Release

This procedure explains how to download and install the latest developer's snapshot.

**NOTE:** ActiveMQ can be run on a Java 1.4.x system, however, Java 1.5 is required to compile/build ActiveMQ.

1.  From a browser, navigate to [www.ActiveMQ.org](http://www.ActiveMQ.org).
2.  Click the Download link in the navigation pane (the left pane).
3.  Click the Current development SNAPSHOT release link.
4.  Select the version of ActiveMQ to download (if necessary, scroll down to see the ActiveMQ snapshots).
5.  Extract the files from the ZIP file into a directory of your choice.
6.  If a binary snapshot was downloaded, proceed to the Starting ActiveMQ section of this document.  
    If a source snapshot was downloaded, perform step 6 and step 7 of the Windows Source Installation procedure.
7.  Following start-up, proceed to the Testing the Installation section.

Installation Procedure for Unix
-------------------------------

#### Unix Binary Installation

This procedure explains how to download and install the binary distribution on a Unix system.

1.  Download the activemq gzip file to the Unix machine, using either a browser or a tool, i.e., wget, scp, ftp, etc.
    
If the Unix machine has a browser: 

   a. Navigate to https://activemq.apache.org. 
   b. Click the "Download" link in the navigation pane (the left pane). 
   c. Click the "ActiveMQ 3.x Release" link under the &amp;quot;Latest Releases&amp;quot; section. This brings up a new page. 
   d. Under the "Download Here" section, select the desired distribution (if necessary, scroll down to see the "Download Here" section). For a binary Unix distribution, the filename will be similar to: activemq-x.x.x.tar.gz. 
   
OR If the Unix machine does NOT have a browser, tools such as wget, scp, or ftp can be used to download the ActiveMQ distribution. It is beyond the scope of this document to explain the use of those tools. For convenience an example is provided below using a Windows machine (that has a browser) and a Unix machine with &amp;quot;wget&amp;quot; installed on it. \*NOTE:\* There are several alternative ways to perform this type of installation. 

   a. Obtain the link to the ActiveMQ distribution file. On the Windows machine with a browser, navigate to www.ActiveMQ.org. 
   b. Click the "Download" link in the left navigation pane. 
   c. Click the "ActiveMQ 3.x Release" link under the &amp;quot;Latest Releases&amp;quot; section. This brings up a new page. 
   d. Under the "Download Here" section, roll-over the desired distribution (if necessary, scroll down to see the "Download Here" section). For a binary Unix distribution the filename will be similar to: activemq-x.x.x.tar.gz. 
   e. Right-click on the distribution name and Copy Shortcut f. On the Unix machine, change to the directory in which ActiveMQ will be installed, e.g., /usr/local. 
   f. Download the ActiveMQ distribution to the Unix machine using the "wget" tool. For example, type "wget" followed by the saved shortcut: wget http://cvs.apache.org/repository/incubator-activemq/distributions/activemq-x.x.x.tar.gz
    
2.  Extract the files from the gzip file into a directory of your choice. For example:
    
    ```
    tar zxvf activemq-x.x.x.tar.gz
    ```
    
3.  If the ActiveMQ start-up script is not executable, change its permisssions. The `activemq` script is located in the bin directory. For example:
    
    ```
    cd [activemq_install_dir]/bin where [activemq_install_dir] is the directory in which ActiveMQ was installed. chmod 755 activemq
    ```
    
4.  Proceed to the Starting ActiveMQ section of this document.
5.  Following start-up, go to the Testing the Installation section.

#### Unix Source Installation

This procedure explains how to download and install the source distribution on a Unix system. This procedure assumes the Unix machine has a browser. Please see the previous [Unix Binary Installation](#unix-binary-installation) section for details on how to install ActiveMQ without a browser.

**NOTE:** ActiveMQ can be run on a Java 1.4.x system, however, Java 1.5 is required to compile/build ActiveMQ.

1.  From a browser, navigate to [activemq.apache.org](http://activemq.apache.org).
2.  Click the Download link in the navigation pane (the left pane).
3.  Click the ActiveMQ 3.x Release link under the "Latest Releases" section. This brings up a new page.
4.  Under the Download Here section, select the desired distribution (if necessary, scroll down to see the "Download Here" section).  
    For a source distribution, the filename will be similar to: activemq-x.x-src.tar.gz.
5.  Extract the files from the ZIP file into a directory of your choice. For example:
    
    ```
    tar zxvf activemq.x.x-src.tar.gz
    ```
    
6.  Build ActiveMQ using Maven 1.0.2 or greater and Java 5:
    
    ```
    The preferred method of building ActiveMQ is the following: cd [activemq_install_dir] where [activemq_install_dir] is the directory in which ActiveMQ was installed. maven clean test If the above build fails on some tests, do the following: cd [activemq_install_dir] maven m:clean maven -Dmaven.test.skip=true
    ```
    
7.  If you prefer to use an IDE then you can auto-generate the IDE's project file using maven plugins:
    
    ```
    maven eclipse or maven idea
    ```
    
    Feel free to use any other applicable IDE. Please refer to the [plugin reference](http://maven.apache.org/reference/plugins/plugins.html) for more details.  
    **NOTE:** Working directories get created relative to the current directory. To create working directories in the proper place, ActiveMQ must be launched from its home/installation directory.
    
8.  Proceed to the Starting ActiveMQ section of this document.
9.  Proceed to Testing the Installation section.

#### Unix Developer's Release

This procedure explains how to download and install the latest developer's snapshot.

**NOTE:** ActiveMQ can be run on a Java 1.4.x system, however, Java 5 is required to compile/build ActiveMQ.

1.  From a browser, navigate to [activemq.apache.org](http://activemq.apache.org).
2.  Click the Download link in the navigation pane (the left pane).
3.  Click the Current development SNAPSHOT release link.
4.  Select the version of ActiveMQ to download (you may have to scroll down to see the ActiveMQ snapshots). The filename will be similar to: activemq-x.x.x-tar.gz or activemq-x.x.x-src.tar.gz.
5.  Extract the files from the gzip file into a directory of your choice. For example:
    
    ```
    For a binary developer's snapshot: tar zxvf activemq-x.x.x.tar.gz For a source developer's snapshot: tar zxvf activemq-x.x.x-src.tar.gz
    ```
    
6.  If a binary snapshot was downloaded, to make it executable, the `activemq` script may need its permissions changed:
    
    ```
    cd [activemq_install_dir]/bin where [activemq_install_dir] is the directory in which ActiveMQ was installed. chmod 755 activemq
    ```
    
    #For a binary snapshot, proceed to the [Starting ActiveMQ](#starting-activemq) section of this document.  
    #If a source snapshot was downloaded perform steps 6 - 8 of the [Unix Source Installation](#unix-source-installation) procedure.
    
7.  Proceed to the [Testing the Installation](#testing-the-installation) section.

Starting ActiveMQ
-----------------

#### On Windows:

From a console window, change to the installation directory and run `activemq`:

```
cd [activemq_install_dir]
```

where activemq_install_dir is the directory in which ActiveMQ was installed, e.g., c:\Program Files\ActiveMQ-3.x.  
Then type:

```
bin\activemq
```

**NOTE:** Working directories get created relative to the current directory. To create working directories in the proper place, ActiveMQ must be launched from its home/installation directory.

#### On Unix:

From a command shell, change to the installation directory and run `activemq`:

```
cd [activemq_install_dir]
```

where activemq_install_dir is the directory in which ActiveMQ was installed, e.g., /usr/local/activemq-3.x.  
Then type:

```
bin/activemq OR bin/activemq &gt; /tmp/smlog 2&gt;&amp;1 &amp;; Note: /tmp/smlog may be changed to another file name.
```

**NOTE:** Working directories get created relative to the current directory. To create working directories in the proper place, ActiveMQ must be launched from its home/installation directory.


> **Warning**  
> 
> Do NOT close the console or shell in which ActiveMQ was started, as that will terminate ActiveMQ (unless ActiveMQ was started with nohup).

```
nohup bin/activemq &gt; /tmp/smlog 2&gt;&amp;1 &amp;
```

Testing the Installation
------------------------

If ActiveMQ is up and running without problems, the Window's console window or the Unix command shell will display information similar to the following log line:

```
INFO ActiveMQ JMS Message Broker (ID:apple-s-Computer.local-51222-1140729837569-0:0) has started
```

  
ActiveMQ's default port is 61616. From another window run netstat and search for port 61616.

From a Windows console, type:

```
netstat -an|find &amp;quot;61616&amp;quot;
```

**OR**

From a Unix command shell, type:

```
netstat -an|grep 61616
```

Stopping ActiveMQ
-----------------

For both Windows and Unix installations, terminate ActiveMQ by typing "CTRL-C" in the console or command shell in which it is running.

If ActiveMQ was started in the background on Unix, the process can be killed, with the following:

```
ps -ef|grep activemq kill [PID] where [PID] is the process id of the ActiveMQ process.
```

Configuring ActiveMQ
----------------------------

The ActiveMQ broker should now run. You can configure the broker by specifying an [Xml Configuration](../using-activemq-classic/xml-configuration.md "Xml Configuration") file as a parameter to the _activemq_ command.

See the [Initial Configuration](../using-activemq-classic/initial-configuration.md "Initial Configuration") for details of which jars you need to add to your classpath to start using ActiveMQ in your Java code

If you want to use JNDI to connect to your JMS provider then please view the [JNDI Support](../connectivity/containers/jndi-support.md "JNDI Support"). If you are a Spring user you should read about [Spring Support](../connectivity/containers/spring-support/index.md "Spring Support")

After the installation, ActiveMQ is running with a basic configuration. For details on configuring options, please see refer to the [Configuration](/components/classic/documentation/overview/faq/configuration "Configuration") section.

Additional Resources
--------------------

If you are new to using ActiveMQ, running the [Examples](examples) is a good next step to learn more about ActiveMQ.
