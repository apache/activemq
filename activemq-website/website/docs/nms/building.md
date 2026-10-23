---
title: "Building NMS"
sidebar_position: 6
---

Currently our main build tool for NMS is [NAnt](http://nant.sourceforge.net/).

Setup For *NIX Users

*   Install [Mono](http://www.mono-project.com)
*   On some operating systems (e.g. OS X), you may need to point the **PKG_CONFIG_PATH** environment variable at the mono packages

    `export PKG_CONFIG_PATH="/Library/Frameworks/Mono.framework/Versions/Current/lib/pkgconfig/:${PKG_CONFIG_PATH}"`

*   Depending on your installation of [Mono](http://www.mono-project.com), you may need additional steps to get NAnt going on OS X. The more recent versions of the Mono installer for OS X seem to set things up properly. However, if you are having problems running NAnt, try the following:
    *   download the binary of [NAnt](http://nant.sourceforge.net/)
    *   create a script file called nant
    *   chmod a+x nant
    *   put the following into the script...

        ```
        #!/bin/sh
        exec /usr/bin/mono /path/to/nant/NAnt.exe "$@"
        ```

Thanks to the hints from [this page](http://cs-sdl.sourceforge.net/index.php/The_absolute_newbies_guide_to_SDL.NET).


### Building NMS with Nant

There are two steps to build the NMS project code, we'll look at each now.

#### Setting up your NAnt Repository.

Before you can build the NMS API and the various provider libraries you need to initialize your local NAnt repository so that you have all the required dependencies available for the provider's NAnt build scripts to pull in during their builds. We maintain a tree in the NMS SVN repo with all the required dependencies which allow you to easily populate you NAnt repo. You can check out this tree by executing the following:
```
svn co https://svn.apache.org/repos/asf/activemq/activemq-dotnet/vendor/
```

Now that you have the vendor tree checkout out you can install the various libaries into you NAnt repo by running each one's build script and with the 'install' target. You will need nant-0.91 or higher. The version 0.90 won't work. Let's take the NUnit testing framework libraries as an example, you install them like so:
```
cd ./activemq-dotnet/vendor/NUnit/2.5.8
nant install
```

Install each of the vendor libraries using the same process and then you are ready to build the NMS libraries.

#### Building the NMS API and Provider libraries.

To build the code with Nant you will need to build the individual modules, starting with the main NMS module first. Following are the steps needed to build all of the modules, but depending on which provider you are using, you may only need to build the main Apache.NMS module and then your specific provider module. For example, if you are only working with ActiveMQ, then you would need to build the Apache.NMS module and the Apache.NMS.ActiveMQ provider module. Here are the steps to build all of the modules for the trunk version. Modify the paths to build a particular tag release (e.g., change the **trunk** folder to **tags\1.0.0**):

```
cd activemq-dotnetcd Apache.NMS\trunk
nant

cd ..\..\Apache.NMS.ActiveMQ\trunk
nant

cd ..\..\Apache.NMS.AMQP\trunk
nant

cd ..\..\Apache.NMS.EMS\trunk
nant

cd ..\..\Apache.NMS.MQTT\trunk
nant

cd ..\..\Apache.NMS.MSMQ\trunk
nant

cd ..\..\Apache.NMS.Stomp\trunk
nant

cd ..\..\Apache.NMS.WCF\trunk
nant

cd ..\..\Apache.NMS.ZMQ\trunk
nant
```

This will build all of the providers. The only module that needs to be built first is the Apache.NMS module, as all of the other provider modules depend upon that module. **IMPORTANT:** In order to build the Apache.NMS.EMS module, you must have a licensed copy of the TIBCO.EMS.dll. This module is not available from The Apache Software Foundation.

> Running the Unit Tests
> 
> The unit tests depend on an ActiveMQ broker running on tcp://localhost:61616 so that the test cases can connect to ActiveMQ and send and receive messages etc. So if you have a distribution of ActiveMQ, [run the broker](http://activemq.apache.org/getting-started.html#GettingStarted-StartingActiveMQ) first before testing Apache.NMS.ActiveMQ.

If you want to run a build with the unit tests (assuming you have a broker running) then type the following from the **trunk** folder of the provider module:

```
nant test
```

Alternatively, you can use the NUnit graphical interface to load the appropriate module's nunit project file. Each project includes a file named Apache.NMS._modulename_.Test.nunit, where _modulename_ matches the provider module's name (e.g., ActiveMQ, AMQP, EMS, MQTT, MSMQ, Stomp, WCF, or ZMQ). This file can be loaded into the NUnit's graphical application to run all of the unit tests and see their output.

## IDE Tips

If you are on windows then you probably want to use Visual Studio 2008; we highly recommend using [ReSharper](http://www.jetbrains.com/resharper/) as well, - its a shame there is not a distro for OS X and Linux.

### Using [MonoDevelop](http://monodevelop.com/) to edit C#

If you are on OS X or Linux (and even Windows nowdays) we recommend the use of [MonoDevelop](http://monodevelop.com) for editing, refactoring and running the code. MonoDevelop can load the NMS Visual Studio Project files, and is integrated with the Mono debugger as well.

### Using Eclipse to edit C#

You can use Eclipse as your IDE to edit C# code.

If you want to use Eclipse there are the following two C# eclipse plugins we are aware of

* [Emonic](http://emonic.sourceforge.net/)
* [BlackSun](http://black-sun.sourceforge.net/)
* [Improve C#](http://www.improve-technologies.com/alpha/esharp/)

We tend to use BlackSun as it has auto-format and outline view support along with compiler warnings (though you might want to change the default font size).
