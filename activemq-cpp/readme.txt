--------------------------------------------------------------------------
ActiveMQ CPP Library
--------------------------------------------------------------------------

This library provides a JMS like interface to an ActiveMQ broker in c++.

Currently the Library only supports the Stomp protocol, future versions 
will contain support for openwire.

UNIT Tests
--------------------------------------------------------------------------

The package contains a complete set of cppunit tests.  In order for you
to build an run the tests, you will need to download and install the 
cppunit suite.  

http://cppunit.sourceforge.net/cppunit-wiki

or on Fedora type 

yum install cppunit*

Make sure that the path to the installed cpp unit library and includes is 
visible in your current shell before you try building the tets.

Integration Tests
--------------------------------------------------------------------------

The library also contains a set of tests that are run against a real AMQ
broker.  Running these without a broker will result in failed tests.
The tests currently hardcode the broker url to be tcp://127.0.0.1:61613, 
you can change this by changing the declaration in IntegrationCommon.cpp
in the test-integration src tree.

Notes for Windows users
--------------------------------------------------------------------------

The builds support using the GNU compiler on Windows, we used the MinGW
package.  There is an issues still outstanding with this in that the sockets
break for no reason when built this way.  We therefore suggest that you
stick with using the MSVC compiler when on windows.

There are a couple or things that you will need to setup to ensure that the
MSVC compile succeeds.

* You need to download and install the Platform SDK if you don't have it 
  installed already.
* Ensure that the path to you MSVC install is set in the PATH env variable.
  you can tests this buy typing cl.exe at the command line, if you get an
  error complaining that its not found, then setup you PATH correctly.
* Set the INCLUDE env variable to include the path to your MSVC includes,
  and the platform SDK includes.
  
  i.e. INCLUDE = D:\Program Files\Microsoft Visual Studio 8\VC\include;D:\Program Files\Microsoft Platform SDK\Include

* Set the LIB env variable to include the path to your MSVC libs, and the
  Platform SDK libs.

  i.e. LIB = D:\Program Files\Microsoft Visual Studio 8\VC\lib;D:\Program Files\Microsoft Platform SDK\Lib

Maven Builds
--------------------------------------------------------------------------

The pacakge currently supports building the library only using maven.

The Mojo Native plugin (from the MOJO maven plugins site) is required.

http://mojo.codehaus.org/maven-native/native-maven-plugin/introduction.html

On the windows platform is was necessay to download the source for this
plugin and build it locally, this shouldn't be necessary on non-windows
platforms, but if you have problems, try that first.

You can get the latest source via subversion:

svn co http://svn.codehaus.org/mojo/trunk/mojo/maven-native

Once you have downloaded the source, install the plugin into your local
repository via: mvn install

Using Maven with activemq-cpp

* type mvn package

This will build the library using the default target for the platform 
you are on, which is release, and the gnu compiler for unix platforms, or
the MSVC compiler on windows platforms.

Makefile Builds
--------------------------------------------------------------------------

The Makefile provided requires some env variable to be set

OSTYPE:  This is the OS you are on and is reflected in the names of the
         makefiles.  Currently your choices are Linux or Windows, both use
         the GNU compiler.  

CONFIG:  This is the build Mode you want to execute, i.e. debug or release.

MAKESUPPORT_HOME:  Path to the folder where the Makefiles are stored.

There are three targets available in the Makefile, lib, test, and integration
whose output is fairly obvious.

Using the Makefile:

* type make to build all targets: lib, tests and integration
* type make < Target Name > to build only the target you need.
* type make clean to remove all of the object, library, and executable files.
