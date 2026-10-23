---
title: "Building ActiveMQ-CPP"
sidebar_position: 2
---
## Dependencies

---

### libuuid

On *nix sytems, the build requires the libuuid library that is part of the e2fsprogs package and is available from [http://e2fsprogs.sourceforge.net/](http://e2fsprogs.sourceforge.net/) which is not always installed by default.

On Fedora/Red Hat/openSUSE, you should install the e2fsprogs and e2fsprogs-devel packages. On Debian/Ubuntu, you should install uuid and uuid-dev packages.

### CppUnit

The package contains a complete set of CppUnit tests. In order for you to build an run the tests, you will need to download and install the CppUnit library. See [http://cppunit.sourceforge.net/cppunit-wiki](http://cppunit.sourceforge.net/cppunit-wiki)

On Fedora/Red Hat/openSUSE, you should install the cppunit and cppunit-devel packages. On Debian/Ubuntu, you should install the build-essential, libcppunit and libcppunit-dev packages.

Make sure that the paths to the installed CppUnit library and includes are visible in your current shell before you try building the tests.

Windows users will need to build the CppUnit library using the CppUnit MSVC project files. A discussion of the build process can be found on the CppUnit wiki under [CppUnit build instructions](http://cppunit.sourceforge.net/cppunit-wiki/BuildingCppUnit1) this covers both MSVC along with many other platforms and tool suites.

### APR

With versions of ActiveMQ-CPP 2.2 and later, we have a dependency on the [Apache Portable Runtime](http://apr.apache.org) project. You'll need to install APR on your system before you'll be able to build ActiveMQ-CPP.

##### Note to Windows Vista/MSVC++ 2008 Users

The Vista SDK has introduced some build problems for APR. For a work-around, follow the instructions [here](https://issues.apache.org/bugzilla/show_bug.cgi?id=40398).

## Building on *nix (Unix/Linux/OS X/Cygwin)

---

### GNU Build System (for building on *nix)

To Generate the ./configure script use to create the Makefiles, you need the following software installed:

|Tool|Recommended Version|
|---|---|
|autoconf|>= 2.59|
|automake|>= 1.9.6|
|libtool|>= 1.5.22|

You can also install the Doxygen and Graphviz packages if you want to generate the API documentation in HTML format.

### Building

This assumes you have all of the project dependencies installed. We're now ready to create the configure script. To do this, run:

```
./autogen.sh
```

> You may see the following warnings when running this command:
> 
> ```
> src/test-integration/Makefile.am:44: 'CXXFLAGS' is a user variable, you should not override it;  
> src/test-integration/Makefile.am:44: use 'AM_CXXFLAGS' instead.  
> src/test/Makefile.am:104: 'CXXFLAGS' is a user variable, you should not override it;  
> src/test/Makefile.am:104: use 'AM_CXXFLAGS' instead.
> ```

These can be ignored. We override `CXXFLAGS` in the makefiles for the unit and integration tests in order to suppress compiler warnings.

This should be run the first time and anytime you change configure.ac or any of the Makefile.am files.

> ##### Solaris 10 Note
>
> CppUnit might not build until you correct the file libstdc++.la to contain the correct data, see this discussion.
>
> [http://forum.sun.com/jive/thread.jspa?threadID=73150](http://forum.sun.com/jive/thread.jspa?threadID=73150)

The configure script will customize the way the software is built and installed into your system along with detecting the available libraries that have been installed. To use the default configuration just run:

```
./configure
```

For more help on how to customize the build configuration, run:

```
./configure --help
```

Once the configure script has run successfully, you are ready to build. Run:

```
make
```

This will build all of the core ActiveMQ CPP source code. To build and install the code into the system directories, run:


You will have to become the superuser in order to be able to install the files.

### Doxygen

To generate the doxygen documentation for the project, just run:

```
make doxygen-run
```

### Running Tests

#### Unit Tests

In order to build and run the suite of unit tests, run:

```
make check
```

This will verify that the library is functioning correctly on the target platform. In addition, it will generate the integration tests binary.

#### Integration Tests

The library also contains a set of tests that are run against a real AMQ broker. These allow you to validate this distribution of ActiveMQ CPP against your broker. Running these without a broker will result in failed tests. The tests currently hard-code the broker url to be tcp://localhost:61613 for stomp and tcp://localhost:61616 for openwire.

The integration tests are built via "make check". To run them, first start a broker and then

```
cd src/test-integration
./activemq-test-integration
```

This will take quite some time to complete, so be patient.

### Example

There is an example application that ships with the distribution in src/examples. The example is compiled by default with the "make" command, but can easily be compiled manually using the command similar to the following (varies by platform).

```
g++ -o main -pthread -I ../main main.cpp ../../src/main/.libs/libactivemq-cpp.a -luuid -I /usr/include/apr-1.0/ -lssl -lcrypto -lapr-1
```

## Building on Windows with MSVC

---

We support using the GNU compiler on Windows, using the Cygwin package. However we also support using the MSVC compiler on Windows.

There are a couple or things that you will need to setup to ensure that the MSVC compile succeeds.

### Including Dependencies

The Project files reference the libraries described in the Dependencies section. To build successfully, you must either place these libraries in a directory listed in the project settings, or add a new location for your library directories.

### MSVC >= 2008

If you're using MSVC >= 2008, it comes with the Windows SDK (so you don't need to download it separately).

For ActiveMQ-CPP >= 2.2, you will have to follow some special instructions for building APR, duscussed above.

### Non-Vista/MSVC < 2008

If you are using Visual C++ < 2008, you need to download and install the Platform SDK if you don't have it installed already (the Platform SDK is called the "Windows SDK" since the release of Windows Vista). Once installed, do the following:

*   Ensure that the path to your MSVC install is set in the PATH env variable. You can test this by typing cl.exe at the command line, if you get an error complaining that it is not found, then you'll need to fix your PATH. Alternatively, you can set up the necessary environment variables by running C:\Program Files\Microsoft Visual Studio 8\Common7\Tools\vsvars32.bat.
*   Set the INCLUDE env variable to include the path to your MSVC includes, and the platform SDK includes. For example:

        INCLUDE = D:\Program Files\Microsoft Visual Studio 8\VC\include;D:\Program Files\Microsoft Platform SDK\Include\*

*   Set the LIB env variable to include the path to your MSVC libs, and the Platform SDK libs. For example:

        LIB = D:\Program Files\Microsoft Visual Studio 8\VC\lib;D:\Program Files\Microsoft Platform SDK\Lib
