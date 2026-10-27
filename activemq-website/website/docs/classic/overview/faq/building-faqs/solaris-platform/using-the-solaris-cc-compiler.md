---
title: "Using the Solaris CC compiler"
slug: /using-the-solaris-cc-compiler
---

Use the Correct Compiler

This Entry only applies to the Sun Studio 11 and above compiler. The Sun Workshop compiler does not seem to support building this library.

In order to use the Solaris compiler you must set some environment variables and then run the autogen.sh and configure scripts.

The following variable need to be set

*   CC = cc
*   CXX = CC

Underneath the configure script will append -library=stlport4 to the CC command so that the Sun Compiler will use STLPort instead of the broken version of STL that they link in by default and ActiveMQ-CPP uses features that it does not support. By using the STLPort library you get access to the features we need, but you must compile your application with STLPort as well to insure compatibility with our library.

So in you own makefiles you must pass -library=stlport4 to CC when you build and link.

You can find out more from Sun on this at [STL Library Comparison](http://www.oracle.com/technetwork/server-storage/solaris/cmp-stlport-libcstd-142559.html)

