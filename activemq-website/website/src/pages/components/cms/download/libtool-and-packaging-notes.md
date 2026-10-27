---
title: "ActiveMQ-CPP, libtool and packaging notes"
---

ActiveMQ-CPP, libtool and packaging notes
=========================================

### Introduction

This page attempts to summarise information to be considered

*   when configuring libtool
*   when packaging ActiveMQ for distribution on Debian, Red Hat, Solaris.

### Concepts

Product version numbers

*   Please see the notes on [ActiveMQ-CPP product version number](/components/cms/download/activemq-cpp-product-version-number)

SONAME

*   At runtime (when dynamic linking occurs) the SONAME embedded in the library must match the SONAME expected by the application.
*   To see the SONAME embedded in the library, `objdump -p libactivemq-cpp-2.1.2.so | grep SONAME`
*   To see the SONAMEs expected by an application, `objdump -p | grep NEEDED`
*   By default, libtool will use the library filename as the SONAME
*   Libtool's default behaviour can be modified by the soname_spec variable
*   Possible SONAMEs fro ActiveMQ CPP:
    *   libactivemq-cpp-2.1.2.so.0 (each executable will depend on a specific build and will refuse to work with any other build)
    *   libactivemq-cpp.so.0 (only one version can be installed at the same time)
    *   libactivemq2-cpp.so.0 (so that 2.x and 3.x can be installed simultaneously)

libtool -release

*   the -release argument tells libtool the release number
*   the release number could be the major version number, the complete version number, or the SVN revision number
*   libtool believes that any two releases are NOT compatible - for example, if an executable expects release number 5, and the library has release number 4 or 6, the executable will refuse to run

libtool -version-info current:revision:age

*   Allows binary compatibility between versions
*   The number at the end of the filename is the interface version number
*   Interface version number is NOT THE SAME as the product version number
*   Interface version number should increment each time the `interface' changes (new function prototypes, new function arguments, change in wire protocol, etc)
*   The `age' number is used to indicate backwards compatibility - if age=1, then this interface is backwards compatible with the previous version only, if age=2, then this interface is backwards compatible with the 2 previous versions

Debian package name

*   The package name includes the following from libtool: release number and interface number
*   for example, if interface version = 4, then the package name = libactivemq-cpp4
*   The product version number is not part of the Debian package name - it is used elsewhere in Debian's control files

