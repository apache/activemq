---
title: "CMS 3.7.1 Download"
---

# CMS 3.7.1 Download

New and Noteworthy
------------------

This is a new patch release of ActiveMQ-CPP, a lot of bugs have been fixed that were found in the v3.7.0 release which should reduce memory consumption and improve overall stability. Several compilation fixes were made as well so things should be better on a number of platforms. Fixes of note:

*   3.7.1 does not compile with gcc-4.4.7 on CentOS-6
*   APR-Util header include missed in latest release.
*   CMS FailoverTransport Leaks Socket Descriptors
*   closing a connection stalled in start because of failover should stop the transport safely.
*   Compilation Error Fix for Sun Studio under Solaris 10
*   Exception lifetime confusion can cause the application to crash
*   Connection didn't switch to the slave broker when the master broker is down
*   Commiting a session with a deleted consumer causes access violation
*   Compilation of 3.7.1 fails for Linux systems (Redhat 5.8 and SuSE SLES 10)
*   Cient doesn't work on Linux Red Hat 6.4 systems, fails when setting thread priority

**NOTE:** Compatible with ActiveMQ Broker versions in the 4.X and 5.X family

API
---

This release is based on the CMS 3.1 API.

Check out the API for this release [here](pathname:///components/cms/api_docs/activemqcpp-3.6.0/html/index.html)

Download Here
-------------

|Description|Download Link|PGP Signature file of download|
|---|---|---|
|Source code for Windows|[activemq-cpp-library-3.7.1.src.zip](http://archive.apache.org/dist/activemq/activemq-cpp/source/activemq-cpp-library-3.7.1-src.zip)|[activemq-cpp-library-3.7.1-src.zip.asc](http://archive.apache.org/dist/activemq/activemq-cpp/source/activemq-cpp-library-3.7.1-src.zip.asc)
|Source code for Unix (gzipped)|[activemq-cpp-library-3.7.1-src.tar.gz](http://archive.apache.org/dist/activemq/activemq-cpp/source/activemq-cpp-library-3.7.1-src.tar.gz)|[activemq-cpp-library-3.7.1-src.tar.gz.asc](http://archive.apache.org/dist/activemq/activemq-cpp/source/activemq-cpp-library-3.7.1-src.tar.gz.asc)
|Source code for Unix (bz2)|[activemq-cpp-library-3.7.1-src.tar.bz2](http://archive.apache.org/dist/activemq/activemq-cpp/source/activemq-cpp-library-3.7.1-src.tar.bz2)|[activemq-cpp-library-3.7.1.src.tar.bz2.asc](http://archive.apache.org/dist/activemq/activemq-cpp/source/activemq-cpp-library-3.7.1-src.tar.bz2.asc)

SVN Tag Checkout
----------------
```
svn co https://svn.apache.org/repos/asf/activemq/activemq-cpp/tags/activemq-cpp-3.7.1/
```

Changelog
---------

For a more detailed view of new features and bug fixes, see the [release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12311207&version=12324543)

