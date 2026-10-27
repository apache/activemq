---
title: "CMS 3.3.0 Download"
---

# CMS 3.3.0 Download

New and Noteworthy
------------------

This is a major release of the ActiveMQ-CPP client which includes some API changes and several new features and improvements. Some additional methods have been added to the CMS API to make it easier to use. Some highlights for this release:

*   Solved some deadlock issues that could occur during failover when using asynchronous consumers.
*   Synchronous Consumers now get an exception if they call receive when the Connection has failed.
*   CMS API now provides an XA domain for linking the CMS client into XA transactions.
*   The Failover Transport now supports the connection rebalancing feature in ActiveMQ.
*   Fixes in the Stomp handling allow the client to work with ActiveMQ Apollo.
*   Better message redelivery processing for Transacted consumers.

**NOTE:** Compatible with ActiveMQ Broker versions in the 4.X and 5.X family

API
---

This release is based on the CMS 2.2 API.

Check out the API for this release [here](pathname:///components/cms/api_docs/activemqcpp-3.3.0/html/index.html)

Download Here
-------------

|Description|Download Link|PGP Signature file of download|
|---|---|---|
|Source code for Windows|[activemq-cpp-library-3.3.0.src.zip](http://archive.apache.org/dist/activemq/activemq-cpp/source/activemq-cpp-library-3.3.0-src.zip)|[activemq-cpp-library-3.3.0-src.zip.asc](http://archive.apache.org/dist/activemq/activemq-cpp/source/activemq-cpp-library-3.3.0-src.zip.asc)
|Source code for Unix (gzipped)|[activemq-cpp-library-3.3.0-src.tar.gz](http://archive.apache.org/dist/activemq/activemq-cpp/source/activemq-cpp-library-3.3.0-src.tar.gz)|[activemq-cpp-library-3.3.0-src.tar.gz.asc](http://archive.apache.org/dist/activemq/activemq-cpp/source/activemq-cpp-library-3.3.0-src.tar.gz.asc)
|Source code for Unix (bz2)|[activemq-cpp-library-3.3.0-src.tar.bz2](http://archive.apache.org/dist/activemq/activemq-cpp/source/activemq-cpp-library-3.3.0-src.tar.bz2)|[activemq-cpp-library-3.3.0.src.tar.bz2.asc](http://archive.apache.org/dist/activemq/activemq-cpp/source/activemq-cpp-library-3.3.0-src.tar.bz2.asc)

SVN Tag Checkout
----------------
```
svn co https://svn.apache.org/repos/asf/activemq/activemq-cpp/tags/activemq-cpp-3.3.0/
```

Changelog
---------

For a more detailed view of new features and bug fixes, see the [release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12311207&version=12315668)

