---
title: "CMS 3.5.0 Download"
---

# CMS 3.5.0 Download

New and Noteworthy
------------------

This is a new Major release of ActiveMQ-CPP with several new APIs and internal changes meant to improve overall stability of the C++ client. Besides a large amount of bug fixing and memory fixing this release also features a lot of new features in the CMS API:

*   Added Message Transformation capabilities for incoming and outgoing messages.
*   You can now query for the type of a Message property so you can choose the best getter method.
*   MapMessage and StreamMessage object now expose an API to find out what type a value is before you try to get a value using one of the getter methods.
*   Polling consumers now can set a Message available listener.
*   Stomp clients can configure the destination prefixes for the Destination types (Queues, Topics...).
*   StreamMessage now has a reset method to allow object reuse.
*   Added an AdvisorySupport class to make dealing with advisory topics easier.
*   Added support for non-blocking sends to Message Producers.  

**NOTE:** Compatible with ActiveMQ Broker versions in the 4.X and 5.X family

API
---

This release is based on the CMS 2.4 API.

Check out the API for this release [here](pathname:///components/cms/api_docs/activemqcpp-3.4.0/html/index.html)

Download Here
-------------

|Description|Download Link|PGP Signature file of download|
|---|---|---|
|Source code for Windows|[activemq-cpp-library-3.5.0.src.zip](http://archive.apache.org/dist/activemq/activemq-cpp/source/activemq-cpp-library-3.5.0-src.zip)|[activemq-cpp-library-3.5.0-src.zip.asc](http://archive.apache.org/dist/activemq/activemq-cpp/source/activemq-cpp-library-3.5.0-src.zip.asc)
|Source code for Unix (gzipped)|[activemq-cpp-library-3.5.0-src.tar.gz](http://archive.apache.org/dist/activemq/activemq-cpp/source/activemq-cpp-library-3.5.0-src.tar.gz)|[activemq-cpp-library-3.5.0-src.tar.gz.asc](http://archive.apache.org/dist/activemq/activemq-cpp/source/activemq-cpp-library-3.5.0-src.tar.gz.asc)
|Source code for Unix (bz2)|[activemq-cpp-library-3.5.0-src.tar.bz2](http://archive.apache.org/dist/activemq/activemq-cpp/source/activemq-cpp-library-3.5.0-src.tar.bz2)|[activemq-cpp-library-3.5.0.src.tar.bz2.asc](http://archive.apache.org/dist/activemq/activemq-cpp/source/activemq-cpp-library-3.5.0-src.tar.bz2.asc)

SVN Tag Checkout
----------------
```
svn co https://svn.apache.org/repos/asf/activemq/activemq-cpp/tags/activemq-cpp-3.5.0/
```

Changelog
---------

For a more detailed view of new features and bug fixes, see the [release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12311207&version=12316380)

