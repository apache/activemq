---
title: "CMS 3.4.2 Download"
---

# CMS 3.4.2 Download

New and Noteworthy
------------------

This is a patch release of the ActiveMQ-CPP client which includes some fixes for issues found since the v3.4.1 release.

*   Some build issues on various platforms were resolved.
*   A problem with setting the username / password from the Connection Factory was fixed.
*   A problem with expired messages and DLQ processing was addressed.
*   An issue with the failover transport and hangs in Connection start() was fixed.
*   A couple of race conditions on Connection close() were addressed.

**NOTE:** Compatible with ActiveMQ Broker versions in the 4.X and 5.X family

API
---

This release is based on the CMS 2.4 API.

Check out the API for this release [here](pathname:///components/cms/api_docs/activemqcpp-3.4.0/html/index.html)

Download Here
-------------

|Description|Download Link|PGP Signature file of download|
|---|---|---|
|Source code for Windows|[activemq-cpp-library-3.4.2.src.zip](http://archive.apache.org/dist/activemq/activemq-cpp/source/activemq-cpp-library-3.4.2-src.zip)|[activemq-cpp-library-3.4.2-src.zip.asc](http://archive.apache.org/dist/activemq/activemq-cpp/source/activemq-cpp-library-3.4.2-src.zip.asc)
|Source code for Unix (gzipped)|[activemq-cpp-library-3.4.2-src.tar.gz](http://archive.apache.org/dist/activemq/activemq-cpp/source/activemq-cpp-library-3.4.2-src.tar.gz)|[activemq-cpp-library-3.4.2-src.tar.gz.asc](http://archive.apache.org/dist/activemq/activemq-cpp/source/activemq-cpp-library-3.4.2-src.tar.gz.asc)
|Source code for Unix (bz2)|[activemq-cpp-library-3.4.2-src.tar.bz2](http://archive.apache.org/dist/activemq/activemq-cpp/source/activemq-cpp-library-3.4.2-src.tar.bz2)|[activemq-cpp-library-3.4.2.src.tar.bz2.asc](http://archive.apache.org/dist/activemq/activemq-cpp/source/activemq-cpp-library-3.4.2-src.tar.bz2.asc)

SVN Tag Checkout
----------------
```
svn co https://svn.apache.org/repos/asf/activemq/activemq-cpp/tags/activemq-cpp-3.4.2/
```

Changelog
---------

For a more detailed view of new features and bug fixes, see the [release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12311207&version=12319269)

