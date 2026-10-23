---
title: "CMS 3.2.3 Download"
---

# CMS 3.2.3 Download

New and Noteworthy
------------------

This is a patch release that addresses issues with the 3.2.3 release. This release fixes an additional issue found in unmarshaling MapMessage objects on Windows as well as fixing a threading issue that causes invalid client Ids to be generated if two connections were created from two different threads at the same time. Also a fix to the pkg-config file that is generated for the build was added.

**NOTE:** Compatible with ActiveMQ Broker versions in the 4.X and 5.X family

API
---

This release is based on the CMS 2.2 API.


Download Here
-------------

|Description|Download Link|PGP Signature file of download|
|---|---|---|
|Source code for Windows|[activemq-cpp-library-3.2.3.src.zip](http://archive.apache.org/dist/activemq/activemq-cpp/source/activemq-cpp-library-3.2.3-src.zip)|[activemq-cpp-library-3.2.3-src.zip.asc](http://archive.apache.org/dist/activemq/activemq-cpp/source/activemq-cpp-library-3.2.3-src.zip.asc)
|Source code for Unix (gzipped)|[activemq-cpp-library-3.2.3-src.tar.gz](http://archive.apache.org/dist/activemq/activemq-cpp/source/activemq-cpp-library-3.2.3-src.tar.gz)|[activemq-cpp-library-3.2.3-src.tar.gz.asc](http://archive.apache.org/dist/activemq/activemq-cpp/source/activemq-cpp-library-3.2.3-src.tar.gz.asc)

SVN Tag Checkout
----------------
```
svn co https://svn.apache.org/repos/asf/activemq/activemq-cpp/tags/activemq-cpp-3.2.3/
```

Changelog
---------

For a more detailed view of new features and bug fixes, see the [release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12311207&version=12315666)

