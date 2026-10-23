---
title: "CMS 2.2 Download"
---

# CMS 2.2 Download

New and Noteworthy
------------------

This release has seen a large restructuring of the codebase to separate out to platform code into a new package called decaf. We are now making use of the APR and APR-Util libraries for platform independence. Also many new tests and bug fixes have made their way into this release.

Some notable new features in this release

*   CmsTemplate - Similar to Spring's JmsTemplate.
*   Prefetch of size zero now supported.
*   The URI can now contain environment variables.

**NOTE:** Compatable with ActiveMQ Broker versions in the 4.0 and 5.0 family

API
---


Download Here
-------------

|Description|Download Link|PGP Signature file of download|
|---|---|---|
|Source code for Windows|[activemq-cpp-2.2.src.zip](http://archive.apache.org/dist/activemq/activemq-cpp/source/activemq-cpp-2.2-src.zip)|[activemq-cpp-2.2-src.zip.asc](http://archive.apache.org/dist/activemq/activemq-cpp/source/activemq-cpp-2.2-src.zip.asc)
|Source code for Unix (gzipped)|[activemq-cpp-2.2-src.tar.gz](http://archive.apache.org/dist/activemq/activemq-cpp/source/activemq-cpp-2.2-src.tar.gz)|[activemq-cpp-2.2-src.tar.gz.asc](http://archive.apache.org/dist/activemq/activemq-cpp/source/activemq-cpp-2.2-src.tar.gz.asc)

SVN Tag Checkout
----------------
```
svn co https://svn.apache.org/repos/asf/activemq/activemq-cpp/tags/activemq-cpp-2.2/
```

Changelog
---------

For a more detailed view of new features and bug fixes, see the [release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12311207&version=12315649)

