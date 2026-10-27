---
title: "CMS API 1.2 Download"
---

# CMS API 1.2 Download

New and Noteworthy
------------------

This is a minor release of the C++ Messaging Service (CMS). This release is was done as part of releasing ActiveMQ-CPP 2.1.2 and contains the following updates:

* Made read methods of BytesMessage const so that asynchronous consumers (onMessage) don't have to use const_cast.

API
---

Check out the API for this release [here](pathname:///components/cms/api_docs/cms-1.2/index.html)

Download Here
-------------

|Description|Download Link|PGP Signature file of download|
|---|---|---|
|Source code for Windows|[cms-1.0-src.zip](http://archive.apache.org/dist/activemq/activemq-cpp/source/cms-1.0-src.zip)|[cms-1.0-src.zip.asc](http://archive.apache.org/dist/activemq/activemq-cpp/source/cms-1.0-src.zip.asc)|
|Source code for Unix|[cms-1.0-src.tar.gz](http://archive.apache.org/dist/activemq/activemq-cpp/source/cms-1.0-src.tar.gz)|[cms-1.0-src.tar.gz.asc](http://archive.apache.org/dist/activemq/activemq-cpp/source/cms-1.0-src.tar.gz.asc)|

SVN Tag Checkout
----------------
```
svn co https://svn.apache.org/repos/asf/activemq/activemq-cpp/tags/cms-1.2/
```

Changelog
---------

The following issues were resolved as part of the [ActiveMQ-CPP 2.1.2 Release](/components/cms/download/212-release)

 

