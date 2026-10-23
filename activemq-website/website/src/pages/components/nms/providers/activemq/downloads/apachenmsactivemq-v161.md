---
title: Download Apache.NMS.ActiveMQ v1.6.1
---

# Download Apache.NMS.ActiveMQ v1.6.1

New and Noteworthy
------------------

This is a bugfix release for the NMS.ActiveMQ client library. Along with some bug fixes though we've also added support for better discovery transport agents and we now support HTTP discovery using the ActiveMQ broker's HTTP discovery registry.

##### NMS.ActiveMQ Updates

The change log for this release is below.

*   Don't fire transport interrupted event when no resume is possible
*   dispatch paused, waiting for outstanding dispatch interruption processing to complete..
*   Better logging for XATransaction
*   Improve the discovery transport to be more robust and allow easier plugin of new agents.
*   Add an HTTP based discovery agent to the discovery transport feature.
*   Individual Acknowledgement seems not working in NMS

API Documentation
-----------------

Refer to the API for this release [here](/components/nms/nms-api)

Download Here
-------------

Description|Download Link|PGP Signature File|Version
---|---|---|---
Apache.NMS.ActiveMQ Source code|[Apache.NMS.ActiveMQ-1.6.1-src.zip](https://archive.apache.org/dist/activemq/apache-nms/1.6.0/Apache.NMS.ActiveMQ-1.6.1-src.zip)|[Apache.NMS.ActiveMQ-1.6.1-src.zip.asc](http://www.apache.org/dist/activemq/apache-nms/1.6.0/Apache.NMS.ActiveMQ-1.6.1-src.zip.asc)|1.6.0.3208
Apache.NMS.ActiveMQ Binary Assemblies|[Apache.NMS.ActiveMQ-1.6.1-bin.zip](https://archive.apache.org/dist/activemq/apache-nms/1.6.0/Apache.NMS.ActiveMQ-1.6.1-bin.zip)|[Apache.NMS.ActiveMQ-1.6.1-bin.zip.asc](http://www.apache.org/dist/activemq/apache-nms/1.6.0/Apache.NMS.ActiveMQ-1.6.1-bin.zip.asc)|1.6.0.3208

SVN Tag Checkout
----------------

svn co [https://svn.apache.org/repos/asf/activemq/activemq-dotnet/Apache.NMS.ActiveMQ/tags/1.6.1/](https://svn.apache.org/repos/asf/activemq/activemq-dotnet/Apache.NMS.ActiveMQ/tags/1.6.1/)

Changelog
---------

For a more detailed view of new features and bug fixes, see the [release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12311201&version=12324792)
