---
title: Download Apache.NMS.ActiveMQ v1.5.0
---

# Download Apache.NMS.ActiveMQ v1.5.0

New and Noteworthy
------------------

This release adds some great new features to the NMS ActiveMQ library and fixes several bugs found in the 1.4.0 release.

##### New in this version

*   New startupMaxReconnectAttempts option in the failover transport.
*   Fixed a memory leak in the Inactivity Monitoring code.
*   Fixed a bug that could cause the Failover to not work.
*   Fixed a bug that caused SSL Certs to not be loaded in some cases.
*   Several other bugfixes for issues found since v1.4.0 was released.
*   Added support for participation in .NET Distributed Transactions (MSDTC).
*   Added .NET 4.0 build.
*   Many more fixes and internal improvements.

API Documentation
-----------------

Refer to the API for this release [here](/components/nms/nms-api)

Apache.NMS.ActiveMQ Client Downloads
------------------------------------

Description|Download Link|PGP Signature File|Version
---|---|---|---
Apache.NMS.ActiveMQ Source code|[Apache.NMS.ActiveMQ-1.5.0-src.zip](https://archive.apache.org/dist/activemq/apache-nms/1.5.0/Apache.NMS.ActiveMQ-1.5.0-src.zip)|[Apache.NMS.ActiveMQ-1.5.0-src.zip.asc](https://archive.apache.org/dist/activemq/apache-nms/1.5.0/Apache.NMS.ActiveMQ-1.5.0-src.zip.asc)|1.5.0.2235
Apache.NMS.ActiveMQ Binary Assemblies|[Apache.NMS.ActiveMQ-1.5.0-bin.zip](https://archive.apache.org/dist/activemq/apache-nms/1.5.0/Apache.NMS.ActiveMQ-1.5.0-bin.zip)|[Apache.NMS.ActiveMQ-1.5.0-bin.zip.asc](https://archive.apache.org/dist/activemq/apache-nms/1.5.0/Apache.NMS.ActiveMQ-1.5.0-bin.zip.asc)|1.5.0.2235

  

**NOTE:** Compatible with ActiveMQ Broker versions in the 4.X and 5.X family.

SVN Tag Checkout
----------------

svn co https://svn.apache.org/repos/asf/activemq/activemq-dotnet/Apache.NMS.ActiveMQ/tags/1.5.0/

Changelog
---------

For a more detailed view of new features and bug fixes, see the [release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12311201&styleName=Html&version=12315640)
