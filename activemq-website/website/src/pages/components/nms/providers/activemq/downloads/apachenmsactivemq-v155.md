---
title: Download Apache.NMS.ActiveMQ v1.5.5
---

# Download Apache.NMS.ActiveMQ v1.5.5

New and Noteworthy
------------------

This release adds some great new fixes to the NMS.ActiveMQ API.

*   Sending to non-existent temp queue causes consumer to shutdown
*   Consumers frequently fail to reconnect after broker outage/failover.
*   FailoverTransport doesn't trigger a reconnect on send of a Tracked Command.
*   ActiveMQ.NMS hangs sometimes due to concurrency problems while accessing static IDictionary
*   Apache.NMS.ActiveMQ discovery protocol throwing ArgumentOutOfRangeException

##### NMS.ActiveMQ Updates

This release focuses on stability fixes to the ActiveMQ provider. No new features were added in this release. See the release notes below for a list of issues that were fixed in this release.

API Documentation
-----------------

Refer to the API for this release [here](/components/nms/nms-api)

Download Here
-------------

Description|Download Link|PGP Signature File|Version
---|---|---|---
Apache.NMS.ActiveMQ Source code|[Apache.NMS.ActiveMQ-1.5.5-src.zip](https://archive.apache.org/dist/activemq/apache-nms/1.5.0/Apache.NMS.ActiveMQ-1.5.5-src.zip)|[Apache.NMS.ActiveMQ-1.5.5-src.zip.asc](https://archive.apache.org/dist/activemq/apache-nms/1.5.0/Apache.NMS.ActiveMQ-1.5.5-src.zip.asc)|1.5.5.2676
Apache.NMS.ActiveMQ Binary Assemblies|[Apache.NMS.ActiveMQ-1.5.5-bin.zip](https://archive.apache.org/dist/activemq/apache-nms/1.5.0/Apache.NMS.ActiveMQ-1.5.5-bin.zip)|[Apache.NMS.ActiveMQ-1.5.5-bin.zip.asc](https://archive.apache.org/dist/activemq/apache-nms/1.5.0/Apache.NMS.ActiveMQ-1.5.5-bin.zip.asc)|1.5.5.2676

SVN Tag Checkout
----------------

svn co https://svn.apache.org/repos/asf/activemq/activemq-dotnet/Apache.NMS.ActiveMQ/tags/1.5.5/

Changelog
---------

For a more detailed view of new features and bug fixes, see the [release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12311201&styleName=Html&version=12320740)
