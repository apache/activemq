---
title: Download Apache.NMS.ActiveMQ v1.6.0
---

# Download Apache.NMS.ActiveMQ v1.6.0

New and Noteworthy
------------------

This release adds some great new features and fixes to the NMS.ActiveMQ API.

##### NMS.ActiveMQ Updates

This release adds several new features to the ActiveMQ client and fixes many bugs that have been reported since the last release.

*   Adds support for non-blocking redelivery.
*   Supports priority backups for Failover transport.
*   Supports an Optimized Ack mode for MessageConsumers.
*   Adds Message Audits to prevent duplicate deliveries on Failover.
*   Implements the ISession Recover method.
*   Adds support for purging locally created temp destinations.
*   Properly handles cluster client re-balancing in Failover Transport.
*   Plus a lot more fixes and stability improvements.

API Documentation
-----------------

Refer to the API for this release [here](/components/nms/nms-api)

Download Here
-------------

Description|Download Link|PGP Signature File|Version
---|---|---|---
Apache.NMS.ActiveMQ Source code|[Apache.NMS.ActiveMQ-1.6.0-src.zip](https://archive.apache.org/dist/activemq/apache-nms/1.6.0/Apache.NMS.ActiveMQ-1.6.0-src.zip)|[Apache.NMS.ActiveMQ-1.6.0-src.zip.asc](http://www.apache.org/dist/activemq/apache-nms/1.6.0/Apache.NMS.ActiveMQ-1.6.0-src.zip.asc)|1.6.0.3072
Apache.NMS.ActiveMQ Binary Assemblies|[Apache.NMS.ActiveMQ-1.6.0-bin.zip](https://archive.apache.org/dist/activemq/apache-nms/1.6.0/Apache.NMS.ActiveMQ-1.6.0-bin.zip)|[Apache.NMS.ActiveMQ-1.6.0-bin.zip.asc](http://www.apache.org/dist/activemq/apache-nms/1.6.0/Apache.NMS.ActiveMQ-1.6.0-bin.zip.asc)|1.6.0.3072

SVN Tag Checkout
----------------

svn co https://svn.apache.org/repos/asf/activemq/activemq-dotnet/Apache.NMS.ActiveMQ/tags/1.6.0/

Changelog
---------

For a more detailed view of new features and bug fixes, see the [release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12311201&styleName=Html&version=12315987)
