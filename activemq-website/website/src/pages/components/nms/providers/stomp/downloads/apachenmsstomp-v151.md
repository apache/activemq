---
title: Apache.NMS.Stomp v1.5.1
---

# Apache.NMS.Stomp v1.5.1

##### New and Noteworthy

This release adds some great new features to the NMS client libraries and fixes several bugs found in the 1.4.0 release.  Some highlights for this release.

*   Fixed a memory leak in the Inactivity Monitoring code.
*   Fixed a bug that could cause the Failover to not work.
*   Fixed a bug that caused SSL Certs to not be loaded in some cases.
*   Fixed a bug that was causing major performance issues for synchronous consumers.
*   Fixed issues found with the Stomp v1.1 support.

##### API Documentation

Refer to the API for this release [here](/components/nms/nms-api)

##### Apache.NMS.Stomp Client Downloads

|Description|Download Link|PGP Signature File|Version|
|---|---|---|---|
|Apache.NMS.Stomp Source code|[Apache.NMS.Stomp-1.5.1-src.zip](https://archive.apache.org/dist/activemq/apache-nms/1.5.0/Apache.NMS.Stomp-1.5.1-src.zip)|[Apache.NMS.Stomp-1.5.1-src.zip.asc](https://archive.apache.org/dist/activemq/apache-nms/1.5.0/Apache.NMS.Stomp-1.5.1-src.zip.asc)|1.5.1.2216|
|Apache.NMS.Stomp Binary Assemblies|[Apache.NMS.Stomp-1.5.1-bin.zip](https://archive.apache.org/dist/activemq/apache-nms/1.5.0/Apache.NMS.Stomp-1.5.1-bin.zip)|[Apache.NMS.Stomp-1.5.1-bin.zip.asc](https://archive.apache.org/dist/activemq/apache-nms/1.5.0/Apache.NMS.Stomp-1.5.1-bin.zip.asc)|1.5.1.2216|

**NOTE:** Compatible with ActiveMQ Broker versions in the 4.X and 5.X family. Support TIBCO EMS 4.1.0 and above.

##### SVN Tag Checkout
```
svn co https://svn.apache.org/repos/asf/activemq/activemq-dotnet/Apache.NMS.Stomp/tags/1.5.1/
```
##### Changelog

For a more detailed view of new features and bug fixes, see the [release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12311201&styleName=Html&version=12315640)
