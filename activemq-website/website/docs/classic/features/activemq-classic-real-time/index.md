---
title: "ActiveMQ Real Time"
slug: /activemq-classic-real-time
---


ActiveMQ Real Time is a sub-project of the popular Apache ActiveMQ messaging system but is designed specifically to meet the needs for low latency high throughput collaborative real-time applications.

It is designed to be used by multiple languages on multiple platforms - and has three main goals:

1.  Low latency pluggable reliable multicast - with different qualities of service. The complexities of group communication means that different reliable multicast protocols must be used to meet the needs of real-time application collaboration. ActiveBlaze supports pluggable protocols, which will include:
    *   NACK-based reliable protocols
    *   Totem protocol
    *   Forward Error correction
2.  Active Group membership management to allow true point-to-point communication and group messaging
3.  Cluster management for shared state, fault tolerance, reliability and availability.

The need for low latency means that the JMS (Java Message Service) API is not always a suitable fit and ActiveBlaze comes with its own bespoke API's to accomplish very high throughput. It is a goal of the project to provide an optional JMS API layer on top of core ActiveBlaze API's to enable it to be a drop in replacement for more traditional hub and spoke messaging deployments.

To enable cross-language support ,enable versioning of the wire format, and aid fast throughput - Apache ActiveBlaze is built on top of the Google's protobuf - a framework for efficiently encoding extensible data structures.

low latency reliable multicast
------------------------------

Group membership
----------------

Support for dynamic joining and leaving of groups, membership notifications and true point to point and queue messaging:

*   Dynamic membership information
*   broadcast messaging
*   point-to-point
*   in boxes
*   configurable heartbeats

Cluster management
------------------

A distributed state map, with the following:

*   Distributed state (Map)
*   Map change listeners
*   write locks
*   lock expiration
*   optional state and lock removal when a member leaves
*   automatic state and lock replication and failover

In more detail ...
------------------

*   [Broadcasting](broadcasting)
*   [Group Membership](group-membership)
*   [Life Cycle](life-cycle)
*   [The ActiveBlaze Message type](the-activeblaze-message-type)

