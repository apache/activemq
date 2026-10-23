---
title: "Active Groups"
slug: /active-groups
---


Active Groups is a dynamic collaboration framework so simplify message passing and shared state between members of the group. It is available in ActiveMQ 6.0

Active Groups includes the following:

*   Dynamic membership information
*   broadcast messaging
*   point-to-point
*   in boxes
*   Distributed state (Map)
*   Map change listeners
*   write locks
*   lock expiration
*   optional state and lock removal when a member leaves
*   automatic state and lock replication and failover
*   configurable heartbeats

Active Groups is peer based collaboration only - though the underlying transport is JMS (which can be peer based too). Although any JMS provider can be used, ActiveGroups can use the membership information available through ActiveMQ to supplement its heart beat infrastructure.

