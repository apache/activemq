---
title: "Delete Inactive Destinations"
slug: /delete-inactive-destinations
---


Inactive Destination

An 'inactive' destination is one that has had no messages pending and no consumers connected for some configured period of time.

By default the broker does _not_ check for inactive destinations. This as governed by the default values for the following options: `schedulePeriodForDestinationPurge="0"` and `gcInactiveDestinations="false"`.

However, the broker can be configured to purge inactive destinations. To do so requires using specially configured destination policy entries in combination with the broker attribute: `schedulePeriodForDestinationPurge > 0`.

Example:
```
<broker xmlns="http://activemq.apache.org/schema/core" schedulePeriodForDestinationPurge="10000">

  <destinationPolicy>
     <policyMap>
        <policyEntries>
           <policyEntry queue=">" gcInactiveDestinations="true" inactiveTimoutBeforeGC="30000"/>
        </policyEntries>
     </policyMap>
  </destinationPolicy>
      
</broker>
```
In this example the broker will check for inactive destinations every `10` seconds, as determined by `schedulePeriodForDestinationPurge="10000"`. When a destination purge is triggered the broker will delete any queue that has been empty for longer than `inactiveTimoutBeforeGC="30000"` milliseconds (default: `60000` milliseconds) and for which `gcInactiveDestinations="true"` is set on its corresponding destination policy entry.

When a destination is removed the broker will log a message like:
```
INFO  Queue                          - TEST.QUEUE Inactive for longer than 30000 ms - removing ...
```
