---
title: "Destinations Plugin"
slug: /destinations-plugin
---


In some environments clients can't create destinations and only administrators are allowed to do that using management consoles or APIs. This plugin allows users to export destinations created during the runtime of the broker and replicate that state on another broker.
```
<plugins>
    <destinationsPlugin location="/workspace/destinations"/>
</plugins>
```
The plugin saves all destinations to the file defined by the location property when it stops. If the property is not specified, broker data directory will be used and the file will be named `destinations` (`${ACTIVEMQ_HOME}/data/localhost/destinations` by default). If this file is present on the broker start, destinations listed in the file will be pre-created. The future improvements can include periodic export or on demand via management API.

