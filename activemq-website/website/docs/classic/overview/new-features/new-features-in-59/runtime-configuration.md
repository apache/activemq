---
title: "Runtime Configuration"
slug: /runtime-configuration
---


From version 5.9.0 a new broker plugin will allow selective changes to a broker xml configuration to take effect without broker restart.  
Consider a retail environment where there are central processors (hubs) and stores(spokes). It is advantageous to be able to add a new store  
without any down time.  

The addition of a new store could entail a new network connector and some new authorization policies, both of which can now be modified on the fly.

The plugin is configured via the broker xml configuration as follows:
```
<broker xmlns="http://activemq.apache.org/schema/core" start="false" ... >
    <plugins>
      <runtimeConfigurationPlugin checkPeriod="1000" />
    </plugins>
    ...
</broker>
```
Note: the `start="false"` attribute. It ensures that xbean-spring does not auto start the broker when the application context is loading.  
This ensures that the plugins reference to the broker is fully initialized before it is used and the xml configuration location can be successfully resolved.

The checkPeriod when > 0 is the time in milliseconds between checks for changes to the lastupdate timestamp of the broker xml configuration url.  
If checkPeriod is 0 (default) there is no automatic check for changes but a manual check can be triggered via JMX.

Configuration elements that support runtime modification

*   `<networkConnectors>`
*   `<destinationPolicy><policyMap><policyEntries>`
*   `<plugins><authorizationPlugin><map><authorizationMap><authorizationEntries>`
*   `<destinationInterceptors><virtualDestinationInterceptor><virtualDestinations>`

Some more implementation detail and links to the tests can be found in [AMQ-4682](https://issues.apache.org/jira/browse/AMQ-4682)

