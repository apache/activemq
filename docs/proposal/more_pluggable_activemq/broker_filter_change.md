## More pluggable ActiveMQ Part I : add more destination control on ActiveMQ Classic `BrokerFilter`

### Problem statement

ActiveMQ Classic natively supports plugin architecture through `BrokerFilter` and dependency injection at runtime defined in `activemq.xml`. There are already a few plugins shipped with ActiveMQ Classic main branch (such as `SimpleAuthenticationPlugin`) that users can configure on `activemq.xml` out of the box. Furthermore, it can support external plugins hosted outside of its main branch (see this [blog post](https://medium.com/p/9cafcb39fbe4) and the [public doc](https://activemq.apache.org/components/classic/documentation/developing-plugins) for more details). That said, there are ways to make the broker engine more pluggable. This design doc is the first part of a bigger discussion, to make ActiveMQ Classic more pluggable and grow its plugin ecosystem. This design doc proposes that the `BrokerFilter` should expose more destination level operations to be intercepted.
### Rationale

By exposing more destination-level events on `BrokerFilter`,a plugin will have more control and allows a plugin to support more features related to destination operations.

### Proposal

In the current state, here are the methods exposed in the `BrokerFilter` that intercept destination-level operations:

1\. `addDestinationInfo`  
2\. `removeDestinationInfo`  
3\. `getDestinations`  
4\. `getDestinationMap`  
5\. `getDurableDestinations`  
6\. `addDestination`  
7\. `removeDestination`  
8\. `virtualDestinationAdded`  
9\. `virtualDestinationRemoved`

We propose to add the following method to the `BrokerFilter`:

1\. `purgeDestination()` method

- This method is triggered when a destination is about to be purged.

2\. `markDestinationForGC(long timeStamp)` method

- This method is triggered when a destination is marked for GC

3\. `browseDestination()` method

- This method is triggered when a destination is browsed

