---
title: "java.lang.NoSuchMethodException org.activemq.ra.ActiveMQResourceAdapter.setUseEmbeddedBroker"
slug: /javalangnosuchmethodexception-orgactivemqraactivemqresourceadaptersetuseembeddedbroker
---

If you get an error like this
```
2005-08-14 17:18:45,618 ERROR [org.jboss.resource.deployment.RARDeployment] Starting failed jboss.jca:service=RARDeployment,name='activemq-ra-3.0.rar' org.jboss.deployment.DeploymentException: Error for resource adapter class org.activemq.ra.ActiveMQResourceAdapter accessing property setter ConfigPropertyMetaData@e62121[name=UseEmbeddedBroker type=java.lang.Boolean value=false descriptions=[DescriptionMetaData@1ad093c[language=en description=Boolean to configure if the connector should create an embedded broker.]]]; - nested throwable: (java.lang.NoSuchMethodException: org.activemq.ra.ActiveMQResourceAdapter.setUseEmbeddedBroker(java.lang.Boolean))
        at org.jboss.deployment.DeploymentException.rethrowAsDeploymentException(DeploymentException.java:39)
        at org.jboss.resource.deployment.ResourceAdapterFactory.createResourceAdapter(ResourceAdapterFactory.java:90)
        at org.jboss.resource.deployment.RARDeployment.startService(RARDeployment.java:101)
        at org.jboss.system.ServiceMBeanSupport.jbossInternalStart(ServiceMBeanSupport.java:272)
        at org.jboss.system.ServiceMBeanSupport.jbossInternalLifecycle(ServiceMBeanSupport.java:222)
        at org.jboss.system.ServiceDynamicMBeanSupport.invoke(ServiceDynamicMBeanSupport.java:110)
        at org.jboss.mx.server.RawDynamicInvoker.invoke(RawDynamicInvoker.java:150)
        at org.jboss.mx.server.MBeanServerImpl.invoke(MBeanServerImpl.java:644)
        at org.jboss.system.ServiceController$ServiceProxy.invoke(ServiceController.java:897)
```

### Fix

You are using 3.0 of ActiveMQ. This bug is fixed in 3.1.x so use 3.1-M6 or later.

