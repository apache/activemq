---
title: "JMSXUserID"
slug: /jmsxuserid
---


JMSXUserID support
------------------

It is sometimes useful to know the authenticated username of the sender of a message. This is not added by default but you can enable it by setting the **populateJMSXUserID** property on the broker via Java code
```
BrokerService broker = new BrokerService();
broker.setPopulateJMSXUserID(true);
broker.start();
```
Or via the [Xml Configuration](xml-configuration)
```
<broker xmlns="http://activemq.org/config/1.0" populateJMSXUserID="true">
   ...
</broker>
```
Or via the [Broker Configuration URI](broker-configuration-uri).

Once enabled this feature adds the JMS property **JMSXUserID** to each JMS message so that a consumer can know exactly who the sender was using the broker's authentication policy. i.e. it is not possibile for a producer to spoof this value if this feature is enabled since the broker attaches the property to the message after the senders connection is authenticated.

If you allow anonymous access, you MUST also add the
```
useAuthenticatedPrincipalForJMSXUserID="true"
```
property of the broker element. Otherwise, anonymous clients can spoof identities by setting the JMSXUserID property on from the client. This property is available in version 5.5 or 5.5-SNAPSHOT > March 12th. Note, though, that for SSL certificate based authentication, e.g., when using `TextFileCertificateLoginModule` JAAS module, this will change the semantics of the broker-provided JMSXUserID. Instead of returning the DN of the certificate, it will provide the name the DN is mapped to by the JAAS module.

