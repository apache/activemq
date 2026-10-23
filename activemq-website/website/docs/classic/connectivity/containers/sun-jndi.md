---
title: "Sun JNDI"
slug: /sun-jndi
---


here's an example jndi.properties file:

java.naming.factory.initial = com.sun.jndi.fscontext.RefFSContextFactory

Here's an example .bindind file:

For ActiveMQ 4.x its

Archive/ClassName=org.apache.activemq.command.ActiveMQQueue
Archive/FactoryName=org.apache.activemq.jndi.JNDIReferenceFactory
Archive/RefAddr/0/Type=physicalName
Archive/RefAddr/0/Content=Archive
Archive/RefAddr/0/Encoding=String

GateIn/ClassName=org.apache.activemq.command.ActiveMQQueue
GateIn/FactoryName=org.apache.activemq.jndi.JNDIReferenceFactory
GateIn/RefAddr/0/Type=physicalName
GateIn/RefAddr/0/Content=GateIn
GateIn/RefAddr/0/Encoding=String

ProviderIn/ClassName=org.apache.activemq.command.ActiveMQQueue
ProviderIn/FactoryName=org.apache.activemq.jndi.JNDIReferenceFactory
ProviderIn/RefAddr/0/Type=physicalName
ProviderIn/RefAddr/0/Content=ProviderIn
ProviderIn/RefAddr/0/Encoding=String

ProviderOut/ClassName=org.apache.activemq.command.ActiveMQQueue
ProviderOut/FactoryName=org.apache.activemq.jndi.JNDIReferenceFactory
ProviderOut/RefAddr/0/Type=physicalName
ProviderOut/RefAddr/0/Content=ProviderOut
ProviderOut/RefAddr/0/Encoding=String

QueueConnectionFactory/ClassName=org.apache.activemq.ActiveMQConnectionFactory
QueueConnectionFactory/FactoryName=org.apache.activemq.jndi.JNDIReferenceFactory
QueueConnectionFactory/RefAddr/0/Type=brokerURL
QueueConnectionFactory/RefAddr/0/Content=vm\\://localhost
QueueConnectionFactory/RefAddr/0/Encoding=String
QueueConnectionFactory/RefAddr/1/Type=brokerName
QueueConnectionFactory/RefAddr/1/Content=ID\\:zdv-1189-1098562819250-0\\:0
QueueConnectionFactory/RefAddr/1/Encoding=String
QueueConnectionFactory/RefAddr/2/Type=password
QueueConnectionFactory/RefAddr/2/Content=defaultPassword
QueueConnectionFactory/RefAddr/2/Encoding=String
QueueConnectionFactory/RefAddr/3/Type=userName
QueueConnectionFactory/RefAddr/3/Content=defaultUser
QueueConnectionFactory/RefAddr/3/Encoding=String

For ActiveMQ 3.x it is

Archive/ClassName=org.activemq.message.ActiveMQQueue
Archive/FactoryName=org.activemq.jndi.JNDIReferenceFactory
Archive/RefAddr/0/Type=physicalName
Archive/RefAddr/0/Content=Archive
Archive/RefAddr/0/Encoding=String

GateIn/ClassName=org.activemq.message.ActiveMQQueue
GateIn/FactoryName=org.activemq.jndi.JNDIReferenceFactory
GateIn/RefAddr/0/Type=physicalName
GateIn/RefAddr/0/Content=GateIn
GateIn/RefAddr/0/Encoding=String

ProviderIn/ClassName=org.activemq.message.ActiveMQQueue
ProviderIn/FactoryName=org.activemq.jndi.JNDIReferenceFactory
ProviderIn/RefAddr/0/Type=physicalName
ProviderIn/RefAddr/0/Content=ProviderIn
ProviderIn/RefAddr/0/Encoding=String

ProviderOut/ClassName=org.activemq.message.ActiveMQQueue
ProviderOut/FactoryName=org.activemq.jndi.JNDIReferenceFactory
ProviderOut/RefAddr/0/Type=physicalName
ProviderOut/RefAddr/0/Content=ProviderOut
ProviderOut/RefAddr/0/Encoding=String

QueueConnectionFactory/ClassName=org.activemq.ActiveMQConnectionFactory
QueueConnectionFactory/FactoryName=org.activemq.jndi.JNDIReferenceFactory
QueueConnectionFactory/RefAddr/0/Type=brokerURL
QueueConnectionFactory/RefAddr/0/Content=vm\\://localhost
QueueConnectionFactory/RefAddr/0/Encoding=String
QueueConnectionFactory/RefAddr/1/Type=brokerName
QueueConnectionFactory/RefAddr/1/Content=ID\\:zdv-1189-1098562819250-0\\:0
QueueConnectionFactory/RefAddr/1/Encoding=String
QueueConnectionFactory/RefAddr/2/Type=password
QueueConnectionFactory/RefAddr/2/Content=defaultPassword
QueueConnectionFactory/RefAddr/2/Encoding=String
QueueConnectionFactory/RefAddr/3/Type=useEmbeddedBroker
QueueConnectionFactory/RefAddr/3/Content=true
QueueConnectionFactory/RefAddr/3/Encoding=String
QueueConnectionFactory/RefAddr/4/Type=userName
QueueConnectionFactory/RefAddr/4/Content=defaultUser
QueueConnectionFactory/RefAddr/4/Encoding=String
QueueConnectionFactory/RefAddr/5/Type=useAsyncSend
QueueConnectionFactory/RefAddr/5/Content=true
QueueConnectionFactory/RefAddr/5/Encoding=String

