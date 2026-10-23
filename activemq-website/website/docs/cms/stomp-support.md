---
title: "Stomp Support"
sidebar_position: 6
---
## ActiveMQ-CPP Stomp Support

[Stomp](https://stomp.github.io/) is a simple text-based protocol supported by the ActiveMQ Broker that allows communication from a variety of clients (e.g. C++, Java, .NET, Ruby, Python, etc). If you'd like to learn more about the stomp protocol, checkout [https://stomp.github.io/](https://stomp.github.io/).

The ActiveMQ-CPP implementation of the CMS API with stomp has some quirks, as it's a simple protocol and doesn't have the full capabilities of, say, [OpenWire](/components/cms/openwire-support). The purpose of this page is to document these quirks so that users understand any strange behaviors that they may see occasionally.

### Message Properties in Stomp CMS

Since Stomp is strictly text-based, it does not support a way to specify the type of message properties (called "header" in stomp lingo). This means that a property sent as an integer could be read by a Stomp CMS client as any of: string, integer, short, long, etc.

When a Java client, for example, sends a message to the broker with an integer property ("myval"=1), the broker adapts the message from openwire to stomp and in the process converts the property "myval" to the string "1" and sends the message to the client. The client receives the string, but allows the user to read this value in any way that will work successfully with the std::istringstream >> operator.

The same goes for writing values to an outgoing message. You can call any of the methods (e.g. setIntProperty). The resulting value that goes out on the wire is still a string, however.

###  Temporary Topics and Queues

The Stomp Protocol does not support the concept of temporary topics or queues.  If you call the createTemporaryTopic or createTemporaryQueue methods of cms::Session an exception of type NotSupportedException is thrown.  To implement request / response type semantics you will need to use standard Topics and Queues.

### Usage notes on Selectors with Stomp

Stomp as a general rule only allows one session per connection.  In ActiveMQ-CPP we have created a sort of virtual session that allows more than one session to be created per connection.  The one caveat is that there still can only be one selector on the main Stomp Session that we create, so whatever the first session is that is created with a selector will be the only selector that will actually have any effect as none of the newly created sessions will apply a selector even if you pass one in the creation of that Session. 

### Stomp and Failover

Currently the Stomp protocol is at V1.0 and doesn't supply the necessary features in the protocol to allow the use of Failover with a Stomp based Transport. The draft version of Stomp v1.1 is under way and includes provisions for keep alive monitoring between broker and client which will allow for detection of dropped connections. Even with that support though there will be limitations on what can be accomplished on Failover of a Stomp client, transactions will not be able to be restored and message recovery will not be possible. A Stomp Failover would need to behave almost as a new Stomp connection to the Broker with automatic re-subscription for active consumers, all currently prefetched messages would be purged from the active consumers. Until the Stomp v1.1 spec is ratified and the ActiveMQ-CPP client code is updated to support this it is not recommended that you combine the Stomp transport and Failover transport.
