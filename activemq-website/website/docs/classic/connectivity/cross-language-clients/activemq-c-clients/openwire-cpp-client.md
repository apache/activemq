---
title: "OpenWire CPP Client"
slug: /openwire-cpp-client
---


OpenWire C++ Client
===================

Goals
-----

We want to be able to provide a C++ API to ActiveMQ that retains rough feature-parity with the Java API while at the same time allowing for more flexibility in application design by opening up lower levels of the event system. We have come up with an API / programming model that maximizes the ways in which the messaging library can adapt to the application using it, not the other way around. Specifically, our goals are:

*   Make no assumption about the threading constraints of the containing application (specifically, allow asynchronous receives within a single-threaded application)
*   Make no assumption about the event loop characteristics of the containing application
*   Make no assumption about libraries needed – i.e. dont enforce use of a particular library or smart pointer implementation
*   Offer tradeoffs in flexibility versus application complexity
*   Provide distinctly C++-friendly APIs for asynchronous event notification
*   Provide pluggable data transports
*   Maximize portability

It makes no assumptions about the threading or event loop model of the containing application, and so puts no restrictions on C++ program design as inherent costs of doing asynchronous messaging. As a result of this flexible and pluggable design, this library is particularly easy to implement as a native layer to a high-level scripting language interface to ActiveMQ since it makes no assumptions about the threading capabilities of the language.

When we use the term programming model, we are referring to the handling of the most complex part of asynchronous messaging APIs – message delivery to the application.

JMS / Java programming model
----------------------------

JMS 1.1 provides two ways for an application to receive messages. One is by a blocking receive in the MsgConsumer class' receive() function. This will suspend the calling thread until a message is received. The other method is through callbacks in subclasses of MessageListener. The OnMessage function is called when a message is received on the topic or queue that the MessageListener was created for.

To sum up – The JMS model imposes a threading structure on the application developer. Either they must use multiple threads each doing synchronous receive() calls, or rely on a separate thread to deliver messages to them. A single-threaded model is impossible.

Where whats best for C++ and Java APIs diverge
----------------------------------------------

The JMS API enforces a particular model of threading on an application. This is fine and well-accepted in the Java programming space, since Java is designed for it to be easiest to multiplex communications with threads. Threads are very easy and well-supported in Java.

However, in C++ the common practice is different. Most C++ programs that handle network communication in an event-driven fashion use an event loop. This is a thread that waits for activity on a number of file descriptors, relying on the OS to wake it up when one has data. File descriptors are the lingua franca of event-driven C and C++ programs, since the OS makes it easy to use them for any kind of event, as well as providing rich functionality to wait on multiple file descriptors at once. In C and C+, threads are error-prone (due to no concurrency features in the language) and often unnecessary when a flexible enough event loop is used. The upshot of this is that the most optimal C\+ API for an asynchronous messaging system is one that works within this common practice of C++ programs, rather than imposing arbitrary restrictions. In particular, even background threads that share no data or code with the application can affect it since the interaction of multiple threads and UNIX signals is not well defined.

In conclusion, C++ developers expect a level of openness and flexibility when using libraries that can deliver events, so that they can integrate multiple sources of events together without resorting to multithreading. Most libraries that do this properly expose the underlying file descriptor of the network connection directly to the application – X11 does this for example – and we will start there with the actual design specification.

C++s lower level nature presents many more inherent design decisions than does Java. In particular, one of the largest issues is that there is no standard smart pointer implementation. Boost is popular, but every enterprise has its own, and every developer has personal preference. It is for this reason that this library does not propose to use any particular smart pointer implementation, as it enforces a design decision on enterprises that have their own requirements in that space. Through careful use of references and API semantics we can make memory ownership explicit enough to avoid the pitfalls. As it is, the API policy is that all returned pointers are the application's responsibility to free. This should make it sufficiently unambiguous.

We are proposing a three-tiered approach to provide the tradeoff between application simplicity and flexibility. The lowest level will do the least and allow the widest range of applications to use it, the next level will be much more functional, but with some design tradeoffs, and the highest we will leave up to enterprise-specific needs.

Proposed C++ API Design
-----------------------

The design that we are proposing consists of two layers. The core library owns no threads and acts simply as a filter – taking data from the broker in and dispatching messages if necessary. This core layer of the library is exposed to allow those C++ developers who do not want any threads other than their own managing events, and/or want to use non-TCP methods of communication with the broker. The socket to the ActiveMQ broker is to be exposed to the application by the transport layer, and data received on it is to be passed opaquely to the core library by the application. This allows for asynchronous messaging with a single-threaded program – a hard requirement for some developers.  
There are shortcomings to this approach. If the application blocks on some other activity and data from the broker is never processed, the TCP socket will back up and messages will build up on the broker side. Also, the single-threaded nature of the library means that the application must do all the I/O work itself, and implement a separate thread in order to make meaningful use of the blocking receive.

We can build a second layer (we call it BrokerSession) on top of the first that provides these features – still integrating into the applications existing event structure, but at the price of a background thread. The background thread handles data from ActiveMQ and passes it to the core library, putting any resulting messages into an internal message buffer (or potentially multiple – see details below). With this the library can provide a much simpler and more direct messaging interface to the application, since it handles broker communication itself in the same way that an application using the core library would.

The second layer simply invokes an interior instance of the first to perform any ActiveMQ-related messaging tasks – no messaging-system-specific code is outside of the core lower level library. As a result, the second layer is a thin one – just the thread, interaction with the transport abstraction layer and functionality passthroughs to the core library.

There is a third level – there is a need for higher-level, application framework libraries for messaging, that might take all control flow from the user and provide the simplest possible environment. The lower levels are designed to allow the most flexibility in the design and requirements of the third level, which is most often enterprise-specific and lies outside the scope of this document. An example of a library at this level would be one that provides a framework that makes it easy to write a completely callback-based application. Since this would require that an application be designed for it from the start, it is at this level that design decisions such as selecting a smart pointer implementation would be appropriate. It is likely that for a particular enterprise, most applications would use something like this, which would be implemented as a thin wrapper over the BrokerSession library.

Transport layer abstraction overview
------------------------------------

Similar to ActiveMQ itself, we provide a transport layer abstraction that manages connection and I/O on file descriptors. Initially only TCP socket support will be provided, but there is value in other implementations – in-process pipe, playing back from a file, UDP, etc. This layer provides logical connect, send, recv, and close operations. Transports are initialized with URIs in the same fashion that other ActiveMQ transports are.

Core library overview
---------------------

The core layer of the library provides a very simple interface – data in, messages out.

![Core Library Usage](</img/Core Library Usage.JPG>)

When the returned file descriptor from the transport layer's connect() call reads active in the application's event loop, the application reads available data and passes that to the library. The library itself has an internal buffer that will deliver a message when one has been fully accumulated. The purpose of this disconnect is that the core library **will not make any blocking I/O calls – or any system calls at all for that matter**. This satisfies the portability goal, as well as ensuring that a single-threaded application can use this library without the potential for losing control of its thread.

The end-to-end lifetime of an outgoing message using the core library is:

*   User calls "send" or "subscribe" etc.
*   The core library constructs the corresponding OpenWire command object
*   The core library marshals this object into a buffer and returns that buffer to the user

The end-to-end lifetime of an incoming message using the core library is:

*   The user gets data from ActiveMQ, possibly by using the included transport library
*   This data is passed to the core library, which does one of two things:

*   if it isn't already receiving a message, it looks at the first few bytes and notes how large the incoming message is by examining OpenWire headers.
*   if it is, it adds the incoming buffer to an internal message buffer, and if this addition completes the incoming message it deserializes it into an OpenWire command object, and the appropriate action is taken.

Notable here is the behavior of message receives - the core library is able to receive messages in piecemeal, and only deliver a message when it has been completely received.

Publication and subscription work in the conventional ways, except that they return data to be sent by the transport layer instead of directly sending it. This allows a higher level to determine exactly how the data is sent – for example, it allows use of non-blocking I/O.

The application is given two choices for message receipt. The library does this by means of one or more internal "message consumer" objects (similar to JMS MessageConsumer objects). There are two kinds – one for synchronous receives, and one for asynchronous. The synchronous consumer(BlockingMessageConsumer) offers a blocking receive that multiple threads can wait on for messages. The asynchronous message consumer uses an event-based interface, notifying the application that a message is ready via a byte written down a pipe. The NonBlockingMessageConsumer class can dispense its event file descriptor as well as received messages – the asynchronous receive() call is nonblocking and returns NULL if no message is ready.

BrokerSession library overview
------------------------------

The higher level library's interface offers a much simpler and more direct API to the developer than the first layer, as a result of having an internal thread to process data from the ActiveMQ broker.

![BrokerSession Library Usage](</img/BrokerSession Library Usage.JPG>)

Since the BrokerSession library contains a copy of the core library, ActiveMQ-specific code is kept encapsulated there. The code in the BrokerSession library does the legwork of managing the core library and broker communication for the application. The use of a background thread to do this and deliver incoming messages to the message consumers greatly simplifies application development.

As in the core library, message dispatch is done explicitly by the application, not from the background thread. Neither library stores message callbacks – it is not required that they be used. This ensures that only threads that are known by the calling application execute application code – the result being that thread-safe programming is not enforced or required (satisfying the goal of not mandating a particular thread model). It is not unexpected that there will commonly be enterprise-specific code layered in front of this to do more customized message delivery (potentially using callbacks, for example).

This library allows both a JMS-like programming model of multiple threads waiting on message queues as well as an event-driven, select-loop model that is more common in C++ programs. This makes it adaptable to the varying requirements of new and existing C++ applications that have design constraints imposed by things other than the messaging library.

Logging and error handling
--------------------------

To fit with the goals of not enforcing particular dependencies and allowing easy enterprise integration, the libraries have a callback-based model for logging. The application registers a logger with the library to be called when log events occur - this logger has function callbacks for error logging, debug logging, etc. This allows the easiest integration with existing logging systems, as these callbacks can then call directly into the native logging environment (such as syslog) or log to a file, or anything else that is appropriate in the particular application.

Error handling is slightly different. The native C++ way of doing error handling is through exceptions. Exceptions work very well for something like the core library, so when an error occurs it will throw an instance of ActiveMQ::Exception to the caller. In the BrokerSession library, however, such exceptions may be caught by the background thread. In this case, that thread will call an application callback to pass along the exception. This ensures that all errors reach the application and can be dealt with appropriately.

The default callback for exception handling simply print the relevant message to standard error.

