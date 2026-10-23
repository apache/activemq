---
title: "Configurable IOException Handling"
description: "Tune how ActiveMQ Classic reacts to disk and JDBC failures with DefaultIOExceptionHandler properties, or plug in your own IOExceptionHandler implementation."
slug: /configurable-ioexception-handling
---

Starting with 5.3.1, ActiveMQ provides configurable IOException handling for its file-based message stores. From version 5.5 the handler is also invoked when the JDBC persistence adapter gets a failure on `getConnection()`.

Default IOException handler
---------------------------

ActiveMQ comes with a default IOException handler, which does the following. When some of the file-based message stores encounter IOException it can be one of the two things: either the disk is unavailable of there's no more space on the disk.

The first case is usually encountered when disk fails or network disk is disconnected. These errors are not "recoverable" and we usually want to shutdown the broker until problems with the disk are solved.

When there's no more space on the disk, we usually want to wait that some space is reclaimed and continue what we have been doing before (exchanging messages). All file-based persistent stores are capable of surviving these kind of errors.

### Configuring Default IOException handler

There are a couple of properties you can use to tune the behavior of the `DefaultIOExceptionHandler`. First, instantiate the exception handler as a bean. Then configure the broker to use the exception handler by setting the broker's `ioExceptionHandler` property.

Example:
```
<bean id="ioExceptionHandler" class="org.apache.activemq.util.DefaultIOExceptionHandler">
  <property name="ignoreAllErrors"><value>true</value></property>
</bean>

<broker xmlns="http://activemq.apache.org/schema/core" ioExceptionHandler="#ioExceptionHandler">
  ...
</broker>
```
Handler configuration properties:

Property|Since|Default Value|Description
---|---|---|---
`ignoreAllErrors`|5.4|`false`|When `true` all errors are ignored and the broker remains running.
`ignoreNoSpaceErrors`|5.4|`true`|When `false` 'no disk space' errors are treated the same as other errors causing the broker to be stopped.
`noSpaceMessage`|5.4|`space`|The string used to match against the exception's message. When matched a 'no disk space' error results.
`ignoreSQLExceptions`|5.5|`true`|If `true` all SQLExceptions are ignored by the handler allowing them to be handled by the persistence adapter's locker. When `false` the exception handler processes the exception.
`sqlExceptionMessage`|5.5|`""`|The SQLException phrase to match when ignoring SQLExceptions. Only matched exceptions are ignored. All SQLExceptions match the default empty string.
`stopStartConnectors`|5.5|`false`|When `true` transport connectors are stopped (client connections are refused), however, the broker will remain running. The transport connectors will be restarted following a successful persistence adapter checkpoint. All exceptions are ignored whilst the transport connectors are stopped. This option ensures that the broker does not need to be manually restarted in the event of a DB restart, for example.
`resumeCheckSleepPeriod`|5.5|`5sec`|The interval between persistence adapter checkpoints. Typically used in conjunction with `stopStartConnectors`.
`systemExitOnShutdown`|5.13|`false`|before stopping the broker, set the broker attribute systemExitOnShutdown to this value to potentially force a jvm exit.

The default configuration will try to find a specified string in the exception message to determine whether it is a 'no disk space' error. On most platforms (at least those we have tested), you'll find the word 'space' in it. Of course, you can customize this to your platform by using `noSpaceMessage` property.

Note: as of ActiveMQ 5.11 the `JDBCIOExceptionHandler` has been deprecated. It has been replaced by the `org.apache.activemq.util.LeaseLockerIOExceptionHandler` that will work with any persistence adapter that supports pluggable storage lockers whether or not a locker is in use.

Writing your own handler

In case this handler doesn't work for you, you can write your own. For example you might want to change the way how you detect full disk and execute some external command, like `df` on Linux to be sure.

All you have to do is implement the `org.apache.activemq.util.IOExceptionHandler` interface then configure the broker to use it:
```
<bean id="ioExceptionHandler" class="com.mycompany.MyIOExceptionHandler">
  <property name="ignoreAllErrors"><value>true</value></property>
</bean>

<broker xmlns="http://activemq.apache.org/schema/core" ioExceptionHandler="#ioExceptionHandler">
  ...
</broker>
```
