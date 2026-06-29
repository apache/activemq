# Apache ActiveMQ Security Policy

## Supported Versions

| Version | Supported          | Note |
| ------- | ------------------ | ---- |
| 6.2.x   | :white_check_mark: |      |
| 6.1.x   | :x: | |
| 6.0.x   | :x: | | 
| 5.19.x   | :white_check_mark: | Except for EOL dependencies: Spring and Jetty |
| <= 5.18.x | :x:                | |

## Reporting a Vulnerability

For information on how to report a new security problem please see [here](https://www.apache.org/security/).
Our existing security advisories are published [here](https://activemq.apache.org/security-advisories).

## ActiveMQ's Security Severity Rating system

Apache ActiveMQ has adopted Apache's Security Severity system. This rating system 
was designed to provider users a simple range of severity to plan their updates 
accordingly.

The CVSS scoring system is complex and there are gaps in the definitions that 
can lead to prolonged negotiation over security scoring. 

The Apache Security Severity system provides a fair balance to users and researchers, 
while easing the effort required by the Apache ActiveMQ volunteers.

## ActiveMQ Security Recommendations

Apache ActiveMQ's flexibility and wide ranging set of capabilities and features lends
itself to being exposed to security vulnerabilities, especially ones from third-party
projects such as Spring and Jolokia.

Apache ActiveMQ project recommends applying defense-in-depth and security-first to provide layers of security to environments running production workloads.

Layers of security provide valuable options to prevent attacks, and to provide a buffer for when vulnerabilities at any layer are reported to provide reasonable time to test and apply fixes without impacting business-critical messaging traffic.

Users are expected to secure their environments

1. The Web Console and Jolokia REST API are not designed to be exposed to the public Internet. Only admins should be granted access.

2. Require user authentication and authorization for all connectivity including JMX, Jolokia, REST API and the web console.

3. Require SSL connections on all transport connectors.

4. Disable transport connectors for protocols that are not used by application clients.

5. Configure an appropriate [maxFrameSize](https://activemq.apache.org/components/classic/documentation/configuring-wire-formats) on transports for your use case to prevent OOM DOS. The default XML configuration defaults to 10 MB but this can be adjusted as needed. 

6. Two-way SSL is the recommended security mechanism for identity and authentication of application clients.

7. Stay current with Java JDK updates

8. Use highest possible security SSL protocol and algorithms.

9. Limit inbound and outbound network connectivity to and from an ActiveMQ server.

10. Do not run the broker using the root user, instead create a user account to use for the broker. Users are expected to secure their OS and their file system with proper permissions and controls. The broker does not try and limit access to files, it relies on the operating system to do so.

11. Normal users need permission to create advisory topics but should generally **not** be given permission to read/write to those topics as those messages are meant for admins. A notable exception is for temporary destination advisory topics. For more information see the authorization section [here](https://activemq.apache.org/components/classic/documentation/security#authorization).

## ActiveMQ Security Improvement Project

The Apache ActiveMQ team has initiated a security hardening project to move from a default configuration that is geared for developer testing and learning to a secured-by-default stance.

1. Enhancements to the SSL authentication plugin to fix wantAuth mode

2. Updates to SSL handling to allow configuring per-transport and per-network connector SSL keys

3. Refactoring of Jetty service to use Jetty-provided configurations instead of Spring-style configuration for Jetty service used by API and web console.

4. [Done] Limiting XBean URI schemes

5. Long-term: Replace Spring as the primary means of configuring and booting Apache ActiveMQ servers.

6. Add allow/deny lists to transport connectors to limit IP addresses

7. [Done] VM Transport creation blocks the XBean factory by default

8. [Done] Limit the maximum size of uncompressed message bodies with the `maxInflatedDataSize` and `maxInflatedDataSizeRatio` settings.

9. [Done] Validate all size values during unmarshalling before using those sizes for allocating buffers

10. [Done] The WebConsole and Jolokia have been restricted to only admins.

## Security vs Features

AI code scanning tools often mistaken designed features as a security issue. It is the responsibility of the reporter to review AI output and verify if it's a real issue. There has been a large number of invalid submissions that could be avoided by simply reviewing the JMS spec and the features of the broker itself.

Some of the most common reported examples:

1. JMS Selectors - An optional query parameter designed to filter messages on a queue or topic that is not security related. It is used by clients to consume a subset of messages on the destination instead of all messages. However, if a client is authorized for a destination it is always free to consume all the messages if it chooses so by simply not setting the selector. Therefore any reports showing issues with selectors allowing the consumption of extra messages would be considered a bug and not a security issue as long as it doesn't escape the destination the client is authorized for.

2. ClientId - A non-secret unique identifier used to provide once-and-only-once delivery that are designed to be used between connections and be deleted. The JMS spec specifically allows any authorized connection to use the same clientid as long as it isn't currently in use. Some protocols, such as MQTT, also allow link stealing and taking over if in use.

3. Durable Subscriptions - The JMS spec allows authorized connections to connect to any existing durable subscription (combination of client id and subscription name) as long as it is offline. Authorized clients are allowed to delete the durable subscriptions as well even if they didn't create it.

4. BlobMessages - Blob message support is a side-channel for moving large messages with the JMS API by routing the large message through a different endpoint such as http, sftp or scp. Clients using BlobMessages are responsible for validating the authenticity and validity of the uri provided by the received message before taking any action such as downloading or deleting the file. ActiveMQ recommends using SSL secured transports, with two-way SSL as the most preferred.

## Non-Security issues

1. Exploits that are only possible because users did not configure authentication or authorization. It is expected users modify the default configuration appropriately to enable security for their environment. 

2. Any attack that require administrative access to be granted. For example, by default Jolokia and the web console now requires administrative access. By definition admins are allowed to do anything, so if the issue requires the user to login with admin credentials then the report will not be accepted and would be treated as a bug.

3. DoS attacks caused by OOM because users did not configure a maxFrameSize or maxInflatedDataSize which are designed to limit the size of messages in memory.
