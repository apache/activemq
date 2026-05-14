# Apache ActiveMQ Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 6.2.x   | :white_check_mark: |
| 6.1.x   | :x: |
| 6.0.x   | :x: |
| 5.19.x   | :white_check_mark: |
| <= 5.18.x | :x:                |

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

Users are advised to secure their environments

1. The web console is not designed to be exposed to the public Internet.

2. Require user authentication and authorization for all connectivity including JMX, Jolokia, REST API and the web console.

3. Require SSL connections on all transport connectors. 

4. Disable transport connectors for protocols that are not used by application clients.

5. Two-way SSL is the recommended security mechanism for identity and authentication of application clients.

6. Stay current with Java JDK updates

7. Use highest possible security SSL protocol and algorithms.

8. Limit inbound and outbound network connectivity to and from an ActiveMQ server.

## Upcoming ActiveMQ Security Improvements

Apache ActiveMQ projects recommends applying defense-in-depth and security-first to provide layers of security to environments running production workloads.

Layers of security provide valuable options to prevent attacks, and to provide a buffer for when vulnerabilities at any layer are reported to provide reasonable time to test and apply fixes without impacting business-critical messaging traffic.

1. Enhancements to the SSL authentication plugin to fix wantAuth mode

2. Updates to SSL handling to allow configuring per-transport and per-network connector SSL keys

3. Refactoring of Jetty service to use Jetty-provided configurations instead of Spring-style configuration for Jetty service used by API and web console.

4. [Done] Limiting XBean URI schemes

5. Long-term: Replace Spring as the primary means of configuring and booting Apache ActiveMQ servers.

6. Add allow/deny lists to transport connectors to limit IP addresses

7. [Done] VM Transport creation blocks the XBean factory by default
