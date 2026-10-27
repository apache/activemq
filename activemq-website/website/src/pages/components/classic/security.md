---
title: Security Advisories - ActiveMQ
---

# Security Advisories - ActiveMQ

Details of security problems fixed in released versions of Apache ActiveMQ 6.x and 5.x are detailed below.

See the main [Security Advisories](/security-advisories) page for details for other components and general information such as reporting new security issues.

*   [CVE-2026-74761](/security-advisories.data/CVE-2026-74761-announcement.txt) - An authenticated client can spoof clientId when removing a durable topic subscription
*   [CVE-2026-61487](/security-advisories.data/CVE-2026-61487-announcement.txt) - Authorization bypass via temporary composite destinations
*   [CVE-2026-59878](/security-advisories.data/CVE-2026-59878-announcement.txt) - AMQP NIO negative frame size validation bypass leading to DoS
*   [CVE-2026-54475](/security-advisories.data/CVE-2026-54475-announcement.txt) - Temporary destination ownership takeover
*   [CVE-2026-53917](/security-advisories.data/CVE-2026-53917-announcement.txt) - Unbounded memory allocation in OpenWire property unmarshalling
*   [CVE-2026-53916](/security-advisories.data/CVE-2026-53916-announcement.txt) - Unbounded header buffer in STOMP NIO codec
*   [CVE-2026-52760](/security-advisories.data/CVE-2026-52760-announcement.txt) - Stored XSS via Unescaped values in ActiveMQ Web Console
*   [CVE-2026-50750](/security-advisories.data/CVE-2026-50750-announcement.txt) - Pre-authentication OpenWire DoS following fix for CVE-2026-49270
*   [CVE-2026-50734](/security-advisories.data/CVE-2026-50734-announcement.txt) - Pre-authentication OpenWire memory-allocation DoS during wire format negotiation
*   [CVE-2026-49877](/security-advisories.data/CVE-2026-49877-announcement.txt) - Authenticated web users retain admin access by default in the Web Console
*   [CVE-2026-49434](/security-advisories.data/CVE-2026-49434-announcement.txt) - LdapNetworkConnector instantiates denied transports and a remote-properties broker
*   [CVE-2026-49432](/security-advisories.data/CVE-2026-49432-announcement.txt) - STOMP negative content-length enables denial of service
*   [CVE-2026-49270](/security-advisories.data/CVE-2026-49270-announcement.txt) - Durable Subscription Disclosure via Crafted BrokerInfo (OpenWire)
*   [CVE-2026-49157](/security-advisories.data/CVE-2026-49157-announcement.txt) - Authenticated low-privilege Web users retain Jolokia broker-management capability by default
*   [CVE-2026-46605](/security-advisories.data/CVE-2026-46605-announcement.txt) - Incomplete authorization during destination removal
*   [CVE-2026-45505](/security-advisories.data/CVE-2026-45505-announcement.txt) - Jolokia "addNetworkConnector" Discovery Wrapper Bypass
*   [CVE-2026-42588](/security-advisories.data/CVE-2026-42588-announcement.txt) - Remote Code Execution via Jolokia addNetworkConnector
*   [CVE-2026-42253](/security-advisories.data/CVE-2026-42253-announcement.txt) - HTTP Response Header Injection via JMS Message Properties
*   [CVE-2026-41044](/security-advisories.data/CVE-2026-41044-announcement.txt) - Authenticated user can perform RCE via DestinationView MBean exposed by Jolokia
*   [CVE-2026-41043](/security-advisories.data/CVE-2026-41043-announcement.txt) - ActiveMQ Web Console - XSS vulnerability when browsing queues
*   [CVE-2026-40466](/security-advisories.data/CVE-2026-40466-announcement.txt) - Possible bypass of CVE-2026-34197 via HTTP discovery second-stage URI
*   [CVE-2026-40046](/security-advisories.data/CVE-2026-40046-announcement.txt) - Missing fix for CVE-2025-66168: MQTT control packet remaining length field is not properly validated
*   [CVE-2026-39304](/security-advisories.data/CVE-2026-39304-announcement.txt) - Incorrect handling of TLSv1.3 KeyUpdate can be exploited to cause DoS via OOM
*   [CVE-2026-34197](/security-advisories.data/CVE-2026-34197-announcement.txt) - Authenticated users could perform RCE via Jolokia MBeans
*   [CVE-2026-33227](/security-advisories.data/CVE-2026-33227-announcement.txt) - Improper Limitation of a Pathname to a Restricted Classpath Directory
*   [CVE-2025-66168](/security-advisories.data/CVE-2025-66168-announcement.txt) - MQTT control packet remaining length field is not properly validated
*   [CVE-2025-54539](/security-advisories.data/CVE-2025-54539-announcement.txt) - Deserialization of Untrusted Data
*   [CVE-2025-29953](/security-advisories.data/CVE-2025-29953-announcement.txt) - Deserialization allowlist bypass
*   [CVE-2025-27533](/security-advisories.data/CVE-2025-27533-announcement.txt) - Unchecked buffer length can cause excessive memory allocation
*   [CVE-2024-32114](/security-advisories.data/CVE-2024-32114-announcement.txt) - Jolokia and REST API were not secured with default configuration
*   [CVE-2023-46604](/security-advisories.data/CVE-2023-46604-announcement.txt) - Unbounded deserialization causes ActiveMQ to be vulnerable to a remote code execution (RCE) attack
*   [CVE-2022-41678](/security-advisories.data/CVE-2022-41678-announcement.txt) - Deserialization vulnerability on Jolokia that allows authenticated users to perform remote code execution (RCE)
*   [CVE-2021-26117](/security-advisories.data/CVE-2021-26117-announcement.txt) - ActiveMQ: LDAP-Authentication does not verify passwords on servers with anonymous bind
*   [CVE-2020-13947](/security-advisories.data/CVE-2020-13947-announcement.txt) - XSS in WebConsole
*   [CVE-2020-13920](/security-advisories.data/CVE-2020-13920-announcement.txt) - JMX MITM vulnerability
*   [CVE-2020-11998](/security-advisories.data/CVE-2020-11998-announcement.txt) - JMX remote client could execute arbitrary code
*   [CVE-2020-1941](/security-advisories.data/CVE-2020-1941-announcement.txt) - XSS in WebConsole
*   [CVE-2019-0222](/security-advisories.data/CVE-2019-0222-announcement.txt) - Corrupt MQTT frame can cause broker shutdown
*   [CVE-2018-8006](/security-advisories.data/CVE-2018-8006-announcement.txt) - ActiveMQ Web Console - Cross-Site Scripting
*   [CVE-2018-11775](/security-advisories.data/CVE-2018-11775-announcement.txt) - Missing TLS Hostname Verification
*   [CVE-2017-15709](/security-advisories.data/CVE-2017-15709-announcement.txt) - Information Leak
*   [CVE-2015-7559](/security-advisories.data/CVE-2015-7559-announcement.txt) - DoS in client via shutdown command
*   [CVE-2016-6810](/security-advisories.data/CVE-2016-6810-announcement.txt) - ActiveMQ Web Console - Cross-Site Scripting
*   [CVE-2016-0734](/security-advisories.data/CVE-2016-0734-announcement.txt) - ActiveMQ Web Console - Clickjacking
*   [CVE-2016-0782](/security-advisories.data/CVE-2016-0782-announcement.txt) - ActiveMQ Web Console - Cross-Site Scripting
*   [CVE-2016-3088](/security-advisories.data/CVE-2016-3088-announcement.txt) - ActiveMQ Fileserver web application vulnerabilities
*   [CVE-2015-5254](/security-advisories.data/CVE-2015-5254-announcement.txt) - Unsafe deserialization in ActiveMQ
*   [CVE-2015-1830](/security-advisories.data/CVE-2015-1830-announcement.txt) - Path traversal leading to unauthenticated RCE in ActiveMQ Classic
*   [CVE-2014-3576](/security-advisories.data/CVE-2014-3576-announcement.txt) - Remote Unauthenticated Shutdown of Broker (DoS)
*   [CVE-2014-3600](/security-advisories.data/CVE-2014-3600-announcement.txt) - Apache ActiveMQ XXE with XPath selectors
*   [CVE-2014-3612](/security-advisories.data/CVE-2014-3612-announcement.txt) - ActiveMQ JAAS: LDAPLoginModule allows empty password authentication and Wildcard Interpretation
