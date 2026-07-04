<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/legal/release-policy.html -->

# Apache ActiveMQ ("Classic") — Threat Model

> **STATUS: v0 DRAFT — for ActiveMQ PMC review.** This document was drafted
> *for* the ActiveMQ maintainers as a starting point they will review, correct,
> and (if they choose) adopt. It is written in the project's own voice on the
> assumption that the PMC ratifies it; until then, every claim tagged
> *(inferred)* is a proposed answer awaiting maintainer confirmation, not an
> established project position.

## §1 Header

- **Project:** Apache ActiveMQ "Classic" — the 5.x and 6.x message broker and
  its Java client. This model covers ActiveMQ Classic only. It does **not**
  cover ActiveMQ Artemis (the next-generation broker under the same PMC),
  which needs its own threat model. *(documented — ActiveMQ website
  distinguishes Classic and Artemis as separate components)*
- **Version binding:** Written against the currently security-supported
  lines — **6.2.x** and **5.19.x**. *(documented — `SECURITY.md`:
  "Only versions 6.2.x and 5.19.x receive security support"; 5.19.x carries the
  caveat "Except for EOL dependencies: Spring and Jetty".)* A report against an
  older, unsupported line (5.18.x and earlier, 5.16/5.17) is triaged against
  that line's behavior but is not eligible for a fix on that line.
- **Date:** 2026-07-04
- **Author:** ASF Security team, drafted via the threat-model-producer
  (Scovetta) rubric at the ActiveMQ PMC's request (path 3).
- **Reporting cross-reference:** Findings that violate a §8 property should be
  reported per the project's `SECURITY.md` and the ASF process at
  <https://www.apache.org/security/>. Findings that fall under §3 (out of
  scope) or §9 (properties not provided) will be closed citing this document.
  Existing advisories are published at
  <https://activemq.apache.org/security-advisories>.
- **Status:** Draft / awaiting maintainer review — 2026-07-04.
- **Provenance legend:** *(documented)* = stated in ActiveMQ's own docs,
  `SECURITY.md`, website, or default config; *(maintainer)* = confirmed by a
  PMC member through this process; *(inferred)* = reasoned from architecture,
  default config, or domain knowledge and **not yet confirmed** — every
  *(inferred)* claim has a matching numbered question in §14.
- **Draft confidence:** ~54 *(documented)* citations / 0 *(maintainer)* / 22
  *(inferred)* tags (mapping to the 10 §14 questions). This is a first
  public-artifact draft built from ActiveMQ's `SECURITY.md`, website, and
  default config; the balance should shift toward *(maintainer)* as the PMC
  reacts.

**What ActiveMQ Classic is.** ActiveMQ Classic is a Java message broker: a
long-running server process that accepts connections from message producers and
consumers over one or more wire protocols (OpenWire, AMQP, MQTT, STOMP,
WebSocket), routes and stores messages on queues and topics, and enforces
optional authentication and authorization. It ships a Java client library, a
persistence layer (KahaDB or JDBC), a management surface (JMX plus a
Jetty-hosted web console), and the ability to link brokers into a
network-of-brokers. It is a network-facing daemon, not an in-process library —
so the threat model is dominated by the boundary between untrusted network
clients and the broker, and by the operator's configuration choices.

---

## §2 Scope and intended use

ActiveMQ Classic is deployed as a **standalone or clustered broker daemon**,
typically as backing message infrastructure for an application or integration
layer. It is also **embedded in-process** (broker + VM transport) inside host
applications, and its **client library** is linked into producers and
consumers. *(documented — embedded broker and VM transport are first-class
documented deployment modes)*

Because it is a network service, there is no single "caller". The roles that
matter, each with its own trust level:

- **Client / connection (untrusted by default):** any party that can open a TCP
  (or WS/HTTP) connection to an enabled transport connector. Until an
  authentication plugin is configured, a client is anonymous. *(documented —
  default `activemq.xml` ships auth/authz plugins disabled)*
- **Authenticated principal (semi-trusted):** a client that has passed JAAS or
  `SimpleAuthenticationPlugin` login and holds a set of group memberships,
  scoped by authorization entries. *(documented — JAAS / authorization docs)*
- **Operator / administrator (fully trusted):** whoever controls
  `activemq.xml`, JAAS config, the filesystem, JMX, and the web console.
  Treated as trusted-by-design. *(documented — `SECURITY.md`: attacks that
  require admin access are excluded "since admins can perform any action by
  design")*
- **Peer broker (authenticated but adversarial):** the other end of a
  network-of-brokers link. Holds a legitimate identity but can behave
  arbitrarily. *(inferred — see §14 Q9)*

### Component-family table

| Family | Representative entry point | Touches outside the process | In this model? |
| --- | --- | --- | --- |
| OpenWire transport & protocol | `tcp://0.0.0.0:61616` OpenWire connector; `activemq-openwire-legacy` | Network (untrusted clients) | **In** |
| Alternate wire protocols | AMQP `5672`, STOMP `61613`, MQTT `1883`, WS `61614` connectors | Network (untrusted clients) | **In** (when enabled) |
| JMS `ObjectMessage` payload deserialization | Java serialized message bodies; `SERIALIZABLE_PACKAGES` allow-list | Network → JVM object graph | **In** |
| Authentication & authorization | JAAS login modules, `SimpleAuthenticationPlugin`, `authorizationEntry` | Filesystem (property files), LDAP | **In** |
| Web console / management REST | Jetty admin app on `8161`, Jolokia/JMX-over-REST | Network (management plane) | **In** |
| JMX management surface | RMI/JMX MBean server | Network / local management | **In** |
| Persistence store | KahaDB files, JDBC store | Filesystem / database | **In** (integrity/availability of stored messages) |
| Network-of-brokers | `networkConnector` broker-to-broker links | Network (peer brokers) | **In** |
| Java client library | `ActiveMQConnectionFactory`, OpenWire client marshaller | Network (broker → client) | **In** |
| Bundled examples / demos / `conf/` samples | `examples/`, sample `activemq.xml`, `credentials.properties` | Varies | **Out** — see §3 |

---

## §3 Out of scope (explicit non-goals)

- **Deployments that did not configure authentication or authorization.** The
  default distribution ships auth/authz **disabled** so the sample broker
  starts out of the box; an unauthenticated-access "exploit" against such a
  deployment is a configuration gap, not a broker vulnerability. *(documented —
  `SECURITY.md`: "Exploits that are only possible because users did not
  configure authentication or authorization" are excluded)*
- **Anything requiring administrator access.** Admins can perform any action by
  design; a finding that presupposes admin/operator control (edit
  `activemq.xml`, write JAAS files, reach JMX, log into the web console with
  operator rights) is out of model. *(documented — `SECURITY.md`)*
- **Resource-exhaustion (OOM) DoS where the operator did not set the relevant
  bound.** DoS by OOM "because users did not configure a `maxFrameSize` or
  `maxInflatedDataSize`" is excluded — the knobs exist and the operator is
  expected to set them. *(documented — `SECURITY.md`)* (The default
  `activemq.xml` in fact ships `wireFormat.maxFrameSize=10485760`.)
- **JMS-specification behaviors that look like access-control gaps but are
  not:** message selectors filtering *within* a destination the principal is
  already authorized for; `ClientId` identifier semantics per the JMS spec;
  durable-subscription access by an already-authorized connection. *(documented
  — `SECURITY.md` "not security issues" list)*
- **`BlobMessage` URI fetching.** The URIs a `BlobMessage` references are the
  client's responsibility to validate; the broker does not vouch for them.
  *(documented — `SECURITY.md`)*
- **Bundled examples, demos, and the sample `conf/` files** (including the
  `admin/admin` web-console credentials and `credentials.properties`
  placeholders). These exist for out-of-the-box startup and local
  experimentation and are not a production posture; they are threat-modeled
  separately (i.e. not at all as a security guarantee). *(inferred — see §14
  Q1)*
- **Third-party JAAS back-ends, LDAP servers, JDBC databases, and the JVM/TLS
  stack themselves.** ActiveMQ integrates with them; their own vulnerabilities
  are out of this model (though our *use* of them is in scope). *(inferred —
  see §14 Q2)*
- **ActiveMQ Artemis.** Separate codebase, separate model. *(documented)*

---

## §4 Trust boundaries and data flow

The primary trust boundary is the **transport connector**: the moment bytes
arrive from a network client and are unmarshalled into broker-internal
protocol commands and message objects. Everything a client sends — protocol
frames, headers, destination names, selectors, and message bodies — is
untrusted until an authentication plugin has established a principal, and
remains authorization-scoped thereafter. *(inferred — see §14 Q3)*

Secondary boundaries:

- **Management plane (web console / JMX / Jolokia) vs. broker core.** The
  management surface is intended for operators, not clients; reaching it is a
  distinct boundary from reaching a transport connector. *(inferred — see §14
  Q4)*
- **Broker vs. persistence store.** KahaDB files and the JDBC database are
  trusted storage: the broker assumes it is the only writer and that the store
  has not been tampered with out of band. *(inferred — see §14 Q5)*
- **Broker vs. peer broker.** A network-of-brokers link crosses into another
  administrative domain's broker. *(inferred — see §14 Q9)*

**Reachability precondition per family** (the test a triager applies before
anything else):

- OpenWire / AMQP / MQTT / STOMP / WS finding — in-model only if reachable from
  bytes an unauthenticated or authenticated *client* can send to an
  **enabled** connector. A finding in `activemq-openwire-legacy` reachable from
  a crafted OpenWire command is the canonical in-model case (this is the
  CVE-2023-46604 class). *(documented — advisory history)*
- `ObjectMessage` deserialization finding — in-model only if the offending
  class is inside the configured `SERIALIZABLE_PACKAGES` allow-list (or the
  operator/client has widened it). A gadget outside the allow-list that is
  nonetheless deserialized is a `VALID` allow-list-bypass finding; a gadget the
  operator explicitly allow-listed is out of model. *(documented — allow-list
  docs; see §11a)*
- Authorization finding — in-model only if a principal reaches a destination or
  operation their granted `read`/`write`/`admin` entries should forbid.
  *(documented — authorization docs)*
- Web-console / JMX finding — in-model only if reachable without operator
  credentials on a surface intended to require them. *(inferred — see §14 Q4)*

---

## §5 Assumptions about the environment

- **Runtime:** a JVM of the version the release line targets, with a
  conformant TLS provider. Java serialization semantics are assumed to be the
  standard JDK's. *(inferred — see §14 Q6)*
- **Clock / TLS:** message expiry, TLS certificate validation, and JAAS
  credential checks assume a correct system clock and a correctly configured
  trust store. *(inferred — see §14 Q6)*
- **Filesystem:** the KahaDB data directory, JAAS property files
  (`users.properties`, `groups.properties`), keystores, and `activemq.xml` are
  on storage writable only by the broker's OS user and trusted operators. The
  broker assumes exclusive ownership of its KahaDB directory. *(inferred — see
  §14 Q5)*
- **Network placement:** the broker expects the operator to place it behind
  appropriate network controls; the project's stance is defense-in-depth
  (authentication, SSL/TLS, network restrictions, OS-level permissions) and the
  defaults are for developer testing, not production. *(documented —
  `SECURITY.md`: "Users are expected to secure their environments")*
- **Credential reload:** from 5.12 onward, JAAS property files are re-read only
  if `reload=true` is set; otherwise credentials are read once at startup. An
  operator who expects live credential revocation without that flag is
  mis-assuming. *(documented — security docs)*

**What the broker does to its host (negative-side-effect inventory).** The
broker *does* open listening sockets (per enabled connectors), open outbound
sockets (network-of-brokers, LDAP, JDBC, `BlobMessage` fetch by clients), read
and write its KahaDB directory, read environment (`ACTIVEMQ_OPTS`,
`ACTIVEMQ_HOME`), and start a JMX/RMI server and a Jetty container when those
are enabled. It is a full daemon and makes **no** "no-side-effects" promise.
The relevant question for an integrator is not *whether* it touches the host
but *which* connectors and management surfaces are enabled. *(inferred — see
§14 Q7)*

### §5a Build-time and configuration variants

ActiveMQ's security envelope is dominated by **runtime configuration**, and
several load-bearing knobs ship in their less-secure state so the sample broker
starts unattended. This is the crux of the model: most "vulnerabilities"
reported against a default install are configuration gaps (§3), and the
maintainer stance below is what licenses that call.

| Knob | Default | Effect on the model | Maintainer stance |
| --- | --- | --- | --- |
| Authentication plugin (JAAS / `SimpleAuthenticationPlugin`) | **disabled** in sample `activemq.xml` | With it off, all clients are anonymous | Dev-convenience; operator must enable — *(documented, `SECURITY.md`)* |
| Authorization plugin (`authorizationPlugin` / `authorizationEntry`) | **disabled** in sample config | With it off, any connected client can use any destination | Dev-convenience; operator must enable — *(documented)* |
| Enabled transport connectors | only OpenWire `tcp://0.0.0.0:61616` enabled; AMQP/STOMP/MQTT/WS commented out | Each enabled connector is a new untrusted surface | Operator opts in per protocol — *(documented, default config)* |
| `wireFormat.maxFrameSize` | `10485760` (10 MB) in default OpenWire URI | Caps a single frame; unset → unbounded-frame OOM | Operator-tunable; OOM when unset is out of model — *(documented, `SECURITY.md`)* |
| `maxInflatedDataSize` | operator-set | Caps decompressed payload; unset → decompression-bomb OOM | Operator must set; OOM when unset is out of model — *(documented, `SECURITY.md`)* |
| `org.apache.activemq.SERIALIZABLE_PACKAGES` (via `ACTIVEMQ_OPTS`) | a default broker-necessary allow-list | Widening (esp. `=*`) re-opens `ObjectMessage` RCE | Narrow list is the supported posture; `*` is explicitly unsafe — *(documented)* |
| Client `setTrustAllPackages(true)` / `setTrustedPackages(...)` | `trustAll` off; default trusted list | Client-side deserialization allow-list; `trustAll` disables it | For testing only per docs — *(documented)* |
| Web-console `authenticate` (jetty.xml) + `jetty-realm.properties` | console present, `admin/admin` sample creds | Unauthenticated or default-cred console = full operator access | Operator must change creds / enable auth / restrict — *(documented, web-console docs)* |
| Anonymous access (`anonymousAccessAllowed`) | off unless set (5.4.0+) | Permits unauthenticated connections as `anonymous` | Explicit operator opt-in — *(documented)* |
| SSL/TLS vs. plain connectors | plain TCP OpenWire in default config | Plain connectors carry traffic unencrypted | Operator chooses `ssl://`/`nio+ssl://` for production — *(documented)* |

**Insecure-default ruling.** Per `SECURITY.md`, the disabled-auth/authz and
unset-resource-bound defaults are **dev-conveniences the operator is expected
to change**, so a report against them is `OUT-OF-MODEL: non-default-build`
(non-default posture), not `VALID`. This ruling reshapes §8, §10, §11a,
and §13 and is confirmed documented — but §14 Q1/Q8 ask the PMC to
ratify the exact boundary (e.g. is the `admin/admin` web-console credential in
the same bucket?).

---

## §6 Assumptions about inputs

The broker accepts protocol frames from network clients, message bodies
(including serialized Java objects), management requests, JAAS credentials, and
peer-broker traffic. The following table is the per-surface trust matrix a
triager uses to look up a specific reported sink.

| Surface / message | Parameter | Attacker-controllable? | Caller/operator must enforce |
| --- | --- | --- | --- |
| OpenWire connector (`:61616`) | protocol command bytes, class-type fields | **yes** (any client to enabled connector) | enable authn; keep `SERIALIZABLE_PACKAGES` narrow; patch OpenWire marshaller |
| OpenWire / all connectors | frame size | **yes** | set `wireFormat.maxFrameSize` |
| Compressed payloads | inflated size | **yes** | set `maxInflatedDataSize` |
| Any connector | destination name, headers, selector string | **yes** | authorization entries; treat selectors as filters not access control |
| `ObjectMessage` body | serialized Java object graph | **yes** | allow-list packages; never `SERIALIZABLE_PACKAGES=*` in production |
| `BlobMessage` | referenced URI | **yes** (from producer) | **client** validates URI before fetch |
| AMQP / MQTT / STOMP / WS | protocol-specific frames | **yes** (when enabled) | enable authn; per-protocol limits |
| JAAS login | username / password / TLS cert | **yes** | correct login-module config; `reload=true` for live revocation |
| Authorization | principal's group memberships | derived from trusted JAAS back-end | keep JAAS store authoritative |
| Web console / Jolokia (`:8161`) | HTTP requests, MBean operations | **yes** if reachable | require auth; change default creds; restrict network |
| JMX | MBean invocations | **yes** if RMI port exposed | restrict/authenticate JMX; do not expose |
| Network-of-brokers | peer commands, forwarded messages | **yes** (peer is adversarial) | authenticate the link; scope forwarded destinations |
| KahaDB / JDBC store | on-disk / in-DB records | no — trusted storage | OS/DB permissions; exclusive broker ownership |

Size/shape/rate: messages are streamed and may be large; the broker relies on
`maxFrameSize`, `maxInflatedDataSize`, `maximumConnections` (default `1000` on
the sample OpenWire connector), producer flow control, and memory/store limits
to bound resource use — **all of which are operator-tuned**. *(documented for
the named knobs; §14 Q8 for the completeness of this list)*

---

## §7 Adversary model

**Primary in-scope adversary:** a network client that can reach an **enabled
transport connector** on a broker where the operator **has** configured
authentication and authorization. The adversary may be fully unauthenticated
(if the operator allows anonymous or a connector is exposed) or an
authenticated low-privilege principal trying to exceed their granted
`read`/`write`/`admin` scope. Capabilities: send arbitrary protocol frames and
message bodies, open many connections, craft malformed or oversized frames,
craft serialized-object payloads, and replay. *(inferred — see §14 Q3)*

**Also in scope:** a network attacker on the path of a plaintext connector
(observe/modify traffic where TLS was not used — though the operator's choice
to run plaintext bounds this), and an **authenticated-but-Byzantine peer
broker** on a network-of-brokers link. *(inferred — see §14 Q3, Q9)*

**Explicitly out of scope:**

- **The operator/administrator.** Anyone who can edit `activemq.xml`, write JAAS
  files, reach JMX, or log into the web console as operator has already won —
  admin actions are by design. *(documented — `SECURITY.md`)*
- **A client attacking a broker with auth/authz turned off.** That is a
  configuration gap, not an adversary the model defends against. *(documented)*
- **A local attacker with filesystem access to the KahaDB directory, keystores,
  or credential files.** Storage is trusted. *(inferred — see §14 Q5)*
- **Attackers exploiting the operator's failure to set documented resource
  bounds.** OOM from unset `maxFrameSize`/`maxInflatedDataSize` is excluded.
  *(documented)*

---

## §8 Security properties the project provides

Each property holds **only** under the configuration the operator is expected
to apply (§5a, §10). "Given valid configuration" below means: an
authentication plugin is enabled, an authorization plugin with appropriate
entries is enabled, resource bounds are set, and `SERIALIZABLE_PACKAGES` is
kept narrow.

1. **Client authentication.** Given a configured JAAS or
   `SimpleAuthenticationPlugin`, the broker rejects connections that do not
   present valid credentials (or a valid TLS client cert, per config).
   *Violation symptom:* an unauthenticated connection performs
   authenticated-only operations. *Severity:* security-critical (auth bypass →
   CVE). *(documented — authentication docs)*
2. **Destination authorization.** Given a configured authorization plugin, a
   principal may only `read`/`write`/`admin` destinations permitted by its
   group's `authorizationEntry` set (wildcards included), and advisory-topic
   access follows the documented rules. *Violation symptom:* a principal
   consumes from / produces to / creates a destination outside its granted
   scope. *Severity:* security-critical (authz bypass → CVE). *(documented —
   authorization docs)*
3. **Deserialization allow-listing for `ObjectMessage`.** With the default (or
   a narrowed) `SERIALIZABLE_PACKAGES` list, the broker and client refuse to
   deserialize classes outside the allow-list, blocking arbitrary-gadget
   `ObjectMessage` RCE. *Violation symptom:* a class outside the configured
   allow-list is instantiated during message unmarshalling. *Severity:*
   security-critical (RCE — this control exists because of CVE-2015-5254).
   *(documented — allow-list docs)*
3a. **OpenWire protocol unmarshalling does not instantiate arbitrary
   classpath classes.** The OpenWire marshaller must not be coercible into
   reflectively constructing attacker-named classes from protocol fields.
   *Violation symptom:* a crafted OpenWire command causes arbitrary class
   instantiation / command execution. *Severity:* security-critical (this is
   the CVE-2023-46604 class; the property is the *post-fix* guarantee).
   *(documented — advisory history)*
4. **Transport confidentiality/integrity when TLS is configured.** On
   `ssl://` / `nio+ssl://` connectors with a correct keystore/truststore,
   client-broker (and broker-broker) traffic is encrypted and the endpoint
   authenticated. *Violation symptom:* traffic readable/modifiable on a
   connector configured for TLS, or accepted cert that should be rejected.
   *Severity:* security-critical. *(documented — SSL transport docs)*
5. **Management-surface access control when enabled.** With web-console
   authentication enabled and non-default credentials, and with JMX
   restricted, the management plane is reachable only by operators. *Violation
   symptom:* an unauthenticated party reads/mutates broker state via the
   console, Jolokia, or JMX on a surface configured to require auth. *Severity:*
   security-critical. *(inferred — see §14 Q4)*
6. **Resource bounds are honored once set.** When the operator sets
   `maxFrameSize`, `maxInflatedDataSize`, `maximumConnections`, memory/store
   limits, and flow control, the broker enforces them; a single client cannot
   exceed the configured frame/inflation/connection ceilings. *Violation
   symptom:* the broker exceeds a **configured** bound (accepts an oversized
   frame, inflates past the cap). *Severity:* security-relevant (DoS) **only
   when a bound is set and breached**; unset-bound OOM is out of model per §3.
   *(documented — `SECURITY.md`, default config)*
7. **Message durability/integrity of the persistence store.** Given exclusive,
   uncorrupted storage, persistent messages survive restart and are delivered
   per their QoS. *Violation symptom:* acknowledged persistent messages lost or
   corrupted absent hardware/OS fault. *Severity:* correctness-critical (data
   loss); not a confidentiality/authz property. *(inferred — see §14 Q5)*

---

## §9 Security properties the project does *not* provide

- **No security from the default distribution.** Out of the box the broker has
  authentication and authorization **disabled** and ships sample credentials;
  it is not secure until configured. The defaults are for developer testing.
  *(documented — `SECURITY.md`, default config)*
- **No protection for operators against themselves.** The model provides
  nothing against an admin, and nothing against a client on an unconfigured
  broker. *(documented)*
- **No unbounded-resource defense by default.** Without `maxFrameSize` /
  `maxInflatedDataSize` (and connection/memory limits) set, a client can drive
  the broker to OOM; that is explicitly the operator's bound to set.
  *(documented — `SECURITY.md`)*
- **No safety when the deserialization allow-list is widened.** Setting
  `SERIALIZABLE_PACKAGES=*` or client `setTrustAllPackages(true)`, or
  allow-listing a package that contains a gadget, re-opens `ObjectMessage`
  deserialization RCE. The allow-list is only as safe as its contents.
  *(documented — allow-list docs)*
- **No transport security on plaintext connectors.** Plain `tcp://`
  OpenWire (the default), and plain AMQP/STOMP/MQTT/WS, carry credentials and
  message bodies in cleartext; confidentiality/integrity require the operator
  to choose an SSL variant. *(documented — OpenWire docs warn "traffic is
  unencrypted")*

**False friends** (things that look like a security control but are not):

- **JMS message selectors are a *filter*, not an access-control boundary.**
  Selectors filter messages *within* a destination the principal is already
  authorized for; they do not restrict which destinations a principal may
  reach. Reporting "a selector let me see a message" on an authorized
  destination is not a vuln. *(documented — `SECURITY.md`)*
- **`ClientId` is an identifier, not an authentication token.** Its behavior
  follows the JMS spec; do not treat it as proof of identity. *(documented —
  `SECURITY.md`)*
- **The `SERIALIZABLE_PACKAGES` allow-list is a *package* gate, not a
  gadget-chain analysis.** It bounds which packages may deserialize; it does
  not certify that every class in an allowed package is gadget-free. *(inferred
  — see §14 Q10)*
- **The web console / Jolokia is a management tool, not a client
  authorization layer.** Reaching it is operator-equivalent; it is not scoped
  by JMS `authorizationEntry` permissions. *(inferred — see §14 Q4)*
- **`BlobMessage` URIs are not validated by the broker.** They look like broker
  data but the fetch and its target are the client's trust decision.
  *(documented — `SECURITY.md`)*

**Well-known attack classes left to the caller/operator:** Java deserialization
gadget-chain RCE (bounded, not eliminated, by the allow-list); SSRF via
client-controlled URIs (`BlobMessage`, and any operator-configured outbound
fetch); XXE/entity attacks in any XML the operator feeds the broker;
credential-stuffing/brute-force against enabled auth (rate limiting is the
operator's); amplification/DoS from unbounded connections or subscriptions when
limits are unset. *(inferred except where cited above — see §14 Q10)*

---

## §10 Downstream (operator) responsibilities

The broker's §8 properties hold only if the **operator/deployer** does the
following. This is a contract, not a how-to.

1. **Enable authentication** (JAAS or `SimpleAuthenticationPlugin`) before
   exposing any connector beyond localhost. *(documented)*
2. **Enable authorization** with `authorizationEntry` rules scoped to least
   privilege, including correct `ActiveMQ.Advisory.>` grants. *(documented)*
3. **Set resource bounds:** `wireFormat.maxFrameSize`, `maxInflatedDataSize`,
   `maximumConnections`, memory/store usage limits, and producer flow control.
   Unset bounds are your DoS exposure, not the broker's bug. *(documented)*
4. **Keep `SERIALIZABLE_PACKAGES` as narrow as possible**; never `=*` in
   production; never `setTrustAllPackages(true)` outside testing. Only
   allow-list packages you trust to be gadget-free. *(documented)*
5. **Patch promptly** — especially the OpenWire marshaller (CVE-2023-46604
   class) and client library; run a supported line (6.2.x / 5.19.x).
   *(documented — `SECURITY.md`)*
6. **Use TLS** (`ssl://` / `nio+ssl://`) for any connector carrying credentials
   or sensitive payloads; the default plain OpenWire connector is not
   encrypted. *(documented)*
7. **Lock down the management plane:** change the `admin/admin` web-console
   credentials, enable console authentication, do not expose Jolokia/JMX to
   untrusted networks, and prefer binding the console to a management
   interface. *(documented — web-console docs; §14 Q4 for exact defaults)*
8. **Enable only the connectors you need** — AMQP/STOMP/MQTT/WS are off by
   default; each you enable is a new untrusted surface to authenticate and
   bound. *(documented — default config)*
9. **Protect storage and credential files:** restrict OS permissions on the
   KahaDB directory, keystores, JAAS property files, and `activemq.xml`; the
   broker assumes exclusive, trusted storage. Set `reload=true` if you need
   live credential revocation. *(documented for reload; §14 Q5 for storage)*
10. **Authenticate and scope network-of-brokers links**; treat a peer broker as
    adversarial. *(inferred — see §14 Q9)*

---

## §11 Known misuse patterns

- **Running the sample/default broker in production** with auth/authz disabled
  and sample credentials — treating the developer-convenience defaults as a
  deployment posture. *(documented — defaults are for testing)*
- **Exposing the OpenWire (or any) connector directly to the public internet**
  without authentication or TLS. *(inferred — see §14 Q3)*
- **Widening the deserialization allow-list** (`SERIALIZABLE_PACKAGES=*` or
  `setTrustAllPackages(true)`) to "make `ObjectMessage` work", re-opening RCE.
  *(documented)*
- **Treating JMS selectors or `ClientId` as security controls** — using a
  selector to "hide" messages or a `ClientId` as authentication. *(documented)*
- **Leaving the web console / Jolokia / JMX reachable** on an untrusted network
  with default or no authentication. *(inferred — see §14 Q4)*
- **Not setting resource bounds** and then treating the resulting OOM as a
  broker vulnerability. *(documented)*
- **Fetching `BlobMessage` URIs without client-side validation**, enabling
  SSRF/arbitrary-fetch from the consumer. *(documented)*

## §11a Known non-findings (recurring false positives)

These are patterns scanners, fuzzers, and AI analyzers repeatedly flag that are
**not** bugs under this model. This section is the highest-leverage input for
suppressing scan noise.

- **"Authentication/authorization can be bypassed on the default broker."** The
  default ships auth/authz disabled by design; this is `OUT-OF-MODEL:
  non-default-build`, licensed by §3 and `SECURITY.md`. *(documented)*
- **"Default web-console credentials `admin/admin`."** Sample credential in the
  distribution meant to be changed; `OUT-OF-MODEL: unsupported-component` /
  non-default posture per §3. *(documented — sample creds; §14 Q1)*
- **"Unbounded memory / OOM on large or compressed messages."** If the reporter
  did not set `maxFrameSize` / `maxInflatedDataSize`, this is `BY-DESIGN:
  property-disclaimed` per §3/§9. If a *configured* bound is breached, it is
  `VALID`. *(documented)*
- **"Java deserialization RCE via `ObjectMessage`."** If the class is outside a
  reasonable `SERIALIZABLE_PACKAGES` list, the allow-list already blocks it —
  `KNOWN-NON-FINDING` unless a genuine allow-list *bypass* is shown (then
  `VALID`). If the operator set `SERIALIZABLE_PACKAGES=*`, it is `OUT-OF-MODEL:
  non-default-build`. *(documented)*
- **"Plaintext credentials/traffic on the OpenWire connector."** The default
  connector is plain TCP by documented design; using TLS is the operator's
  choice — `BY-DESIGN` per §9 unless a *configured* TLS connector fails to
  encrypt (then `VALID`). *(documented)*
- **"A message selector returned a message I filtered on"** / **"`ClientId`
  reuse"** / **"durable subscriber saw messages"** on an authorized
  destination — explicit non-issues per `SECURITY.md`; `BY-DESIGN` per §9.
  *(documented)*
- **"`BlobMessage` SSRF."** URI validation is the client's job; `BY-DESIGN` per
  §3/§9. *(documented)*
- **Findings in `examples/` or bundled demos.** `OUT-OF-MODEL:
  unsupported-component` per §3. *(inferred — see §14 Q1)*
- **CVE history against unsupported lines** (5.18.x and earlier). Triaged
  against that line but not fix-eligible; run a supported line. *(documented —
  `SECURITY.md` supported-versions)*

## §12 Conditions that would change this model

- A **new transport protocol** or wire-format command, or a change to OpenWire
  unmarshalling.
- A change to the **default posture** — e.g. shipping auth/authz enabled, or
  changing which connectors or resource bounds default on. (This would move
  several §11a items from out-of-model toward `VALID`.)
- A change to the **deserialization allow-list** default set or mechanism.
- A **new management surface** or a change to web-console / Jolokia / JMX
  default authentication.
- A change to the **supported-version** set in `SECURITY.md`.
- **Model-incompleteness evidence:** any report that cannot be routed to a
  single §13 disposition is a `MODEL-GAP` and triggers a revision of §8/§9
  rather than an ad-hoc call.

## §13 Triage dispositions

| Disposition | Meaning | Licensed by |
| --- | --- | --- |
| `VALID` | Violates a §8 property via an in-scope adversary/input on a properly-configured broker (e.g. authz bypass, OpenWire-marshaller RCE, allow-list bypass, TLS failure on a TLS connector, breach of a *configured* resource bound). | §8, §6, §7 |
| `VALID-HARDENING` | No §8 property broken, but the API/config makes a §11 misuse easy enough to warrant hardening. Fixed at maintainer discretion; typically no CVE. | §11 |
| `OUT-OF-MODEL: trusted-input` | Requires control of an input the model marks trusted (KahaDB/JDBC store, JAAS back-end data, `activemq.xml`). | §6 |
| `OUT-OF-MODEL: adversary-not-in-scope` | Requires operator/admin access or local filesystem access. | §7 |
| `OUT-OF-MODEL: unsupported-component` | Lands in `examples/`, demos, or sample `conf/`. | §3 |
| `OUT-OF-MODEL: non-default-build` | Only manifests with auth/authz disabled, resource bounds unset, `SERIALIZABLE_PACKAGES` widened, or other discouraged non-default posture. | §5a, §3 |
| `BY-DESIGN: property-disclaimed` | Concerns a property §9 explicitly does not provide (plaintext default, selector-as-ACL, `BlobMessage` URI, unset-bound OOM). | §9 |
| `KNOWN-NON-FINDING` | Matches a §11a recurring false positive. | §11a |
| `MODEL-GAP` | Cannot be routed to any of the above; triggers a §12 revision. | §12 |

## §14 Open questions for the maintainers

Each question states a **proposed answer** for the PMC to confirm, correct, or
strike, and names where the answer lands. Grouped in waves.

**Wave 1 — scope & the insecure-default boundary (reshapes §3, §5a, §11a, §13):**

1. *(→ §3, §11a)* We treat the bundled `examples/`, demos, sample
   `activemq.xml`, and the `admin/admin` web-console credential /
   `credentials.properties` placeholders as **out of model** (developer
   convenience, not a production posture), so findings there are
   `OUT-OF-MODEL`. Is the `admin/admin` credential specifically in that bucket,
   or do you consider a shipped default credential a `VALID-HARDENING` item?
2. *(→ §3)* We put third-party JAAS back-ends, LDAP/JDBC servers, and the
   JVM/TLS stack **out of model** (our *use* of them stays in). Correct?
3. *(→ §4, §7, §11)* We state the primary trust boundary is the transport
   connector and the primary adversary is a network client reaching an
   **enabled** connector on an **auth-configured** broker (plus a
   path-plaintext attacker where TLS was not chosen). Confirm this is the right
   framing and that "exposing a connector to the internet unauthenticated" is a
   §11 misuse, not a broker defect.

**Wave 2 — management plane & storage (reshapes §4, §5, §8, §9):**

4. *(→ §4, §8, §9)* We model the web console (Jetty, `:8161`), Jolokia, and JMX
   as an **operator-only management plane** whose access control is separate
   from JMS `authorizationEntry` permissions, and we're unsure of the exact
   shipped defaults (does the console ship with `authenticate=true`? does it
   bind to `0.0.0.0` or localhost? is Jolokia exposed by default?). Please
   confirm the default authentication state and bind address of the management
   surfaces.
5. *(→ §4, §5, §7, §8)* We assume the KahaDB directory, JDBC store, keystores,
   and credential files are **trusted storage** with the broker as exclusive
   writer, and that a local-filesystem attacker is out of scope. Confirm; and
   confirm that message durability/integrity (§8 #7) is a correctness — not a
   confidentiality/authz — guarantee.
6. *(→ §5)* We assume a conformant JVM, correct system clock, and correctly
   configured TLS trust store as environmental preconditions. Anything else
   load-bearing (specific JDK floor per line, RMI/JMX assumptions)?
7. *(→ §5)* Our "what the broker does to the host" inventory says it opens
   listening + outbound sockets, owns its KahaDB dir, reads `ACTIVEMQ_OPTS`,
   and starts JMX/Jetty when enabled — and makes **no** no-side-effects
   promise. Is that inventory complete and correct?

**Wave 3 — resource bounds, peers, deserialization edges (reshapes §6, §8, §9):**

8. *(→ §5a, §6)* Is the resource-bound set we list (`maxFrameSize`,
   `maxInflatedDataSize`, `maximumConnections`, memory/store limits, producer
   flow control) the **complete** set an operator must set to make DoS
   in-model, or are there others (per-destination limits, connection-rate
   limits)? And is "breach of a *configured* bound" the correct line for `VALID`
   vs `BY-DESIGN`?
9. *(→ §2, §7, §8, §10)* We model a network-of-brokers peer as
   **authenticated-but-adversarial** and expect operators to authenticate/scope
   the link. Do you provide any safety guarantee across a broker link (e.g.
   forwarded-message authorization), or is a compromised/malicious peer wholly
   the operator's trust decision?
10. *(→ §9)* We frame `SERIALIZABLE_PACKAGES` as a **package-level gate, not a
    gadget-chain guarantee** — an allow-listed package containing a gadget is
    the operator's risk, and only an allow-list *bypass* (a class outside the
    list getting deserialized) is `VALID`. Confirm this is the right line, and
    confirm the default trusted-package list is considered gadget-safe by the
    project.

---

*End of v0 draft. Provenance counts and every §14 answer should be folded back
in as the PMC reviews; promote *(inferred)* → *(maintainer)* and retire the
matching question as each is resolved.*
