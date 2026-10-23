---
title: "LDAP Broker Discovery Mechanism"
description: "Let ActiveMQ Classic brokers discover network connections from an LDAP v3 directory with ldapNetworkConnector, including server support and examples."
slug: /ldap-broker-discovery-mechanism
---


Configuring network topologies can be quite tedious when the number of brokers in the system is large. To help ease the configuration overhead for these types of situations, a broker can be configured to look up its broker connections using a LDAP v3 directory server.

> **Note**
> 
> The basic feature was added to satisfy [AMQ-358](https://issues.apache.org/activemq/browse/AMQ-358). There are known problems and limitations with this implementation. These deficiencies have been addressed in [AMQ-1587](https://issues.apache.org/activemq/browse/AMQ-1587). The features discussed on this page require the patch attached to JIRA issue [AMQ-1587](https://issues.apache.org/activemq/browse/AMQ-1587). This patch should apply cleanly to the ActiveMQ 5.0.0 release or the current development trunk.

LDAP v3 Directory Server Compliance
-----------------------------------

The following table lists a known subset of directory servers and their compliance to work with the LDAP discovery feature. Most LDAP v3 directory servers will support this feature if they properly implement the [RFC2307](http://www.ietf.org/rfc/rfc2307.txt) schemas. In order to support the persistent search capabilities the server must implement the extension defined in [draft-ietf-ldapext-psearch-03.txt](http://www.ietf.org/proceedings/01aug/I-D/draft-ietf-ldapext-psearch-03.txt).

Vendor|Product|Version|[RFC2307](http://www.ietf.org/rfc/rfc2307.txt)|[draft-ietf-ldapext-psearch-03.txt](http://www.ietf.org/proceedings/01aug/I-D/draft-ietf-ldapext-psearch-03.txt)
---|---|---|---|---
Apache|ApacheDS|1.0.x|![(tick)](/img/check.png)|![(tick)](/img/check.png)
Microsoft|Active Directory|Windows 2000, Windows 2003|![(warning)](/img/warning.png)|![(error)](/img/error.png)
Microsoft|Active Directory|Windows 2003 R2|![(tick)](/img/check.png)|![(error)](/img/error.png)
Sun|OpenDS|0.9.x|![(tick)](/img/check.png)|![(tick)](/img/check.png)
OpenLDAP|OpenLDAP|2.3.x, 2.4.x|![(tick)](/img/check.png)|![(error)](/img/error.png)

![(warning)](/img/warning.png) LDAP v3 directory server which do not support [RFC2307](http://www.ietf.org/rfc/rfc2307.txt) by default. Support can be added by manually importing them. See vendor specific setup requirements on how to do this.  
![(error)](/img/error.png) LDAP v3 directory servers which do not support the [draft-ietf-ldapext-psearch-03.txt](http://www.ietf.org/proceedings/01aug/I-D/draft-ietf-ldapext-psearch-03.txt).

LDAP Network Connector Properties
---------------------------------

Property|Default|Required|Description
---|---|---|---
uri|null|**Yes**|The URI of the LDAP v3 Server to connect to (i.e. ldap://host:port, failover://(ldap://host1:port,ldap://host2:port).
base|null|**Yes**|The RDN base used as the root for the search criteria.
user|null|**Yes**, if not using anonymousAuthentication|The username needed to bind to the server.
password|null|**Yes**, if not using anonymousAuthentication|The password needed to bind to the server.
anonymousAuthentication|false|**Yes**, if not using user/password|Enable if you want to bind to the server anonymously. This is recommended over using user/password properties since your login credentials will not be stored in an unencrypted XML file.
searchEventListener|false|**No**|Enable if you want the broker to stay in sync with changes made to entries matching the search criteria.
searchScope|ONELEVEL_SCOPE|**No**|Can be any of search scopes defined by javax.naming.directory.SearchControls class. OBJECT_SCOPE - search the named object defined by base. ONELEVEL_SCOPE - search one level of the base. SUBTREE_SCOPE - search entire subtree rooted at the base.
searchFilter|(&(objectClass=ipHost)(objectClass=ipService))|**No**|Can be any filter that conforms to [RFC2254](http://www.ietf.org/rfc/rfc2254.txt). If a custom one is specified the actual search filter used will be (&(&(objectClass=ipHost)(objectClass=ipService))(USER CUSTOM))

> **Other Properties**
> 
> All of the properties defined in [Networks of Brokers](http://activemq.apache.orgFeatures/Clustering/networks-of-brokers) are also available to the ldapNetworkConnector. Any of the properties defined for a normal networkConnector will be used as connection parameters to any discovered brokers matching the search criteria.

Example 1: Simple Network of Brokers
------------------------------------

### Network Configuration

Topology|LDAP v3 Directory Structure|Entry
---|---|---
![](/img/Example1-Topology.jpg)|![](/img/Example1-DirectoryStructure.jpg)|![](/img/Example1-Entry.jpg)

### ActiveMQ Configuration (activemq.xml)

**srv-a.mydomain.com**
```
<broker brokerName="srv-a.mydomain.com" ...>
   ...

   <networkConnectors>
      <ldapNetworkConnector uri="ldap://myldap.mydomain.com:389"
                            base="dc=brokers,dc=mydomain,dc=com"
                            anonymousAuthentication="true"
                            searchFilter="(cn=*)"
                            searchScope="SUBTREE_SCOPE"
                            />
   </networkConnectors>

   ...
</broker>
```

**srv-b.mydomain.com**
```
<broker brokerName="srv-b.mydomain.com" ...>
   ...

   <networkConnectors/>
      <!-- NO NETWORK CONNECTORS -->
   </networkConnectors>

   ...
</broker>
```

Example 2: Larger Network of Brokers
------------------------------------

### Network Configuration

Topology|LDAP v3 Directory Structure
---|---
![](/img/Example2-Topology.jpg)|![](/img/Example2-DirectoryStructure.jpg)

### ActiveMQ Configuration (activemq.xml)

**srv-a.mydomain.com**
```
<broker brokerName="srv-a.mydomain.com" ...>
   ...

   <networkConnectors>
      <ldapNetworkConnector uri="ldap://myldap.mydomain.com:389"
                            base="dc=brokers-for-srv-a,dc=mydomain,dc=com"
                            anonymousAuthentication="true"
                            searchFilter="(cn=*)"
                            searchScope="SUBTREE_SCOPE"
                            networkTTL="2"
                            />
      <!-- networkTTL=2 since we want messages to flow from srv-a to srv-c -->
   </networkConnectors>

   ...
</broker>
```

**srv-b.mydomain.com**
```
<broker brokerName="srv-b.mydomain.com" ...>
   ...

   <networkConnectors/>
      <ldapNetworkConnector uri="ldap://myldap.mydomain.com:389"
                            base="dc=brokers-other,dc=mydomain,dc=com"
                            anonymousAuthentication="true"
                            searchFilter="(cn=*)"
                            searchScope="SUBTREE_SCOPE"
                            />
   </networkConnectors>

   ...
</broker>
```

**srv-c.mydomain.com**
```
<broker brokerName="srv-c.mydomain.com" ...>
   ...

   <networkConnectors/>
      <!-- NO NETWORK CONNECTORS -->
   </networkConnectors>

   ...
</broker>
```

**srv-d.mydomain.com**
```
<broker brokerName="srv-d.mydomain.com" ...>
   ...

   <networkConnectors/>
      <ldapNetworkConnector uri="ldap://myldap.mydomain.com:389"
                            base="dc=brokers-other,dc=mydomain,dc=com"
                            anonymousAuthentication="true"
                            searchFilter="(cn=*)"
                            searchScope="SUBTREE_SCOPE"
                            />
   </networkConnectors>

   ...
</broker>
```
