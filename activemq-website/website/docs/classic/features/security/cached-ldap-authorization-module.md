---
title: "Cached LDAP Authorization Module"
description: "Set up the ActiveMQ Classic cachedLDAPAuthorizationMap to load destination permissions from Apache DS or OpenLDAP, with LDIF samples and push or pull updates."
slug: /cached-ldap-authorization-module
---


> **Available since 5.6**
> 
> Cached LDAP authorization module is an implementation of an default authorization module that initializes and updates data from LDAP. It supports all standard features like defining wildcard policy entries and entry for temporary destinations.

Initializing
------------

We provide two ldif files for easy starting. The first one is for [Apache Directory Server](http://directory.apache.org/) ([ldif](https://github.com/apache/activemq/tree/main/activemq-unit-tests/src/test/resources/org/apache/activemq/security/activemq-apacheds.ldif)), which we use in embedded mode for testing. For an example on how to initialize the embedded ApacheDS with this ldif file take a look at [CachedLDAPSecurityTest](https://github.com/apache/activemq/tree/main/activemq-unit-tests/src/test/java/org/apache/activemq/security/CachedLDAPSecurityTest.java)

The other one is for [OpenLDAP](http://www.openldap.org/) ([ldif](https://github.com/apache/activemq/tree/main/activemq-unit-tests/src/test/resources/org/apache/activemq/security/activemq-openldap.ldif))

The provided ldif and examples assume `dc=activemq,dc=apache,dc=org` suffix to be used for entries, so the configuration similar to the one shown in the following snippet
```
suffix          "dc=activemq,dc=apache,dc=org"
rootdn          "cn=admin,dc=activemq,dc=apache,dc=org"
# Cleartext passwords, especially for the rootdn, should
# be avoid.  See slappasswd(8) and slapd.conf(5) for details.
# Use of strong authentication encouraged.
rootpw          {SSHA}lfAYn54xCFghgQv5B2Kqn3d3eLojqxtS
```
should be put into your `slapd.conf`

To initialize your (properly configured) OpenLDAP do something like
```
ldapadd -x -D "cn=admin,dc=activemq,dc=apache,dc=org" -w sunflower -f activemq-openldap.ldif
```

Configuring
-----------

Once entries are in LDAP, you can configure the module to load entries from there. A default values are adapted for embedded Apache DS server, so all you have to do in that case is add your plugin to the broker xml conf
```
<authorizationPlugin>
    <map>
        <cachedLDAPAuthorizationMap/>
    </map>
</authorizationPlugin>
```
For the OpenLDAP case, you should define more parameters
```
<authorizationPlugin>
    <map>
        <cachedLDAPAuthorizationMap
            connectionURL="ldap://localhost:389"
            connectionUsername="cn=admin,dc=activemq,dc=apache,dc=org"
            connectionPassword="sunflower"
            queueSearchBase="ou=Queue,ou=Destination,ou=ActiveMQ,dc=activemq,dc=apache,dc=org"
            topicSearchBase="ou=Topic,ou=Destination,ou=ActiveMQ,dc=activemq,dc=apache,dc=org"
            tempSearchBase="ou=Temp,ou=Destination,ou=ActiveMQ,dc=activemq,dc=apache,dc=org"
            refreshInterval="300000"
            legacyGroupMapping="false"
        />
    </map>
</authorizationPlugin>
```
Full examples of configurations for [Apache DS](https://github.com/apache/activemq/tree/main/activemq-unit-tests/src/test/resources/org/apache/activemq/security/activemq-apacheds.xml) and [OpenLDAP](https://github.com/apache/activemq/tree/main/activemq-unit-tests/src/test/resources/org/apache/activemq/security/activemq-openldap.xml)

The list of all properties for `cachedLDAPAuthorizationMap`

property|default value|description|version
---|---|---
connectionURL|ldap://localhost:1024|LDAP Server connection address. This can either be a single URL or a space-separated list of URLs.|
connectionUsername|uid=admin,ou=system|Dn to be used for connecting to the server|
connectionPassword|secret|Password to be used for connecting to the server|
connectionProtocol|s|Connection protocol to be used for connecting to the server|
authentication|simple|Authentication method to be used when connecting to the server|
queueSearchBase|ou=Queue,ou=Destination,ou=ActiveMQ,ou=system|Base dn of queue related entries|5.7 and later
topicSearchBase|ou=Topic,ou=Destination,ou=ActiveMQ,ou=system|Base dn of topic related entries|5.7 and later
tempSearchBase|ou=Temp,ou=Destination,ou=ActiveMQ,ou=system|Base dn of temporary destinations related entries|5.7 and later
refreshInterval|-1|Interval (in milliseconds) of pulling changes from the server, -1 means pulling is off, see #Updates for more info|
legacyGroupMapping|true|Should permission group members be configured as CN and not a full DN|5.7 and later

Updates
-------

Many LDAP servers supports so called "persistent search" feature which allows applications to receive changes in LDAP in a "push" manner. By default this plugin assumes that LDAP server supports this feature and will "register" to get live updates.

For servers that doesn't support this yet (like OpenLDAP), we provide "pull" updates. In this case you need to set `refreshInterval` property, which will define the update period for the plugin (so in this case, updates will not be immediately applied)

