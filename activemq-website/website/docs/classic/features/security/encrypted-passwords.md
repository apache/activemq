---
title: "Encrypted passwords"
description: "Encrypt passwords for ActiveMQ Classic config files with the encrypt command, ENC() values in credentials-enc.properties and a Jasypt property placeholder."
slug: /encrypted-passwords
---


As of ActiveMQ 5.4.1 you can encrypt your passwords and safely store them in configuration files. To encrypt the password, you can use the newly added `encrypt` command like:
```
$ bin/activemq encrypt --password activemq --input mypassword
...
Encrypted text: eeWjNyX6FY8Fjp3E+F6qTytV11bZItDp
```
Where the password you want to encrypt is passed with the `input` argument, while the `password` argument is a secret used by the encryptor. In a similar fashion you can test-out your passwords like:
```
$ bin/activemq decrypt  --password activemq --input eeWjNyX6FY8Fjp3E+F6qTytV11bZItDp
...
Decrypted text: mypassword
```
**Note:** It is recommended that you use only alphanumeric characters for the password. Special characters, such as `$/^&`, are not supported.

As of the 5.16.0 release, support has been added to specify an algorithm
parameter to the "encrypt" and "decrypt" commands. By default, the algorithm
that is used is "PBEWithMD5AndDES". To use a more modern encryption algorithm
you can specify:
```
$ bin/activemq encrypt --password activemq --input mypassword --algorithm PBEWITHHMACSHA256ANDAES_256
...
Encrypted text: h/cWj/ZZelMt3Y7NSzUG2vHYSnfWK561qjNg9Ywyr9yT72ru7pR4IEUnHLIdLSOb
```

The next step is to add the password to the appropriate configuration file, `$ACTIVEMQ_HOME/conf/credentials-enc.properties` by default.
```
activemq.username=system
activemq.password=ENC(mYRkg+4Q4hua1kvpCCI2hg==)
guest.password=ENC(Cf3Jf3tM+UrSOoaKU50od5CuBa8rxjoL)
...
jdbc.password=ENC(eeWjNyX6FY8Fjp3E+F6qTytV11bZItDp)
```
Note that we used `ENC()` to wrap our encrypted passwords. You can mix plain and encrypted passwords in your properties files, so encrypted ones must be wrapped this way.

Finally, you need to instruct your property loader to decrypt variables when it loads properties to the memory. Instead of standard property loader we'll use the special one (see `\$ACTIVEMQ_HOME/conf/activemq-security.xml`) to achieve this.
```
<bean id="environmentVariablesConfiguration" class="org.jasypt.encryption.pbe.config.EnvironmentStringPBEConfig">
  <property name="algorithm" value="PBEWithMD5AndDES" />
  <property name="passwordEnvName" value="ACTIVEMQ\_ENCRYPTION\_PASSWORD" />
</bean>
                                                                     
<bean id="configurationEncryptor" class="org.jasypt.encryption.pbe.StandardPBEStringEncryptor">
  <property name="config" ref="environmentVariablesConfiguration" />
</bean> 
    
<bean id="propertyConfigurer" class="org.jasypt.spring31.properties.EncryptablePropertyPlaceholderConfigurer"> 
  <constructor-arg ref="configurationEncryptor" /> 
  <property name="location" value="file:${activemq.base}/conf/credentials-enc.properties"/> 
</bean>
```
With this configuration ActiveMQ will try to load your encryptor password from the `ACTIVEMQ_ENCRYPTION_PASSWORD` environment variable and then use it to decrypt passwords from `credential-enc.properties` file.

Alternative is to use a simple variant and store encryptor password in the xml file, like this
```
<bean id="configurationEncryptor" class="org.jasypt.encryption.pbe.StandardPBEStringEncryptor">
  <property name="algorithm" value="PBEWithMD5AndDES"/>
  <property name="password" value="activemq"/>
</bean>
```
but with that you'll lose the secrecy of the encryptor's secret. You may also consult [http://www.jasypt.org/advancedCommunity/FAQ/configuration.md](http://www.jasypt.org/advancedCommunity/FAQ/configuration) for more ideas on how to configure Jasypt.

Finally, we can use properties like we'd normally do
```
<simpleAuthenticationPlugin>
  <users>
    <authenticationUser username="system" password="${activemq.password}"
      groups="users,admins"/>
    <authenticationUser username="user" password="${guest.password}"
      groups="users"/>
    <authenticationUser username="guest" password="${guest.password}" groups="guests"/>
  </users>
</simpleAuthenticationPlugin>
```
or
```
<bean id="mysql-ds" class="org.apache.commons.dbcp.BasicDataSource" destroy-method="close">
  <property name="driverClassName" value="com.mysql.jdbc.Driver"/>
  <property name="url" value="jdbc:mysql://localhost/activemq?relaxAutoCommit=true"/>
  <property name="username" value="activemq"/>
  <property name="password" value="${jdbc.password}"/>
  <property name="maxActive" value="200"/>
  <property name="poolPreparedStatements" value="true"/>
</bean>
```
If you want to run the broker with this configuration, you need to do the following:

*   Set environment variable:
    ```
    $ export ACTIVEMQ\_ENCRYPTION\_PASSWORD=activemq
    ```
*   Start the broker:
    ```
    $ bin/activemq start xbean:conf/activemq-security.xml
    ```
*   Unset the environment variable:
    ```
    $ unset ACTIVEMQ\_ENCRYPTION\_PASSWORD
    ```

In this way your encryptor secret is never saved on your system and your encrypted passwords are safely stored in the configuration files.
