# \[ActiveMQ Classic\] Support hashed credential in PropertiesLoginModule

## Problem statement

ActiveMQ mainline uses Jetty 11 (ActiveMQ 5.18.x uses Jetty 9\) for serving its web console. Because of the version upgrade in Jetty, ActiveMQ 6.x is able to configure a JAAS plugin for web console authentication and it is the default for ActiveMQ 6.x users. Currently, that default plugin `PropertiesLoginModule` (in activemq-jaas package) only supports storing credentials in plain text. This design doc explores ways to support storing credentials that are securely hashed in the default plugin provided by ActiveMQ Classic

## Rationale

Even tho users, in theory, can implement their own JAAS plugin (which can support whatever security requirement they want) and inject it in jetty.xml for authentication, having ActiveMQ Classic default `PropertiesLoginModule` to support more secure feature like storing securely hashed credentials can benefit users who don’t have the security expertise and make ActiveMQ Classic more secure by default. Furthermore, ActiveMQ Artemis has taken a similar approach and supports storing hashed credentials.

## Configuration options

**Option 1\. Align with ActiveMQ Artemis**  
We can align the configuration options of properties files with the ones in [ActiveMQ Artemis](https://activemq.apache.org/components/artemis/documentation/2.5.0/security.html) to provide consistent user experience. I.E user can configure the `org.apache.activemq.jaas.PropertiesLoginModule` like this

In `login.config`
```shell
activemq {  
    org.apache.activemq.jaas.PropertiesLoginModule required  
    org.apache.activemq.jaas.properties.user="users.properties"  
    org.apache.activemq.jaas.properties.group="groups.properties"  
    org.apache.activemq.jaas.properties.password.codec="\<codec\>";  
}
```

In `users.properties`  
```shell
admin=ENC(\<hashed credential\>)
```
Pros:

- Consistent user experience with ActiveMQ Artemis to reduce friction of adoption
- More extensible configuration for the codec

Cons:

- The configuration is more complex than its alternative (just specify the hashing algorithm and not the class of its codec) to users who are not familiar with ActiveMQ.

**Option 2\. Encode the algorithm into the credential \[Recommended\]**  
Alternatively, we can provide a simpler configuration option (similar to Jetty `HashLoginService`) and user only needs to change one file

In `login.config` (no change)  
```shell
activemq {  
    org.apache.activemq.jaas.PropertiesLoginModule required  
    org.apache.activemq.jaas.properties.user="users.properties"  
    org.apache.activemq.jaas.properties.group="groups.properties";;  
}
```

In `users.properties` (for example)
```shell
# \<hashing algorithm\>:\<password hash in base64\>  
admin=SHA-384:KTDAO6m5/4zmgtBKtqtLBnDYnv92LBcDQpiq1JCKBhBypbeyIoqDLxrei35hGY6j
```

Pros:

- Easy to configure with sensible default

Cons:

- Less extensible and it doesn’t allow user to specify its own implementation of password hashing/encryption class

## Module implementation

**Option 1\. Align with ActiveMQ Artemis**  
We can mimic the implementation that exists currently in ActiveMQ Artemis. Introduce a few classes that load, and handle password hashing and comparison/validation.

Pros:

- It uses class loader to let user loads a codec it specifies

Cons:

- More code to write, depending on the use case, it might be over-engineering

**Option 2\. Use jasypt ConfigurablePasswordEncryptor \[Recommended\]**  
ActiveMQ already uses `jasypt` in its activemq-jaas package (Note, Artemis doesn’t take jasypt as a dependency). It uses `StandardPBEStringEncryptor` for example to implement encryption. The idea is to use [`ConfigurablePasswordEncryptor`](http://www.jasypt.org/api/jasypt/1.9.3/org/jasypt/util/password/ConfigurablePasswordEncryptor.html) and do something like
```java
ConfigurablePasswordEncryptor passwordEncryptor = new ConfigurablePasswordEncryptor();  
passwordEncryptor.setAlgorithm(algorithm);
```

To handle password hashing and validation

Pros:

- Simple to implement and doesn’t require taking on new dependencies

Cons:

- Not as extensible as the first option.

## Deliverables

This work will be broken down into 2 PRs:

1. Implement the hashing logic as proposed.
2. Implement CLI tool to let users create hashed credentials.

## Future ideas to further harden the security of web console

1. Even though the credential is now securely hashed, the web console is still susceptible to brute force attack. To harden the security further, we can provide override for the Jetty `authenticator` to implement lock out mechanism after a certain number of failed attempts. 

