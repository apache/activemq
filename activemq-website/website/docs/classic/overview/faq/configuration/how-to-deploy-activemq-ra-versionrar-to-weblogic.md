---
title: "How to deploy activemq-ra-version.rar to weblogic"
slug: /how-to-deploy-activemq-ra-versionrar-to-weblogic
---

### This is a guide on how to deploy ActiveMQ's resouce adapter to weblogic 9.1.

1.  Create a new domain in weblogic using the configuration wizard (Start menu BEA Products -> Tools -> configuration Wizard ).
2.  Add the jar dependencies (these are the jars inside the rar file..for some reason weblogic is not loading these from the rar file)  in the classpath . One way to do this is by dropping the jars in the lib directory of the domain (`<%BEA\_HOME%>`\user_projects\domains\\`<%DOMAIN_NAME%>`\lib). 
3.  Add the BrokerXmlConfig file called "broker-config.xml" in the classpath. This can be found in the rar file "activemq-rar-*.rar". Otherwise, modify ra.xml by providing an absolute path to "broker-config.xml", see below.
    ```
    <config-property>
      <description>
        Sets the XML configuration file used to configure the ActiveMQ broker via
        Spring if using embedded mode.
    
        BrokerXmlConfig is the filename which is assumed to be on the classpath unless
        a URL is specified. So a value of foo/bar.xml would be assumed to be on the
        classpath whereas file:dir/file.xml would use the file system.
        Any valid URL string is supported.
      </description>
      <config-property-name>BrokerXmlConfig</config-property-name>
      <config-property-type>java.lang.String</config-property-type>
      <config-property-value>xbean:file:C:\\broker-config.xml</config-property-value>
    </config-property>
    ```
2.  Start the domain's weblogic server (BEA Products -> User Projects -> `<domain_name>` -> Start Admin Server for Weblogic Server Domain).
3.  Start the domain's admin console (BEA Products -> User Projects -> `<domain_name>` -> Admin Server Console) and enter the username and password.
4.  On the main menu, click "Deployments" under "Your Deployed Resources"
5.  On the deployment table, click "install" button. If the button is disabled, then click "Lock & Edit" to enable it.
6.  Browse for the activemq-ra-*.rar file then click next (for testing purpose, choose the default onwards).
7.  Click the "activate changes" button to check if the resource adapter is properly deployed.

