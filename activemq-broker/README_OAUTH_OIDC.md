# OAuth and OIDC Implementation for ActiveMQ

## Overview
This document outlines the plan to implement OAuth and OIDC authentication for ActiveMQ. The implementation will be done in a maxiumum of four stages:
1. Initial declaration of changes and setup.
2. Implementation of OAuth and OIDC methods.
3. Adding unit and integration tests.
4. Implementing logging for OAuth and OIDC operations.

## Plugin configuration in the activemq.xml file

    <plugins>
        <bean id="oidcAuthenticationPlugin" class="org.apache.activemq.security.OIDCAuthenticationPlugin">
            <property name="clientId" value="YOUR_COMPANY_CLIENT_ID"/>
            <property name="clientSecret" value="YOUR_COMPANY_CLIENT_SECRET"/>
            <property name="oidcServerUrl" value="https://oidc-server.com"/>
            <property name="oidcIssuer" value="https://oidc-issuer.com"/>
        </bean>
    </plugins>