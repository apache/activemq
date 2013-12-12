/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.shiro;

import org.apache.activemq.ConfigurationException;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerPluginSupport;
import org.apache.activemq.shiro.authc.AuthenticationFilter;
import org.apache.activemq.shiro.authc.AuthenticationPolicy;
import org.apache.activemq.shiro.authc.DefaultAuthenticationPolicy;
import org.apache.activemq.shiro.authz.AuthorizationFilter;
import org.apache.activemq.shiro.env.IniEnvironment;
import org.apache.activemq.shiro.subject.ConnectionSubjectFactory;
import org.apache.activemq.shiro.subject.DefaultConnectionSubjectFactory;
import org.apache.activemq.shiro.subject.SubjectFilter;
import org.apache.shiro.config.Ini;
import org.apache.shiro.env.Environment;
import org.apache.shiro.mgt.SecurityManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @since 5.10.0
 */
public class ShiroPlugin extends BrokerPluginSupport {

    private static final Logger LOG = LoggerFactory.getLogger(ShiroPlugin.class);

    private volatile boolean enabled = true;

    private Broker broker; //the downstream broker after any/all Shiro-specific broker filters

    private SecurityManager securityManager;
    private Environment environment;
    private IniEnvironment iniEnvironment; //only used if the above environment instance is not explicitly configured

    private SubjectFilter subjectFilter;

    private AuthenticationFilter authenticationFilter;

    private AuthorizationFilter authorizationFilter;

    public ShiroPlugin() {

        //Default if this.environment is not configured. See the ensureEnvironment() method below.
        iniEnvironment = new IniEnvironment();

        authorizationFilter = new AuthorizationFilter();

        // we want to share one AuthenticationPolicy instance across both the AuthenticationFilter and the
        // ConnectionSubjectFactory:
        AuthenticationPolicy authcPolicy = new DefaultAuthenticationPolicy();

        authenticationFilter = new AuthenticationFilter();
        authenticationFilter.setAuthenticationPolicy(authcPolicy);
        authenticationFilter.setNext(authorizationFilter);

        subjectFilter = new SubjectFilter();
        DefaultConnectionSubjectFactory subjectFactory = new DefaultConnectionSubjectFactory();
        subjectFactory.setAuthenticationPolicy(authcPolicy);
        subjectFilter.setConnectionSubjectFactory(subjectFactory);
        subjectFilter.setNext(authenticationFilter);
    }

    public SubjectFilter getSubjectFilter() {
        return subjectFilter;
    }

    public void setSubjectFilter(SubjectFilter subjectFilter) {
        this.subjectFilter = subjectFilter;
        this.subjectFilter.setNext(this.authenticationFilter);
    }

    public AuthenticationFilter getAuthenticationFilter() {
        return authenticationFilter;
    }

    public void setAuthenticationFilter(AuthenticationFilter authenticationFilter) {
        this.authenticationFilter = authenticationFilter;
        this.authenticationFilter.setNext(this.authorizationFilter);
        this.subjectFilter.setNext(authenticationFilter);
    }

    public AuthorizationFilter getAuthorizationFilter() {
        return authorizationFilter;
    }

    public void setAuthorizationFilter(AuthorizationFilter authorizationFilter) {
        this.authorizationFilter = authorizationFilter;
        this.authorizationFilter.setNext(this.broker);
        this.authenticationFilter.setNext(authorizationFilter);
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
        if (isInstalled()) {
            //we're running, so apply the changes now:
            applyEnabled(enabled);
        }
    }

    public boolean isEnabled() {
        if (isInstalled()) {
            return getNext() == this.subjectFilter;
        }
        return enabled;
    }

    private void applyEnabled(boolean enabled) {
        if (enabled) {
            //ensure the SubjectFilter and downstream filters are used:
            super.setNext(this.subjectFilter);
        } else {
            //Shiro is not enabled, restore the original downstream broker:
            super.setNext(this.broker);
        }
    }

    public Environment getEnvironment() {
        return environment;
    }

    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    public SecurityManager getSecurityManager() {
        return securityManager;
    }

    public void setSecurityManager(SecurityManager securityManager) {
        this.securityManager = securityManager;
    }

    public void setIni(Ini ini) {
        this.iniEnvironment.setIni(ini);
    }

    public void setIniConfig(String iniConfig) {
        this.iniEnvironment.setIniConfig(iniConfig);
    }

    public void setIniResourcePath(String resourcePath) {
        this.iniEnvironment.setIniResourcePath(resourcePath);
    }

    // ===============================================================
    // Authentication Configuration
    // ===============================================================
    public void setAuthenticationEnabled(boolean authenticationEnabled) {
        this.authenticationFilter.setEnabled(authenticationEnabled);
    }

    public boolean isAuthenticationEnabled() {
        return this.authenticationFilter.isEnabled();
    }

    public AuthenticationPolicy getAuthenticationPolicy() {
        return authenticationFilter.getAuthenticationPolicy();
    }

    public void setAuthenticationPolicy(AuthenticationPolicy authenticationPolicy) {
        authenticationFilter.setAuthenticationPolicy(authenticationPolicy);
        //also set it on the ConnectionSubjectFactory:
        ConnectionSubjectFactory factory = subjectFilter.getConnectionSubjectFactory();
        if (factory instanceof DefaultConnectionSubjectFactory) {
            ((DefaultConnectionSubjectFactory) factory).setAuthenticationPolicy(authenticationPolicy);
        }
    }

    // ===============================================================
    // Authorization Configuration
    // ===============================================================
    public void setAuthorizationEnabled(boolean authorizationEnabled) {
        this.authorizationFilter.setEnabled(authorizationEnabled);
    }

    public boolean isAuthorizationEnabled() {
        return this.authorizationFilter.isEnabled();
    }

    private Environment ensureEnvironment() throws ConfigurationException {
        if (this.environment != null) {
            return this.environment;
        }

        //this.environment is null - set it:
        if (this.securityManager != null) {
            this.environment = new Environment() {
                @Override
                public SecurityManager getSecurityManager() {
                    return ShiroPlugin.this.securityManager;
                }
            };
            return this.environment;
        }

        this.iniEnvironment.init(); //will automatically catch any config errors and throw.

        this.environment = iniEnvironment;

        return this.iniEnvironment;
    }

    @Override
    public Broker installPlugin(Broker broker) throws Exception {

        Environment environment = ensureEnvironment();

        this.authorizationFilter.setEnvironment(environment);
        this.authenticationFilter.setEnvironment(environment);
        this.subjectFilter.setEnvironment(environment);

        this.broker = broker;
        this.authorizationFilter.setNext(broker);
        this.authenticationFilter.setNext(this.authorizationFilter);
        this.subjectFilter.setNext(this.authenticationFilter);

        Broker next = this.subjectFilter;
        if (!this.enabled) {
            //not enabled at startup - default to the original broker:
            next = broker;
        }

        setNext(next);
        return this;
    }

    private boolean isInstalled() {
        return getNext() != null;
    }
}
