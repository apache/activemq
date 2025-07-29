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

package org.apache.activemq.security;

import org.apache.activemq.broker.Broker;

/**
 * A JAAS based SSL certificate authentication plugin.
 *
 * @org.apache.xbean.XBean description="Provides a JAAS based authentication plugin
 * which uses properties for non-SSL and certificates for SSL"
 * 
 * 
 */
public class JaasDualAuthenticationPlugin extends JaasAuthenticationPlugin {
    private String sslConfiguration = "activemq-ssl-domain";
    private boolean certificateRequired = false;

    public Broker installPlugin(Broker broker) {
        initialiseJaas();
        return new JaasDualAuthenticationBroker(broker, configuration, sslConfiguration, certificateRequired);
    }

    // Properties
    // -------------------------------------------------------------------------

    /**
     * Set the JAAS SSL configuration domain
     */
    public void setSslConfiguration(String sslConfiguration) {
        this.sslConfiguration = sslConfiguration;
    }

    public String getSslConfiguration() {
        return sslConfiguration;
    }

    public void setCertificateRequired(boolean certificateRequired) {
        this.certificateRequired = certificateRequired;
    }

    public boolean isCertificateRequired() {
        return this.certificateRequired;
    }
}
