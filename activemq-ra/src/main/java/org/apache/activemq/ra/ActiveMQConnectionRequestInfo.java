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
package org.apache.activemq.ra;

import java.io.Serializable;

import javax.resource.spi.ConnectionRequestInfo;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.ActiveMQSslConnectionFactory;
import org.apache.activemq.RedeliveryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Must override equals and hashCode (JCA spec 16.4)
 */
public class ActiveMQConnectionRequestInfo implements ConnectionRequestInfo, Serializable, Cloneable {

    private static final long serialVersionUID = -5754338187296859149L;
    protected Logger log = LoggerFactory.getLogger(getClass());

    private String userName;
    private String password;
    private String serverUrl;
    private String clientid;
    private Boolean useInboundSession;
    private RedeliveryPolicy redeliveryPolicy;
    private ActiveMQPrefetchPolicy prefetchPolicy;
    private Boolean useSessionArgs;
    private String trustStore;
    private String trustStorePassword;
    private String keyStore;
    private String keyStorePassword;
    private String keyStoreKeyPassword;

    public ActiveMQConnectionRequestInfo copy() {
        try {
            ActiveMQConnectionRequestInfo answer = (ActiveMQConnectionRequestInfo) clone();
            if (redeliveryPolicy != null) {
                answer.redeliveryPolicy = redeliveryPolicy.copy();
            }
            return answer;
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException("Could not clone: " + e, e);
        }
    }

    /**
     * Returns true if this object will configure an ActiveMQConnectionFactory
     * in any way
     */
    public boolean isConnectionFactoryConfigured() {
        return serverUrl != null || clientid != null || redeliveryPolicy != null || prefetchPolicy != null;
    }

    /**
     * Configures the given connection factory
     */
    public void configure(ActiveMQConnectionFactory factory, MessageActivationSpec activationSpec) {
        if (serverUrl != null) {
            factory.setBrokerURL(serverUrl);
        }
        if (clientid != null) {
            factory.setClientID(clientid);
        }
        if (redeliveryPolicy != null) {
            factory.setRedeliveryPolicy(redeliveryPolicy);
        }
        if (prefetchPolicy != null) {
            factory.setPrefetchPolicy(prefetchPolicy);
        }
        if (factory instanceof ActiveMQSslConnectionFactory) {
            String trustStore = defaultValue(activationSpec == null ? null : activationSpec.getTrustStore(), getTrustStore());
            String trustStorePassword = defaultValue(activationSpec == null ? null : activationSpec.getTrustStorePassword(), getTrustStorePassword());
            String keyStore = defaultValue(activationSpec == null ? null : activationSpec.getKeyStore(), getKeyStore());
            String keyStorePassword = defaultValue(activationSpec == null ? null : activationSpec.getKeyStorePassword(), getKeyStorePassword());
            String keyStoreKeyPassword = defaultValue(activationSpec == null ? null : activationSpec.getKeyStoreKeyPassword(), getKeyStoreKeyPassword());
            ActiveMQSslConnectionFactory sslFactory = (ActiveMQSslConnectionFactory) factory;
            if (trustStore != null) {
                try {
                    sslFactory.setTrustStore(trustStore);
                } catch (Exception e) {
                    log.warn("Unable to set TrustStore", e);
                }
            }
            if (trustStorePassword != null) {
                sslFactory.setTrustStorePassword(trustStorePassword);
            }
            if (keyStore != null) {
                try {
                    sslFactory.setKeyStore(keyStore);
                } catch (Exception e) {
                    log.warn("Unable to set KeyStore", e);
                }
            }
            if (keyStorePassword != null) {
                sslFactory.setKeyStorePassword(keyStorePassword);
            }
            if (keyStoreKeyPassword != null) {
                sslFactory.setKeyStoreKeyPassword(keyStoreKeyPassword);
            }
        }
    }

    /**
     * @see javax.resource.spi.ConnectionRequestInfo#hashCode()
     */
    @Override
    public int hashCode() {
        int rc = 0;
        if (useInboundSession != null) {
            rc ^= useInboundSession.hashCode();
        }
        if (useSessionArgs != null) {
            rc ^= useSessionArgs.hashCode();
        }
        if (serverUrl != null) {
            rc ^= serverUrl.hashCode();
        }
        return rc;
    }

    /**
     * @see javax.resource.spi.ConnectionRequestInfo#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (!getClass().equals(o.getClass())) {
            return false;
        }
        ActiveMQConnectionRequestInfo i = (ActiveMQConnectionRequestInfo) o;
        if (notEqual(serverUrl, i.serverUrl)) {
            return false;
        }
        if (notEqual(useInboundSession, i.useInboundSession)) {
            return false;
        }
        if (notEqual(useSessionArgs, i.useSessionArgs)) {
            return false;
        }
        return true;
    }

    private boolean notEqual(Object o1, Object o2) {
        return (o1 == null ^ o2 == null) || (o1 != null && !o1.equals(o2));
    }

    /**
     * @return Returns the url.
     */
    public String getServerUrl() {
        return serverUrl;
    }

    /**
     * @param url
     *        The url to set.
     */
    public void setServerUrl(String url) {
        this.serverUrl = url;
    }

    /**
     * @return Returns the password.
     */
    public String getPassword() {
        return password;
    }

    /**
     * @param password
     *        The password to set.
     */
    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * @return Returns the userid.
     */
    public String getUserName() {
        return userName;
    }

    /**
     * @param userid
     *        The userid to set.
     */
    public void setUserName(String userid) {
        this.userName = userid;
    }

    /**
     * @return Returns the clientid.
     */
    public String getClientid() {
        return clientid;
    }

    /**
     * @param clientid
     *        The clientid to set.
     */
    public void setClientid(String clientid) {
        this.clientid = clientid;
    }

    public String getTrustStore() {
        return trustStore;
    }

    public void setTrustStore(String trustStore) {
        this.trustStore = trustStore;
    }

    public String getTrustStorePassword() {
        return trustStorePassword;
    }

    public void setTrustStorePassword(String trustStorePassword) {
        this.trustStorePassword = trustStorePassword;
    }

    public String getKeyStore() {
        return keyStore;
    }

    public void setKeyStore(String keyStore) {
        this.keyStore = keyStore;
    }

    public String getKeyStorePassword() {
        return keyStorePassword;
    }

    public void setKeyStorePassword(String keyStorePassword) {
        this.keyStorePassword = keyStorePassword;
    }

    public String getKeyStoreKeyPassword() {
        return keyStoreKeyPassword;
    }

    public void setKeyStoreKeyPassword(String keyStoreKeyPassword) {
        this.keyStoreKeyPassword = keyStoreKeyPassword;
    }

    @Override
    public String toString() {
        return new StringBuffer("ActiveMQConnectionRequestInfo{ userName = '").append(userName).append("' ").append(", serverUrl = '").append(serverUrl)
            .append("' ").append(", clientid = '").append(clientid).append("' ")
            .append(", useSessionArgs = '").append(useSessionArgs).append("' ").append(", useInboundSession = '").append(useInboundSession).append("'  }")
            .toString();
    }

    public Boolean getUseInboundSession() {
        return useInboundSession;
    }

    public void setUseInboundSession(Boolean useInboundSession) {
        this.useInboundSession = useInboundSession;
    }

    public boolean isUseInboundSessionEnabled() {
        return useInboundSession != null && useInboundSession.booleanValue();
    }

    public Double getRedeliveryBackOffMultiplier() {
        return Double.valueOf(redeliveryPolicy().getBackOffMultiplier());
    }

    public Long getInitialRedeliveryDelay() {
        return Long.valueOf(redeliveryPolicy().getInitialRedeliveryDelay());
    }

    public Long getMaximumRedeliveryDelay() {
        return Long.valueOf(redeliveryPolicy().getMaximumRedeliveryDelay());
    }

    public Integer getMaximumRedeliveries() {
        return Integer.valueOf(redeliveryPolicy().getMaximumRedeliveries());
    }

    public Boolean getRedeliveryUseExponentialBackOff() {
        return Boolean.valueOf(redeliveryPolicy().isUseExponentialBackOff());
    }

    public void setRedeliveryBackOffMultiplier(Double value) {
        if (value != null) {
            redeliveryPolicy().setBackOffMultiplier(value);
        }
    }

    public void setInitialRedeliveryDelay(Long value) {
        if (value != null) {
            redeliveryPolicy().setInitialRedeliveryDelay(value.longValue());
        }
    }

    public void setMaximumRedeliveryDelay(Long value) {
        if (value != null) {
            redeliveryPolicy().setMaximumRedeliveryDelay(value.longValue());
        }
    }

    public void setMaximumRedeliveries(Integer value) {
        if (value != null) {
            redeliveryPolicy().setMaximumRedeliveries(value.intValue());
        }
    }

    public void setRedeliveryUseExponentialBackOff(Boolean value) {
        if (value != null) {
            redeliveryPolicy().setUseExponentialBackOff(value.booleanValue());
        }
    }

    public Integer getDurableTopicPrefetch() {
        return Integer.valueOf(prefetchPolicy().getDurableTopicPrefetch());
    }

    public Integer getOptimizeDurableTopicPrefetch() {
        return Integer.valueOf(prefetchPolicy().getOptimizeDurableTopicPrefetch());
    }

    @Deprecated
    public Integer getInputStreamPrefetch() {
        return 0;
    }

    public Integer getQueueBrowserPrefetch() {
        return Integer.valueOf(prefetchPolicy().getQueueBrowserPrefetch());
    }

    public Integer getQueuePrefetch() {
        return Integer.valueOf(prefetchPolicy().getQueuePrefetch());
    }

    public Integer getTopicPrefetch() {
        return Integer.valueOf(prefetchPolicy().getTopicPrefetch());
    }

    public void setAllPrefetchValues(Integer i) {
        if (i != null) {
            prefetchPolicy().setAll(i.intValue());
        }
    }

    public void setDurableTopicPrefetch(Integer durableTopicPrefetch) {
        if (durableTopicPrefetch != null) {
            prefetchPolicy().setDurableTopicPrefetch(durableTopicPrefetch.intValue());
        }
    }

    public void setOptimizeDurableTopicPrefetch(Integer optimizeDurableTopicPrefetch) {
        if (optimizeDurableTopicPrefetch != null) {
            prefetchPolicy().setOptimizeDurableTopicPrefetch(optimizeDurableTopicPrefetch.intValue());
        }
    }

    public void setQueueBrowserPrefetch(Integer queueBrowserPrefetch) {
        if (queueBrowserPrefetch != null) {
            prefetchPolicy().setQueueBrowserPrefetch(queueBrowserPrefetch.intValue());
        }
    }

    public void setQueuePrefetch(Integer queuePrefetch) {
        if (queuePrefetch != null) {
            prefetchPolicy().setQueuePrefetch(queuePrefetch.intValue());
        }
    }

    public void setTopicPrefetch(Integer topicPrefetch) {
        if (topicPrefetch != null) {
            prefetchPolicy().setTopicPrefetch(topicPrefetch.intValue());
        }
    }

    /**
     * Returns the redelivery policy; not using bean properties to avoid
     * breaking compatibility with JCA configuration in J2EE
     */
    public RedeliveryPolicy redeliveryPolicy() {
        if (redeliveryPolicy == null) {
            redeliveryPolicy = new RedeliveryPolicy();
        }
        return redeliveryPolicy;
    }

    /**
     * Returns the prefetch policy; not using bean properties to avoid breaking
     * compatibility with JCA configuration in J2EE
     */
    public ActiveMQPrefetchPolicy prefetchPolicy() {
        if (prefetchPolicy == null) {
            prefetchPolicy = new ActiveMQPrefetchPolicy();
        }
        return prefetchPolicy;
    }

    public boolean isUseSessionArgs() {
        return useSessionArgs != null ? useSessionArgs.booleanValue() : false;
    }

    public Boolean getUseSessionArgs() {
        return useSessionArgs;
    }

    public void setUseSessionArgs(Boolean useSessionArgs) {
        this.useSessionArgs = useSessionArgs;
    }

    protected String defaultValue(String value, String defaultValue) {
        if (value != null) {
            return value;
        }
        return defaultValue;
    }
}
