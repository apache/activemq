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
import org.apache.activemq.RedeliveryPolicy;

/**
 *  Must override equals and hashCode (JCA spec 16.4)
 */
public class ActiveMQConnectionRequestInfo implements ConnectionRequestInfo, Serializable, Cloneable {

    private static final long serialVersionUID = -5754338187296859149L;

    private String userName;
    private String password;
    private String serverUrl;
    private String clientid;
    private Boolean useInboundSession;
    private RedeliveryPolicy redeliveryPolicy;
    private ActiveMQPrefetchPolicy prefetchPolicy;

    public ActiveMQConnectionRequestInfo copy() {
        try {
            ActiveMQConnectionRequestInfo answer = (ActiveMQConnectionRequestInfo)clone();
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
    public void configure(ActiveMQConnectionFactory factory) {
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
    }

    /**
     * @see javax.resource.spi.ConnectionRequestInfo#hashCode()
     */
    public int hashCode() {
        int rc = 0;
        if (useInboundSession != null) {
            rc ^= useInboundSession.hashCode();
        }
        if (serverUrl != null) {
            rc ^= serverUrl.hashCode();
        }
        return rc;
    }

    /**
     * @see javax.resource.spi.ConnectionRequestInfo#equals(java.lang.Object)
     */
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (!getClass().equals(o.getClass())) {
            return false;
        }
        ActiveMQConnectionRequestInfo i = (ActiveMQConnectionRequestInfo)o;
        if (notEqual(serverUrl, i.serverUrl)) {
            return false;
        }
        if (notEqual(useInboundSession, i.useInboundSession)) {
            return false;
        }
        return true;
    }

    /**
     * @param i
     * @return
     */
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
     * @param url The url to set.
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
     * @param password The password to set.
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
     * @param userid The userid to set.
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
     * @param clientid The clientid to set.
     */
    public void setClientid(String clientid) {
        this.clientid = clientid;
    }

    @Override
    public String toString() {
        return new StringBuffer("ActiveMQConnectionRequestInfo{ userName = '").append(userName).append("' ")
                .append(", serverUrl = '").append(serverUrl).append("' ")
                .append(", clientid = '").append(clientid).append("' ")
                .append(", userName = '").append(userName).append("' ")
                .append(", useInboundSession = '").append(useInboundSession).append("'  }")
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

    public Integer getInputStreamPrefetch() {
        return Integer.valueOf(prefetchPolicy().getInputStreamPrefetch());
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

    public void setInputStreamPrefetch(Integer inputStreamPrefetch) {
        if (inputStreamPrefetch != null) {
            prefetchPolicy().setInputStreamPrefetch(inputStreamPrefetch.intValue());
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
}
