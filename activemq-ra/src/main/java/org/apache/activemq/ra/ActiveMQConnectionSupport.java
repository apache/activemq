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

import javax.jms.JMSException;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSslConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class providing support for creating physical connections to an
 * ActiveMQ instance.
 *
 *
 */
public class ActiveMQConnectionSupport {

    private ActiveMQConnectionRequestInfo info = new ActiveMQConnectionRequestInfo();
    protected Logger log = LoggerFactory.getLogger(getClass());

    /**
     * Creates a factory for obtaining physical connections to an Active MQ
     * broker. The factory is configured with the given configuration
     * information.
     *
     * @param connectionRequestInfo
     *        the configuration request information
     * @param activationSpec
     * @return the connection factory
     * @throws java.lang.IllegalArgumentException
     *         if the server URL given in the configuration information is not a
     *         valid URL
     */
    protected ActiveMQConnectionFactory createConnectionFactory(ActiveMQConnectionRequestInfo connectionRequestInfo, MessageActivationSpec activationSpec) {
        // ActiveMQSslConnectionFactory defaults to TCP anyway
        ActiveMQConnectionFactory factory = new ActiveMQSslConnectionFactory();
        connectionRequestInfo.configure(factory, activationSpec);
        return factory;
    }

    /**
     * Creates a new physical connection to an Active MQ broker identified by
     * given connection request information.
     *
     * @param connectionRequestInfo
     *        the connection request information identifying the broker and any
     *        required connection parameters, e.g. username/password
     * @return the physical connection
     * @throws JMSException
     *         if the connection could not be established
     */
    public ActiveMQConnection makeConnection(ActiveMQConnectionRequestInfo connectionRequestInfo) throws JMSException {
        return makeConnection(connectionRequestInfo, createConnectionFactory(connectionRequestInfo, null));
    }

    /**
     * Creates a new physical connection to an Active MQ broker using a given
     * connection factory and credentials supplied in connection request
     * information.
     *
     * @param connectionRequestInfo
     *        the connection request information containing the credentials to
     *        use for the connection request
     * @return the physical connection
     * @throws JMSException
     *         if the connection could not be established
     */
    public ActiveMQConnection makeConnection(ActiveMQConnectionRequestInfo connectionRequestInfo, ActiveMQConnectionFactory connectionFactory)
        throws JMSException {
        String userName = connectionRequestInfo.getUserName();
        String password = connectionRequestInfo.getPassword();
        ActiveMQConnection physicalConnection = (ActiveMQConnection) connectionFactory.createConnection(userName, password);

        String clientId = connectionRequestInfo.getClientid();
        if (clientId != null && clientId.length() > 0) {
            physicalConnection.setClientID(clientId);
        }
        return physicalConnection;
    }

    /**
     * Gets the connection request information.
     *
     * @return the connection request information
     */
    public ActiveMQConnectionRequestInfo getInfo() {
        return info;
    }

    /**
     * Sets the connection request information as a whole.
     *
     * @param connectionRequestInfo
     *        the connection request information
     */
    protected void setInfo(ActiveMQConnectionRequestInfo connectionRequestInfo) {
        info = connectionRequestInfo;
        if (log.isDebugEnabled()) {
            log.debug(this + ", setting [info] to: " + info);
        }
    }

    protected boolean notEqual(Object o1, Object o2) {
        return (o1 == null ^ o2 == null) || (o1 != null && !o1.equals(o2));
    }

    protected String emptyToNull(String value) {
        if (value == null || value.length() == 0) {
            return null;
        } else {
            return value;
        }
    }

    protected String defaultValue(String value, String defaultValue) {
        if (value != null) {
            return value;
        }
        return defaultValue;
    }

    // ///////////////////////////////////////////////////////////////////////
    //
    // Java Bean getters and setters for this ResourceAdapter class.
    //
    // ///////////////////////////////////////////////////////////////////////

    /**
     * @return client id
     */
    public String getClientid() {
        return emptyToNull(info.getClientid());
    }

    /**
     * @param clientid
     */
    public void setClientid(String clientid) {
        if (log.isDebugEnabled()) {
            log.debug(this + ", setting [clientid] to: " + clientid);
        }
        info.setClientid(clientid);
    }

    /**
     * @return password
     */
    public String getPassword() {
        return emptyToNull(info.getPassword());
    }

    /**
     * @param password
     */
    public void setPassword(String password) {
        if (log.isDebugEnabled()) {
            log.debug(this + ", setting [password] property");
        }
        info.setPassword(password);
    }

    /**
     * @return server URL
     */
    public String getServerUrl() {
        return info.getServerUrl();
    }

    /**
     * @param url
     */
    public void setServerUrl(String url) {
        if (log.isDebugEnabled()) {
            log.debug(this + ", setting [serverUrl] to: " + url);
        }
        info.setServerUrl(url);
    }

    public String getTrustStore() {
        return info.getTrustStore();
    }

    public void setTrustStore(String trustStore) {
        if (log.isDebugEnabled()) {
            log.debug(this + ", setting [trustStore] to: " + trustStore);
        }
        info.setTrustStore(trustStore);
    }

    public String getTrustStorePassword() {
        return info.getTrustStorePassword();
    }

    public void setTrustStorePassword(String trustStorePassword) {
        if (log.isDebugEnabled()) {
            log.debug(this + ", setting [trustStorePassword] to: " + trustStorePassword);
        }
        info.setTrustStorePassword(trustStorePassword);
    }

    public String getKeyStore() {
        return info.getKeyStore();
    }

    public void setKeyStore(String keyStore) {
        if (log.isDebugEnabled()) {
            log.debug(this + ", setting [keyStore] to: " + keyStore);
        }
        info.setKeyStore(keyStore);
    }

    public String getKeyStorePassword() {
        return info.getKeyStorePassword();
    }

    public void setKeyStorePassword(String keyStorePassword) {
        if (log.isDebugEnabled()) {
            log.debug(this + ", setting [keyStorePassword] to: " + keyStorePassword);
        }
        info.setKeyStorePassword(keyStorePassword);
    }

    public String getKeyStoreKeyPassword() {
        return info.getKeyStoreKeyPassword();
    }

    public void setKeyStoreKeyPassword(String keyStoreKeyPassword) {
        if (log.isDebugEnabled()) {
            log.debug(this + ", setting [keyStoreKeyPassword] to: " + keyStoreKeyPassword);
        }
        info.setKeyStoreKeyPassword(keyStoreKeyPassword);
    }

    /**
     * @return user name
     */
    public String getUserName() {
        return emptyToNull(info.getUserName());
    }

    /**
     * @param userid
     */
    public void setUserName(String userid) {
        if (log.isDebugEnabled()) {
            log.debug("setting [userName] to: " + userid);
        }
        info.setUserName(userid);
    }

    /**
     * @return durable topic prefetch
     */
    public Integer getDurableTopicPrefetch() {
        return info.getDurableTopicPrefetch();
    }

    /**
     * @param optimizeDurableTopicPrefetch
     */
    public void setOptimizeDurableTopicPrefetch(Integer optimizeDurableTopicPrefetch) {
        if (log.isDebugEnabled()) {
            log.debug("setting [optimizeDurableTopicPrefetch] to: " + optimizeDurableTopicPrefetch);
        }
        info.setOptimizeDurableTopicPrefetch(optimizeDurableTopicPrefetch);
    }

    /**
     * @return durable topic prefetch
     */
    public Integer getOptimizeDurableTopicPrefetch() {
        return info.getOptimizeDurableTopicPrefetch();
    }

    /**
     * @param durableTopicPrefetch
     */
    public void setDurableTopicPrefetch(Integer durableTopicPrefetch) {
        if (log.isDebugEnabled()) {
            log.debug("setting [durableTopicPrefetch] to: " + durableTopicPrefetch);
        }
        info.setDurableTopicPrefetch(durableTopicPrefetch);
    }

    /**
     * @return initial redelivery delay
     */
    public Long getInitialRedeliveryDelay() {
        return info.getInitialRedeliveryDelay();
    }

    /**
     * @param value
     */
    public void setInitialRedeliveryDelay(Long value) {
        if (log.isDebugEnabled()) {
            log.debug("setting [initialRedeliveryDelay] to: " + value);
        }
        info.setInitialRedeliveryDelay(value);
    }

    /**
     * @return initial redelivery delay
     */
    public Long getMaximumRedeliveryDelay() {
        return info.getMaximumRedeliveryDelay();
    }

    /**
     * @param value
     */
    public void setMaximumRedeliveryDelay(Long value) {
        if (log.isDebugEnabled()) {
            log.debug("setting [maximumRedeliveryDelay] to: " + value);
        }
        info.setMaximumRedeliveryDelay(value);
    }

    /**
     * @return input stream prefetch
     */
    @Deprecated
    public Integer getInputStreamPrefetch() {
        return 0;
    }

    /**
     * @return maximum redeliveries
     */
    public Integer getMaximumRedeliveries() {
        return info.getMaximumRedeliveries();
    }

    /**
     * @param value
     */
    public void setMaximumRedeliveries(Integer value) {
        if (log.isDebugEnabled()) {
            log.debug("setting [maximumRedeliveries] to: " + value);
        }
        info.setMaximumRedeliveries(value);
    }

    /**
     * @return queue browser prefetch
     */
    public Integer getQueueBrowserPrefetch() {
        return info.getQueueBrowserPrefetch();
    }

    /**
     * @param queueBrowserPrefetch
     */
    public void setQueueBrowserPrefetch(Integer queueBrowserPrefetch) {
        if (log.isDebugEnabled()) {
            log.debug("setting [queueBrowserPrefetch] to: " + queueBrowserPrefetch);
        }
        info.setQueueBrowserPrefetch(queueBrowserPrefetch);
    }

    /**
     * @return queue prefetch
     */
    public Integer getQueuePrefetch() {
        return info.getQueuePrefetch();
    }

    /**
     * @param queuePrefetch
     */
    public void setQueuePrefetch(Integer queuePrefetch) {
        if (log.isDebugEnabled()) {
            log.debug("setting [queuePrefetch] to: " + queuePrefetch);
        }
        info.setQueuePrefetch(queuePrefetch);
    }

    /**
     * @return redelivery backoff multiplier
     */
    public Double getRedeliveryBackOffMultiplier() {
        return info.getRedeliveryBackOffMultiplier();
    }

    /**
     * @param value
     */
    public void setRedeliveryBackOffMultiplier(Double value) {
        if (log.isDebugEnabled()) {
            log.debug("setting [redeliveryBackOffMultiplier] to: " + value);
        }
        info.setRedeliveryBackOffMultiplier(value);
    }

    /**
     * @return redelivery use exponential backoff
     */
    public Boolean getRedeliveryUseExponentialBackOff() {
        return info.getRedeliveryUseExponentialBackOff();
    }

    /**
     * @param value
     */
    public void setRedeliveryUseExponentialBackOff(Boolean value) {
        if (log.isDebugEnabled()) {
            log.debug("setting [redeliveryUseExponentialBackOff] to: " + value);
        }
        info.setRedeliveryUseExponentialBackOff(value);
    }

    /**
     * @return topic prefetch
     */
    public Integer getTopicPrefetch() {
        return info.getTopicPrefetch();
    }

    /**
     * @param topicPrefetch
     */
    public void setTopicPrefetch(Integer topicPrefetch) {
        if (log.isDebugEnabled()) {
            log.debug("setting [topicPrefetch] to: " + topicPrefetch);
        }
        info.setTopicPrefetch(topicPrefetch);
    }

    /**
     * @param i
     */
    public void setAllPrefetchValues(Integer i) {
        info.setAllPrefetchValues(i);
    }

    /**
     * @return use inbound session enabled
     */
    public boolean isUseInboundSessionEnabled() {
        return info.isUseInboundSessionEnabled();
    }

    /**
     * @return use inbound session
     */
    public Boolean getUseInboundSession() {
        return info.getUseInboundSession();
    }

    /**
     * @param useInboundSession
     */
    public void setUseInboundSession(Boolean useInboundSession) {
        if (log.isDebugEnabled()) {
            log.debug("setting [useInboundSession] to: " + useInboundSession);
        }
        info.setUseInboundSession(useInboundSession);
    }

    public boolean isUseSessionArgs() {
        return info.isUseSessionArgs();
    }

    public Boolean getUseSessionArgs() {
        return info.getUseSessionArgs();
    }

    /**
     * if true, calls to managed connection factory.connection.createSession
     * will respect the passed in args. When false (default) the args are
     * ignored b/c the container will do transaction demarcation via xa or local
     * transaction rar contracts. This option is useful when a managed
     * connection is used in plain jms mode and a jms transacted session session
     * is required.
     *
     * @param useSessionArgs
     */
    public void setUseSessionArgs(Boolean useSessionArgs) {
        if (log.isDebugEnabled()) {
            log.debug(this + ", setting [useSessionArgs] to: " + useSessionArgs);
        }
        info.setUseSessionArgs(useSessionArgs);
    }
}
