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

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;
import javax.resource.ResourceException;
import javax.resource.spi.InvalidPropertyException;
import javax.resource.spi.ResourceAdapter;

import org.apache.activemq.RedeliveryPolicy;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.selector.SelectorParser;

/**
 * Configures the inbound JMS consumer specification using ActiveMQ
 * 
 * @org.apache.xbean.XBean element="activationSpec"
 * @version $Revision$ $Date: 2007-08-11 17:29:21 -0400 (Sat, 11 Aug
 *          2007) $
 */
public class ActiveMQActivationSpec implements MessageActivationSpec, Serializable {

    /** Auto-acknowledge constant for <code>acknowledgeMode</code> property * */
    public static final String AUTO_ACKNOWLEDGE_MODE = "Auto-acknowledge";
    /** Dups-ok-acknowledge constant for <code>acknowledgeMode</code> property * */
    public static final String DUPS_OK_ACKNOWLEDGE_MODE = "Dups-ok-acknowledge";
    /** Durable constant for <code>subscriptionDurability</code> property * */
    public static final String DURABLE_SUBSCRIPTION = "Durable";
    /** NonDurable constant for <code>subscriptionDurability</code> property * */
    public static final String NON_DURABLE_SUBSCRIPTION = "NonDurable";
    public static final int INVALID_ACKNOWLEDGE_MODE = -1;

    private static final long serialVersionUID = -7153087544100459975L;

    private transient MessageResourceAdapter resourceAdapter;
    private String destinationType;
    private String messageSelector;
    private String destination;
    private String acknowledgeMode = AUTO_ACKNOWLEDGE_MODE;
    private String userName;
    private String password;
    private String clientId;
    private String subscriptionName;
    private String subscriptionDurability = NON_DURABLE_SUBSCRIPTION;
    private String noLocal = "false";
    private String useRAManagedTransaction = "false";
    private String maxSessions = "10";
    private String maxMessagesPerSessions = "10";
    private String enableBatch = "false";
    private String maxMessagesPerBatch = "10";
    private RedeliveryPolicy redeliveryPolicy;

    /**
     * @see javax.resource.spi.ActivationSpec#validate()
     */
    public void validate() throws InvalidPropertyException {
        List<String> errorMessages = new ArrayList<String>();
        List<PropertyDescriptor> propsNotSet = new ArrayList<PropertyDescriptor>();
        try {
            if (!isValidDestination(errorMessages)) {
                propsNotSet.add(new PropertyDescriptor("destination", ActiveMQActivationSpec.class));
            }
            if (!isValidDestinationType(errorMessages)) {
                propsNotSet.add(new PropertyDescriptor("destinationType", ActiveMQActivationSpec.class));
            }
            if (!isValidAcknowledgeMode(errorMessages)) {
                propsNotSet.add(new PropertyDescriptor("acknowledgeMode", ActiveMQActivationSpec.class));
            }
            if (!isValidSubscriptionDurability(errorMessages)) {
                propsNotSet.add(new PropertyDescriptor("subscriptionDurability", ActiveMQActivationSpec.class));
            }
            if (!isValidClientId(errorMessages)) {
                propsNotSet.add(new PropertyDescriptor("clientId", ActiveMQActivationSpec.class));
            }
            if (!isValidSubscriptionName(errorMessages)) {
                propsNotSet.add(new PropertyDescriptor("subscriptionName", ActiveMQActivationSpec.class));
            }
            if (!isValidMaxMessagesPerSessions(errorMessages)) {
                propsNotSet.add(new PropertyDescriptor("maxMessagesPerSessions", ActiveMQActivationSpec.class));
            }
            if (!isValidMaxSessions(errorMessages)) {
                propsNotSet.add(new PropertyDescriptor("maxSessions", ActiveMQActivationSpec.class));
            }
            if (!isValidMessageSelector(errorMessages)) {
                propsNotSet.add(new PropertyDescriptor("messageSelector", ActiveMQActivationSpec.class));
            }
            if (!isValidNoLocal(errorMessages)) {
                propsNotSet.add(new PropertyDescriptor("noLocal", ActiveMQActivationSpec.class));
            }
            if (!isValidUseRAManagedTransaction(errorMessages)) {
                propsNotSet.add(new PropertyDescriptor("useRAManagedTransaction", ActiveMQActivationSpec.class));
            }
            if (!isValidEnableBatch(errorMessages)) {
                propsNotSet.add(new PropertyDescriptor("enableBatch", ActiveMQActivationSpec.class));
            }
            if (!isValidMaxMessagesPerBatch(errorMessages)) {
                propsNotSet.add(new PropertyDescriptor("maxMessagesPerBatch", ActiveMQActivationSpec.class));
            }

        } catch (IntrospectionException e) {
            e.printStackTrace();
        }

        if (propsNotSet.size() > 0) {
            StringBuffer b = new StringBuffer();
            b.append("Invalid settings:");
            for (Iterator<String> iter = errorMessages.iterator(); iter.hasNext();) {
                b.append(" ");
                b.append(iter.next());
            }
            InvalidPropertyException e = new InvalidPropertyException(b.toString());
            final PropertyDescriptor[] descriptors = propsNotSet.toArray(new PropertyDescriptor[propsNotSet.size()]);
            e.setInvalidPropertyDescriptors(descriptors);
            throw e;
        }
    }

    private boolean isValidUseRAManagedTransaction(List<String> errorMessages) {
        try {
            new Boolean(noLocal);
            return true;
        } catch (Throwable e) {
            //
        }
        errorMessages.add("noLocal must be set to: true or false.");
        return false;
    }

    private boolean isValidNoLocal(List<String> errorMessages) {
        try {
            new Boolean(noLocal);
            return true;
        } catch (Throwable e) {
            //
        }
        errorMessages.add("noLocal must be set to: true or false.");
        return false;
    }

    private boolean isValidMessageSelector(List<String> errorMessages) {
        try {
            if (!isEmpty(messageSelector)) {
                new SelectorParser().parse(messageSelector);
            }
            return true;
        } catch (Throwable e) {
            errorMessages.add("messageSelector not set to valid message selector: " + e.getMessage());
            return false;
        }
    }

    private boolean isValidMaxSessions(List<String> errorMessages) {
        try {
            if (Integer.parseInt(maxSessions) > 0) {
                return true;
            }
        } catch (NumberFormatException e) {
            //
        }
        errorMessages.add("maxSessions must be set to number > 0");
        return false;
    }

    private boolean isValidMaxMessagesPerSessions(List<String> errorMessages) {
        try {
            if (Integer.parseInt(maxMessagesPerSessions) > 0) {
                return true;
            }
        } catch (NumberFormatException e) {
            //
        }
        errorMessages.add("maxMessagesPerSessions must be set to number > 0");
        return false;
    }

    private boolean isValidMaxMessagesPerBatch(List<String> errorMessages) {
        try {
            if (Integer.parseInt(maxMessagesPerBatch) > 0) {
                return true;
            }
        } catch (NumberFormatException e) {
            //
        }
        errorMessages.add("maxMessagesPerBatch must be set to number > 0");
        return false;
    }

    private boolean isValidEnableBatch(List<String> errorMessages) {
        try {
            new Boolean(enableBatch);
            return true;
        } catch (Throwable e) {
            //
        }
        errorMessages.add("enableBatch must be set to: true or false");
        return false;
    }

    /**
     * @see javax.resource.spi.ResourceAdapterAssociation#getResourceAdapter()
     */
    public ResourceAdapter getResourceAdapter() {
        return resourceAdapter;
    }

    /**
     * @see javax.resource.spi.ResourceAdapterAssociation#setResourceAdapter(javax.resource.spi.ResourceAdapter)
     */
    public void setResourceAdapter(ResourceAdapter resourceAdapter) throws ResourceException {
        // spec section 5.3.3
        if (this.resourceAdapter != null) {
            throw new ResourceException("ResourceAdapter already set");
        }
        if (!(resourceAdapter instanceof MessageResourceAdapter)) {
            throw new ResourceException("ResourceAdapter is not of type: " + MessageResourceAdapter.class.getName());
        }
        this.resourceAdapter = (MessageResourceAdapter)resourceAdapter;
    }

    // ///////////////////////////////////////////////////////////////////////
    //
    // Java Bean getters and setters for this ActivationSpec class.
    //
    // ///////////////////////////////////////////////////////////////////////
    /**
     * @return Returns the destinationType.
     */
    public String getDestinationType() {
        if (!isEmpty(destinationType)) {
            return destinationType;
        }
        return null;
    }

    /**
     * @param destinationType The destinationType to set.
     */
    public void setDestinationType(String destinationType) {
        this.destinationType = destinationType;
    }

    /**
     * 
     */
    public String getPassword() {
        if (!isEmpty(password)) {
            return password;
        }
        return null;
    }

    /**
     * 
     */
    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * 
     */
    public String getUserName() {
        if (!isEmpty(userName)) {
            return userName;
        }
        return null;
    }

    /**
     * 
     */
    public void setUserName(String userName) {
        this.userName = userName;
    }

    /**
     * @return Returns the messageSelector.
     */
    public String getMessageSelector() {
        if (!isEmpty(messageSelector)) {
            return messageSelector;
        }
        return null;
    }

    /**
     * @param messageSelector The messageSelector to set.
     */
    public void setMessageSelector(String messageSelector) {
        this.messageSelector = messageSelector;
    }

    /**
     * @return Returns the noLocal.
     */
    public String getNoLocal() {
        return noLocal;
    }

    /**
     * @param noLocal The noLocal to set.
     */
    public void setNoLocal(String noLocal) {
        if (noLocal != null) {
            this.noLocal = noLocal;
        }
    }

    /**
     * 
     */
    public String getAcknowledgeMode() {
        if (!isEmpty(acknowledgeMode)) {
            return acknowledgeMode;
        }
        return null;
    }

    /**
     * 
     */
    public void setAcknowledgeMode(String acknowledgeMode) {
        this.acknowledgeMode = acknowledgeMode;
    }

    /**
     * 
     */
    public String getClientId() {
        if (!isEmpty(clientId)) {
            return clientId;
        }
        return null;
    }

    /**
     * 
     */
    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    /**
     * 
     */
    public String getDestination() {
        if (!isEmpty(destination)) {
            return destination;
        }
        return null;
    }

    /**
     * 
     */
    public void setDestination(String destination) {
        this.destination = destination;
    }

    /**
     * 
     */
    public String getSubscriptionDurability() {
        if (!isEmpty(subscriptionDurability)) {
            return subscriptionDurability;
        }
        return null;
    }

    /**
     * 
     */
    public void setSubscriptionDurability(String subscriptionDurability) {
        this.subscriptionDurability = subscriptionDurability;
    }

    /**
     * 
     */
    public String getSubscriptionName() {
        if (!isEmpty(subscriptionName)) {
            return subscriptionName;
        }
        return null;
    }

    /**
     * 
     */
    public void setSubscriptionName(String subscriptionName) {
        this.subscriptionName = subscriptionName;
    }

    private boolean isValidSubscriptionName(List<String> errorMessages) {
        if (!isDurableSubscription() ? true : subscriptionName != null && subscriptionName.trim().length() > 0) {
            return true;
        }
        errorMessages.add("subscriptionName must be set since durable subscription was requested.");
        return false;
    }

    private boolean isValidClientId(List<String> errorMessages) {
        if (!isDurableSubscription() ? true : clientId != null && clientId.trim().length() > 0) {
            return true;
        }
        errorMessages.add("clientId must be set since durable subscription was requested.");
        return false;
    }

    /**
     * 
     */
    public boolean isDurableSubscription() {
        return DURABLE_SUBSCRIPTION.equals(subscriptionDurability);
    }

    private boolean isValidSubscriptionDurability(List<String> errorMessages) {
        // subscriptionDurability only applies to Topics
        if (DURABLE_SUBSCRIPTION.equals(subscriptionDurability) && getDestinationType() != null && !Topic.class.getName().equals(getDestinationType())) {
            errorMessages.add("subscriptionDurability cannot be set to: " + DURABLE_SUBSCRIPTION + " when destinationType is set to " + Queue.class.getName()
                              + " as it is only valid when destinationType is set to " + Topic.class.getName() + ".");
            return false;
        }
        if (NON_DURABLE_SUBSCRIPTION.equals(subscriptionDurability) || DURABLE_SUBSCRIPTION.equals(subscriptionDurability)) {
            return true;
        }
        errorMessages.add("subscriptionDurability must be set to: " + NON_DURABLE_SUBSCRIPTION + " or " + DURABLE_SUBSCRIPTION + ".");
        return false;
    }

    private boolean isValidAcknowledgeMode(List<String> errorMessages) {
        if (AUTO_ACKNOWLEDGE_MODE.equals(acknowledgeMode) || DUPS_OK_ACKNOWLEDGE_MODE.equals(acknowledgeMode)) {
            return true;
        }
        errorMessages.add("acknowledgeMode must be set to: " + AUTO_ACKNOWLEDGE_MODE + " or " + DUPS_OK_ACKNOWLEDGE_MODE + ".");
        return false;
    }

    private boolean isValidDestinationType(List<String> errorMessages) {
        if (Queue.class.getName().equals(destinationType) || Topic.class.getName().equals(destinationType)) {
            return true;
        }
        errorMessages.add("destinationType must be set to: " + Queue.class.getName() + " or " + Topic.class.getName() + ".");
        return false;
    }

    private boolean isValidDestination(List<String> errorMessages) {
        if (!(destination == null || destination.equals(""))) {
            return true;
        }
        errorMessages.add("destination is a required field and must be set to the destination name.");
        return false;
    }

    private boolean isEmpty(String value) {
        return value == null || "".equals(value.trim());
    }

    /**
     * 
     */
    @Override
    public String toString() {
        return "ActiveMQActivationSpec{" + "acknowledgeMode='" + acknowledgeMode + "'" + ", destinationType='" + destinationType + "'" + ", messageSelector='" + messageSelector + "'"
               + ", destination='" + destination + "'" + ", clientId='" + clientId + "'" + ", subscriptionName='" + subscriptionName + "'" + ", subscriptionDurability='" + subscriptionDurability
               + "'" + "}";
    }

    public int getAcknowledgeModeForSession() {
        if (AUTO_ACKNOWLEDGE_MODE.equals(acknowledgeMode)) {
            return Session.AUTO_ACKNOWLEDGE;
        } else if (DUPS_OK_ACKNOWLEDGE_MODE.equals(acknowledgeMode)) {
            return Session.DUPS_OK_ACKNOWLEDGE;
        } else {
            return INVALID_ACKNOWLEDGE_MODE;
        }
    }

    /**
     * A helper method mostly for use in Dependency Injection containers which
     * allows you to customize the destination and destinationType properties
     * from a single ActiveMQDestination POJO
     */
    public void setActiveMQDestination(ActiveMQDestination destination) {
        setDestination(destination.getPhysicalName());
        if (destination instanceof Queue) {
            setDestinationType(Queue.class.getName());
        } else {
            setDestinationType(Topic.class.getName());
        }
    }

    /**
     * 
     */
    public ActiveMQDestination createDestination() {
        if (isEmpty(destinationType) || isEmpty(destination)) {
            return null;
        }

        ActiveMQDestination dest = null;
        if (Queue.class.getName().equals(destinationType)) {
            dest = new ActiveMQQueue(destination);
        } else if (Topic.class.getName().equals(destinationType)) {
            dest = new ActiveMQTopic(destination);
        } else {
            assert false : "Execution should never reach here";
        }
        return dest;
    }

    public String getMaxMessagesPerSessions() {
        return maxMessagesPerSessions;
    }

    /**
     * 
     */
    public void setMaxMessagesPerSessions(String maxMessagesPerSessions) {
        if (maxMessagesPerSessions != null) {
            this.maxMessagesPerSessions = maxMessagesPerSessions;
        }
    }

    /**
     * 
     */
    public String getMaxSessions() {
        return maxSessions;
    }

    /**
     * 
     */
    public void setMaxSessions(String maxSessions) {
        if (maxSessions != null) {
            this.maxSessions = maxSessions;
        }
    }

    /**
     * 
     */
    public String getUseRAManagedTransaction() {
        return useRAManagedTransaction;
    }

    /**
     * 
     */
    public void setUseRAManagedTransaction(String useRAManagedTransaction) {
        if (useRAManagedTransaction != null) {
            this.useRAManagedTransaction = useRAManagedTransaction;
        }
    }

    /**
     * 
     */
    public int getMaxMessagesPerSessionsIntValue() {
        return Integer.parseInt(maxMessagesPerSessions);
    }

    /**
     * 
     */
    public int getMaxSessionsIntValue() {
        return Integer.parseInt(maxSessions);
    }

    public boolean isUseRAManagedTransactionEnabled() {
        return Boolean.valueOf(useRAManagedTransaction).booleanValue();
    }

    /**
     * 
     */
    public boolean getNoLocalBooleanValue() {
        return Boolean.valueOf(noLocal).booleanValue();
    }

    public String getEnableBatch() {
        return enableBatch;
    }

    /**
     * 
     */
    public void setEnableBatch(String enableBatch) {
        if (enableBatch != null) {
            this.enableBatch = enableBatch;
        }
    }

    public boolean getEnableBatchBooleanValue() {
        return Boolean.valueOf(enableBatch).booleanValue();
    }

    public int getMaxMessagesPerBatchIntValue() {
        return Integer.parseInt(maxMessagesPerBatch);
    }

    public String getMaxMessagesPerBatch() {
        return maxMessagesPerBatch;
    }

    /**
     * 
     */
    public void setMaxMessagesPerBatch(String maxMessagesPerBatch) {
        if (maxMessagesPerBatch != null) {
            this.maxMessagesPerBatch = maxMessagesPerBatch;
        }
    }

    /**
     * 
     */
    public short getBackOffMultiplier() {
        if (redeliveryPolicy == null) {
            return 0;
        }
        return redeliveryPolicy.getBackOffMultiplier();
    }

    /**
     * 
     */
    public long getInitialRedeliveryDelay() {
        if (redeliveryPolicy == null) {
            return 0;
        }
        return redeliveryPolicy.getInitialRedeliveryDelay();
    }

    /**
     * 
     */
    public int getMaximumRedeliveries() {
        if (redeliveryPolicy == null) {
            return 0;
        }
        return redeliveryPolicy.getMaximumRedeliveries();
    }

    /**
     * 
     */
    public boolean isUseExponentialBackOff() {
        if (redeliveryPolicy == null) {
            return false;
        }
        return redeliveryPolicy.isUseExponentialBackOff();
    }

    /**
     * 
     */
    public void setBackOffMultiplier(short backOffMultiplier) {
        lazyCreateRedeliveryPolicy().setBackOffMultiplier(backOffMultiplier);
    }

    /**
     * 
     */
    public void setInitialRedeliveryDelay(long initialRedeliveryDelay) {
        lazyCreateRedeliveryPolicy().setInitialRedeliveryDelay(initialRedeliveryDelay);
    }

    /**
     * 
     */
    public void setMaximumRedeliveries(int maximumRedeliveries) {
        lazyCreateRedeliveryPolicy().setMaximumRedeliveries(maximumRedeliveries);
    }

    /**
     * 
     */
    public void setUseExponentialBackOff(boolean useExponentialBackOff) {
        lazyCreateRedeliveryPolicy().setUseExponentialBackOff(useExponentialBackOff);
    }

    // don't use getter to avoid causing introspection errors in containers
    /**
     * 
     */
    public RedeliveryPolicy redeliveryPolicy() {
        return redeliveryPolicy;
    }

    protected RedeliveryPolicy lazyCreateRedeliveryPolicy() {
        if (redeliveryPolicy == null) {
            redeliveryPolicy = new RedeliveryPolicy();
        }
        return redeliveryPolicy;
    }
}
