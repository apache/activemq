/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.ra;

import org.apache.activemq.RedeliveryPolicy;

import javax.resource.spi.ResourceAdapter;
import javax.resource.spi.ActivationSpec;
import java.util.List;

/**
 * 
 */
public interface MessageActivationSpec extends ActivationSpec {
    boolean isValidUseRAManagedTransaction(List<String> errorMessages);

    boolean isValidNoLocal(List<String> errorMessages);

    boolean isValidMessageSelector(List<String> errorMessages);

    boolean isValidMaxSessions(List<String> errorMessages);

    boolean isValidMaxMessagesPerSessions(List<String> errorMessages);

    boolean isValidMaxMessagesPerBatch(List<String> errorMessages);

    boolean isValidEnableBatch(List<String> errorMessages);

    /**
     * @see javax.resource.spi.ResourceAdapterAssociation#getResourceAdapter()
     */
    ResourceAdapter getResourceAdapter();

    /**
     * @return Returns the destinationType.
     */
    String getDestinationType();

    String getPassword();

    String getUserName();

    /**
     * @return Returns the messageSelector.
     */
    String getMessageSelector();

    /**
     * @return Returns the noLocal.
     */
    String getNoLocal();

    String getAcknowledgeMode();

    String getClientId();

    String getDestination();

    String getSubscriptionDurability();

    String getSubscriptionName();

    boolean isValidSubscriptionName(List<String> errorMessages);

    boolean isValidClientId(List<String> errorMessages);

    boolean isDurableSubscription();

    boolean isValidSubscriptionDurability(List<String> errorMessages);

    boolean isValidAcknowledgeMode(List<String> errorMessages);

    boolean isValidDestinationType(List<String> errorMessages);

    boolean isValidDestination(List<String> errorMessages);

    boolean isEmpty(String value);

    int getAcknowledgeModeForSession();

    String getMaxMessagesPerSessions();

    String getMaxSessions();

    String getUseRAManagedTransaction();

    int getMaxMessagesPerSessionsIntValue();

    int getMaxSessionsIntValue();

    boolean isUseRAManagedTransactionEnabled();

    boolean getNoLocalBooleanValue();

    String getEnableBatch();

    boolean getEnableBatchBooleanValue();

    int getMaxMessagesPerBatchIntValue();

    String getMaxMessagesPerBatch();

    double getBackOffMultiplier();
    
    long getMaximumRedeliveryDelay();

    long getInitialRedeliveryDelay();

    int getMaximumRedeliveries();

    boolean isUseExponentialBackOff();

    RedeliveryPolicy redeliveryPolicy();

    RedeliveryPolicy lazyCreateRedeliveryPolicy();
}
