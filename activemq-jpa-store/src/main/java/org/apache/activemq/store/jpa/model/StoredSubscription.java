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
package org.apache.activemq.store.jpa.model;

import javax.persistence.Basic;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

import org.apache.openjpa.persistence.jdbc.Index;

/** 
 */
@Entity
public class StoredSubscription {

    /**
     * Application identity class for Magazine.
     */
    public static class SubscriptionId {

        public String destination;
        public String clientId;
        public String subscriptionName;

        public boolean equals(Object other) {
            if (other == this) {
                return true;
            }
            if (!(other instanceof SubscriptionId)) {
                return false;
            }

            SubscriptionId sid = (SubscriptionId)other;
            return (destination == sid.destination || (destination != null && destination.equals(sid.destination)))
                   && (clientId == sid.clientId || (clientId != null && clientId.equals(sid.clientId)))
                   && (subscriptionName == sid.subscriptionName || (subscriptionName != null && subscriptionName.equals(sid.subscriptionName)));
        }

        /**
         * Hashcode must also depend on identity values.
         */
        public int hashCode() {
            return ((destination == null) ? 0 : destination.hashCode()) ^ ((clientId == null) ? 0 : clientId.hashCode()) ^ ((subscriptionName == null) ? 0 : subscriptionName.hashCode());
        }

        public String toString() {
            return destination + ":" + clientId + ":" + subscriptionName;
        }

        public String getClientId() {
            return clientId;
        }

        public void setClientId(String clientId) {
            this.clientId = clientId;
        }

        public String getDestination() {
            return destination;
        }

        public void setDestination(String destination) {
            this.destination = destination;
        }

        public String getSubscriptionName() {
            return subscriptionName;
        }

        public void setSubscriptionName(String subscriptionName) {
            this.subscriptionName = subscriptionName;
        }
    }

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private long id;

    @Basic
    @Index(enabled = true, unique = false)
    private String destination;
    @Basic
    @Index(enabled = true, unique = false)
    private String clientId;
    @Basic
    @Index(enabled = true, unique = false)
    private String subscriptionName;

    @Basic
    private long lastAckedId;
    @Basic
    private String selector;
    @Basic
    private String subscribedDestination;

    public long getLastAckedId() {
        return lastAckedId;
    }

    public void setLastAckedId(long lastAckedId) {
        this.lastAckedId = lastAckedId;
    }

    public String getSelector() {
        return selector;
    }

    public void setSelector(String selector) {
        this.selector = selector;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getSubscriptionName() {
        return subscriptionName;
    }

    public void setSubscriptionName(String subscriptionName) {
        this.subscriptionName = subscriptionName;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getSubscribedDestination() {
        return subscribedDestination;
    }

    public void setSubscribedDestination(String subscribedDestination) {
        this.subscribedDestination = subscribedDestination;
    }
}
