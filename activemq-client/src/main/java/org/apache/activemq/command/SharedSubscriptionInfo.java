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
package org.apache.activemq.command;

/**
 * Extends {@link SubscriptionInfo} with a {@code shared} flag for JMS 3.1
 * shared subscription persistence.
 *
 * <p>The v13 OpenWire marshallers create instances of this class, so the
 * {@code shared} flag survives KahaDB journal round-trips. Recovery code
 * detects shared subscriptions via {@code info instanceof SharedSubscriptionInfo}.
 */
public class SharedSubscriptionInfo extends SubscriptionInfo {

    protected boolean shared;

    public SharedSubscriptionInfo() {
    }

    public SharedSubscriptionInfo(String clientId, String subscriptionName) {
        super(clientId, subscriptionName);
    }

    public boolean isShared() {
        return shared;
    }

    public void setShared(boolean shared) {
        this.shared = shared;
    }
}
