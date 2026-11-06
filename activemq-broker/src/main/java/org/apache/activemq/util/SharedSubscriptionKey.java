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
package org.apache.activemq.util;

import org.apache.activemq.command.SubscriptionInfo;

/**
 * A {@link SubscriptionKey} subclass that handles null {@code clientId} safely.
 *
 * <p>The parent class NPEs on null {@code clientId} because
 * {@code hashCode()} calls {@code clientId.hashCode()} unconditionally.
 * This subclass coalesces null to an empty-string sentinel before calling
 * {@code super()}, avoiding the NPE while maintaining compatibility with
 * all existing {@code Map<SubscriptionKey, ...>} usage.
 */
public class SharedSubscriptionKey extends SubscriptionKey {

    public static final String SHARED_CLIENT_ID = "";

    public SharedSubscriptionKey(String subscriptionName) {
        super(SHARED_CLIENT_ID, subscriptionName);
    }

    public SharedSubscriptionKey(String clientId, String subscriptionName) {
        super(clientId != null ? clientId : SHARED_CLIENT_ID, subscriptionName);
    }

    public SharedSubscriptionKey(SubscriptionInfo info) {
        this(info.getClientId(), info.getSubscriptionName());
    }
}
