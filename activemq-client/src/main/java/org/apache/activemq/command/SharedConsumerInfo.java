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
 * Extends {@link ConsumerInfo} with JMS 3.1 shared subscription fields.
 *
 * <p>The v13 OpenWire marshallers create instances of this class instead of
 * {@code ConsumerInfo}, so the {@code shared} and {@code durable} fields
 * survive wire serialization and KahaDB persistence round-trips.
 *
 * <p>Broker-side code detects shared consumers via
 * {@code info instanceof SharedConsumerInfo}.
 */
public class SharedConsumerInfo extends ConsumerInfo {

    protected boolean shared;
    protected boolean durable;
    protected boolean userSpecifiedClientId;

    public SharedConsumerInfo() {
    }

    public SharedConsumerInfo(ConsumerId consumerId) {
        super(consumerId);
    }

    public boolean isShared() {
        return shared;
    }

    public void setShared(boolean shared) {
        this.shared = shared;
    }

    @Override
    public boolean isDurable() {
        return durable;
    }

    public void setDurable(boolean durable) {
        this.durable = durable;
    }

    public boolean isUserSpecifiedClientId() {
        return userSpecifiedClientId;
    }

    public void setUserSpecifiedClientId(boolean userSpecifiedClientId) {
        this.userSpecifiedClientId = userSpecifiedClientId;
    }

    @Override
    public SharedConsumerInfo copy() {
        SharedConsumerInfo info = new SharedConsumerInfo();
        copy(info);
        return info;
    }

    public void copy(SharedConsumerInfo info) {
        super.copy(info);
        info.shared = shared;
        info.durable = durable;
        info.userSpecifiedClientId = userSpecifiedClientId;
    }
}
