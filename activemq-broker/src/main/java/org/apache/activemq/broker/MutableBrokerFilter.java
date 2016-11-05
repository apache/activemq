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
package org.apache.activemq.broker;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Like a BrokerFilter but it allows you to switch the getNext().broker. This
 * has more overhead than a BrokerFilter since access to the getNext().broker
 * has to synchronized since it is mutable
 *
 *
 */
public class MutableBrokerFilter extends BrokerFilter {

    protected AtomicReference<Broker> next = new AtomicReference<Broker>();

    public MutableBrokerFilter(Broker next) {
    	super(null); // prevent future code from using the inherited 'next'
        this.next.set(next);
    }

    @Override
    public Broker getAdaptor(Class<?> type) {
        if (type.isInstance(this)) {
            return this;
        }
        return next.get().getAdaptor(type);
    }

    public Broker getNext() {
        return next.get();
    }

    public void setNext(Broker next) {
        this.next.set(next);
    }

}
