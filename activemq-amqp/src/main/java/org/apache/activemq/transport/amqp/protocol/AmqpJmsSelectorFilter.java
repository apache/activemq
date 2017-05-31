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
package org.apache.activemq.transport.amqp.protocol;

import static org.apache.activemq.transport.amqp.AmqpSupport.JMS_SELECTOR_CODE;

import org.apache.qpid.proton.amqp.DescribedType;

/**
 * A Described Type wrapper for JMS selector values.
 */
public class AmqpJmsSelectorFilter implements DescribedType {

    private final String selector;

    public AmqpJmsSelectorFilter(String selector) {
        this.selector = selector;
    }

    @Override
    public Object getDescriptor() {
        return JMS_SELECTOR_CODE;
    }

    @Override
    public Object getDescribed() {
        return this.selector;
    }

    @Override
    public String toString() {
        return "AmqpJmsSelectorType{" + selector + "}";
    }
}
