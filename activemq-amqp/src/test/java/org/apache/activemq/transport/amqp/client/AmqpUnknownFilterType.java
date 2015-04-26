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
package org.apache.activemq.transport.amqp.client;

import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedLong;

/**
 * A Described Type wrapper for an unsupported filter that the broker should ignore.
 */
public class AmqpUnknownFilterType implements DescribedType {

    public static final AmqpUnknownFilterType UNKOWN_FILTER = new AmqpUnknownFilterType();

    public static final UnsignedLong UNKNOWN_FILTER_CODE = UnsignedLong.valueOf(0x0000468C00000099L);
    public static final Symbol UNKNOWN_FILTER_NAME = Symbol.valueOf("apache.org:unkown-filter:string");
    public static final Object[] UNKNOWN_FILTER_IDS = new Object[] { UNKNOWN_FILTER_CODE, UNKNOWN_FILTER_NAME };

    private final String payload;

    public AmqpUnknownFilterType() {
        this.payload = "UnknownFilter{}";
    }

    @Override
    public Object getDescriptor() {
        return UNKNOWN_FILTER_CODE;
    }

    @Override
    public Object getDescribed() {
        return this.payload;
    }
}
