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
package org.apache.activemq.advisory;

import java.util.EventObject;

import javax.jms.Destination;

import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.DestinationInfo;
import org.apache.activemq.command.ActiveMQDestination;

/**
 * An event caused when a destination is created or deleted
 *
 * 
 */
public class DestinationEvent extends EventObject {
    private static final long serialVersionUID = 2442156576867593780L;
    private DestinationInfo destinationInfo;

    public DestinationEvent(DestinationSource source, DestinationInfo destinationInfo) {
        super(source);
        this.destinationInfo = destinationInfo;
    }

    public ActiveMQDestination getDestination() {
        return getDestinationInfo().getDestination();
    }

    public boolean isAddOperation() {
        return getDestinationInfo().isAddOperation();
    }

    public long getTimeout() {
        return getDestinationInfo().getTimeout();
    }

    public boolean isRemoveOperation() {
        return getDestinationInfo().isRemoveOperation();
    }

    public DestinationInfo getDestinationInfo() {
        return destinationInfo;
    }
}