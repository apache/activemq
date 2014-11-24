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
package org.apache.activemq.filter;

import java.lang.IllegalStateException;
import javax.jms.*;
import org.apache.activemq.command.ActiveMQDestination;

/*
  * allow match to any set  of composite destinations, both queues and topics
 */
public class AnyDestination extends ActiveMQDestination {

    public AnyDestination(ActiveMQDestination[] destinations) {
        super(destinations);
        // ensure we are small when it comes to comparison in DestinationMap
        physicalName = "!0";
    }

    @Override
    protected String getQualifiedPrefix() {
        return "Any://";
    }

    @Override
    public byte getDestinationType() {
        return ActiveMQDestination.QUEUE_TYPE & ActiveMQDestination.TOPIC_TYPE;
    }

    @Override
    public byte getDataStructureType() {
        throw new IllegalStateException("not for marshalling");
    }

    @Override
    public boolean isQueue() {
        return true;
    }

    @Override
    public boolean isTopic() {
        return true;
    }
}
