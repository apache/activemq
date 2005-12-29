/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.transport.stomp;

import org.apache.activemq.command.ActiveMQDestination;

import javax.jms.Destination;

import java.net.ProtocolException;

class DestinationNamer {
    static ActiveMQDestination convert(String name) throws ProtocolException {
        if (name == null) {
            return null;
        }
        else if (name.startsWith("/queue/")) {
            String q_name = name.substring("/queue/".length(), name.length());
            return ActiveMQDestination.createDestination(q_name, ActiveMQDestination.QUEUE_TYPE);
        }
        else if (name.startsWith("/topic/")) {
            String t_name = name.substring("/topic/".length(), name.length());
            return ActiveMQDestination.createDestination(t_name, ActiveMQDestination.TOPIC_TYPE);
        }
        else {
            throw new ProtocolException("Illegal destination name: [" + name + "] -- ActiveMQ TTMP destinations " + "must begine with /queue/ or /topic/");
        }

    }

    static String convert(Destination d) {
        if (d == null) {
            return null;
        }
        ActiveMQDestination amq_d = (ActiveMQDestination) d;
        String p_name = amq_d.getPhysicalName();

        StringBuffer buffer = new StringBuffer();
        if (amq_d.isQueue()) {
            buffer.append("/queue/");
        }
        if (amq_d.isTopic()) {
            buffer.append("/topic/");
        }
        buffer.append(p_name);

        return buffer.toString();
    }
}
