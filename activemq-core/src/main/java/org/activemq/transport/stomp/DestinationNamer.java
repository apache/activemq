/*
 * Copyright (c) 2005 Your Corporation. All Rights Reserved.
 */
package org.activemq.transport.stomp;

import org.activemq.command.ActiveMQDestination;

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
