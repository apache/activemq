/**
 *
 * Copyright 2004 The Apache Software Foundation
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
package org.apache.activemq.advisory;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTopic;

public class AdvisorySupport {
    
    public static final String ADVISORY_TOPIC_PREFIX = "ActiveMQ.Advisory.";
    public static final ActiveMQTopic CONNECTION_ADVISORY_TOPIC = new ActiveMQTopic(ADVISORY_TOPIC_PREFIX+"Connection");
    public static final ActiveMQTopic QUEUE_ADVISORY_TOPIC = new ActiveMQTopic(ADVISORY_TOPIC_PREFIX+"Queue");
    public static final ActiveMQTopic TOPIC_ADVISORY_TOPIC = new ActiveMQTopic(ADVISORY_TOPIC_PREFIX+"Topic");
    public static final ActiveMQTopic TEMP_QUEUE_ADVISORY_TOPIC = new ActiveMQTopic(ADVISORY_TOPIC_PREFIX+"TempQueue");
    public static final ActiveMQTopic TEMP_TOPIC_ADVISORY_TOPIC = new ActiveMQTopic(ADVISORY_TOPIC_PREFIX+"TempTopic");
    public static final String PRODUCER_ADVISORY_TOPIC_PREFIX = ADVISORY_TOPIC_PREFIX+"Producer.";
    public static final String CONSUMER_ADVISORY_TOPIC_PREFIX = ADVISORY_TOPIC_PREFIX+"Consumer.";
    public static final String ADIVSORY_MESSAGE_TYPE = "Advisory";
    public static final ActiveMQTopic TEMP_DESTINATION_COMPOSITE_ADVISORY_TOPIC = new ActiveMQTopic(TEMP_QUEUE_ADVISORY_TOPIC+","+TEMP_TOPIC_ADVISORY_TOPIC);

    public static ActiveMQTopic getConnectionAdvisoryTopic() {
        return CONNECTION_ADVISORY_TOPIC;
    }

    public static ActiveMQTopic getConsumerAdvisoryTopic(ActiveMQDestination destination) {
        String name = CONSUMER_ADVISORY_TOPIC_PREFIX+destination.getQualifiedName();
        return new ActiveMQTopic(name);
    }
    
    public static ActiveMQTopic getProducerAdvisoryTopic(ActiveMQDestination destination) {
        String name = PRODUCER_ADVISORY_TOPIC_PREFIX+destination.getQualifiedName();
        return new ActiveMQTopic(name);
    }
    
    public static ActiveMQTopic getDestinationAdvisoryTopic(ActiveMQDestination destination) {
        switch( destination.getDestinationType() ) {
        case ActiveMQDestination.QUEUE_TYPE:
            return QUEUE_ADVISORY_TOPIC;
        case ActiveMQDestination.TOPIC_TYPE:
            return TOPIC_ADVISORY_TOPIC;
        case ActiveMQDestination.TEMP_QUEUE_TYPE:
            return TEMP_QUEUE_ADVISORY_TOPIC;
        case ActiveMQDestination.TEMP_TOPIC_TYPE:
            return TEMP_TOPIC_ADVISORY_TOPIC;
        }
        throw new RuntimeException("Unknown destination type: "+destination.getDestinationType());
    }

    public static boolean isDestinationAdvisoryTopic(ActiveMQDestination destination) {
        if( destination.isComposite() ) {
            ActiveMQDestination[] compositeDestinations = destination.getCompositeDestinations();
            for (int i = 0; i < compositeDestinations.length; i++) {
                if( isDestinationAdvisoryTopic(compositeDestinations[i]) ) {
                    return true;
                }
            }
            return false;
        } else {
            return 
                destination.equals(TEMP_QUEUE_ADVISORY_TOPIC)
                || destination.equals(TEMP_TOPIC_ADVISORY_TOPIC)
                || destination.equals(QUEUE_ADVISORY_TOPIC)
                || destination.equals(TOPIC_ADVISORY_TOPIC);
        }
    }

    public static boolean isAdvisoryTopic(ActiveMQDestination destination) {
        if( destination.isComposite() ) {
            ActiveMQDestination[] compositeDestinations = destination.getCompositeDestinations();
            for (int i = 0; i < compositeDestinations.length; i++) {
                if( isAdvisoryTopic(compositeDestinations[i]) ) {
                    return true;
                }
            }
            return false;
        } else {
            return destination.isTopic() && destination.getPhysicalName().startsWith(ADVISORY_TOPIC_PREFIX);
        }
    }

    public static boolean isConnectionAdvisoryTopic(ActiveMQDestination destination) {
        if( destination.isComposite() ) {
            ActiveMQDestination[] compositeDestinations = destination.getCompositeDestinations();
            for (int i = 0; i < compositeDestinations.length; i++) {
                if( isConnectionAdvisoryTopic(compositeDestinations[i]) ) {
                    return true;
                }
            }
            return false;
        } else {
            return destination.equals(CONNECTION_ADVISORY_TOPIC);
        }
    }

    public static boolean isProducerAdvisoryTopic(ActiveMQDestination destination) {
        if( destination.isComposite() ) {
            ActiveMQDestination[] compositeDestinations = destination.getCompositeDestinations();
            for (int i = 0; i < compositeDestinations.length; i++) {
                if( isProducerAdvisoryTopic(compositeDestinations[i]) ) {
                    return true;
                }
            }
            return false;
        } else {
            return destination.isTopic() && destination.getPhysicalName().startsWith(PRODUCER_ADVISORY_TOPIC_PREFIX);
        }
    }

    public static boolean isConsumerAdvisoryTopic(ActiveMQDestination destination) {
        if( destination.isComposite() ) {
            ActiveMQDestination[] compositeDestinations = destination.getCompositeDestinations();
            for (int i = 0; i < compositeDestinations.length; i++) {
                if( isConsumerAdvisoryTopic(compositeDestinations[i]) ) {
                    return true;
                }
            }
            return false;
        } else {
            return destination.isTopic() && destination.getPhysicalName().startsWith(CONSUMER_ADVISORY_TOPIC_PREFIX);
        }
    }

}
