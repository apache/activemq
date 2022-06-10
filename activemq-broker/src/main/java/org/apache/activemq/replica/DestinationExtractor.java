package org.apache.activemq.replica;

import org.apache.activemq.broker.region.BaseDestination;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DestinationFilter;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.Topic;

public class DestinationExtractor {

    static Queue extractQueue(Destination destination) {
        return extract(destination, Queue.class);
    }

    static Topic extractTopic(Destination destination) {
        return extract(destination, Topic.class);
    }

    static BaseDestination extractBaseDestination(Destination destination) {
        return extract(destination, BaseDestination.class);
    }

    private static <T extends Destination> T extract(Destination destination, Class<T> clazz) {
        Destination result = destination;
        while (result != null && !clazz.isInstance(result)) {
            if (result instanceof DestinationFilter) {
                result = ((DestinationFilter) result).getNext();
            } else {
                return null;
            }
        }
        return (T) result;
    }
}
