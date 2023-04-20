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
package org.apache.activemq.replica;

import org.apache.activemq.broker.region.BaseDestination;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DestinationFilter;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.Topic;

public class DestinationExtractor {

    public static Queue extractQueue(Destination destination) {
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
