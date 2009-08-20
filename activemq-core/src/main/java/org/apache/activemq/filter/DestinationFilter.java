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

import java.io.IOException;

import javax.jms.JMSException;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.util.JMSExceptionSupport;

/**
 * Represents a filter which only operates on Destinations
 * 
 * @version $Revision: 1.3 $
 */
public abstract class DestinationFilter implements BooleanExpression {

    public static final String ANY_DESCENDENT = ">";
    public static final String ANY_CHILD = "*";

    public Object evaluate(MessageEvaluationContext message) throws JMSException {
        return matches(message) ? Boolean.TRUE : Boolean.FALSE;
    }

    public boolean matches(MessageEvaluationContext message) throws JMSException {
        try {
            if (message.isDropped()) {
                return false;
            }
            return matches(message.getMessage().getDestination());
        } catch (IOException e) {
            throw JMSExceptionSupport.create(e);
        }
    }

    public abstract boolean matches(ActiveMQDestination destination);

    public static DestinationFilter parseFilter(ActiveMQDestination destination) {
        if (destination.isComposite()) {
            return new CompositeDestinationFilter(destination);
        }
        String[] paths = DestinationPath.getDestinationPaths(destination);
        int idx = paths.length - 1;
        if (idx >= 0) {
            String lastPath = paths[idx];
            if (lastPath.equals(ANY_DESCENDENT)) {
                return new PrefixDestinationFilter(paths, destination.getDestinationType());
            } else {
                while (idx >= 0) {
                    lastPath = paths[idx--];
                    if (lastPath.equals(ANY_CHILD)) {
                        return new WildcardDestinationFilter(paths, destination.getDestinationType());
                    }
                }
            }
        }

        // if none of the paths contain a wildcard then use equality
        return new SimpleDestinationFilter(destination);
    }
}
