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
package org.apache.activemq.util;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.command.ActiveMQDestination;

/**
 * Special converter for String -> List<ActiveMQDestination> to be used instead of a
 * {@link java.beans.PropertyEditor} which otherwise causes
 * memory leaks as the JDK {@link java.beans.PropertyEditorManager}
 * is a static class and has strong references to classes, causing
 * problems in hot-deployment environments.
 */
public class StringToListOfActiveMQDestinationConverter {

    public static List<ActiveMQDestination> convertToActiveMQDestination(Object value) {
        if (value == null) {
            return null;
        }

        // text must be enclosed with []

        String text = value.toString();
        if (text.startsWith("[") && text.endsWith("]")) {
            text = text.substring(1, text.length() - 1).trim();

            if (text.isEmpty()) {
                return null;
            }

            String[] array = text.split(",");

            List<ActiveMQDestination> list = new ArrayList<ActiveMQDestination>();
            for (String item : array) {
                list.add(ActiveMQDestination.createDestination(item.trim(), ActiveMQDestination.QUEUE_TYPE));
            }

            return list;
        } else {
            return null;
        }
    }

    public static String convertFromActiveMQDestination(Object value) {
        return convertFromActiveMQDestination(value, false);
    }

    public static String convertFromActiveMQDestination(Object value, boolean includeOptions) {
        if (value == null) {
            return null;
        }

        StringBuilder sb = new StringBuilder("[");
        if (value instanceof List) {
            List list = (List) value;
            for (int i = 0; i < list.size(); i++) {
                Object e = list.get(i);
                if (e instanceof ActiveMQDestination) {
                    ActiveMQDestination destination = (ActiveMQDestination) e;
                    if (includeOptions && destination.getOptions() != null) {
                        try {
                            //Reapply the options as URI parameters
                            sb.append(destination.toString() + URISupport.applyParameters(
                                new URI(""), destination.getOptions()));
                        } catch (URISyntaxException e1) {
                            sb.append(destination);
                        }
                    } else {
                        sb.append(destination);
                    }
                    if (i < list.size() - 1) {
                        sb.append(", ");
                    }
                }
            }
        }
        sb.append("]");

        if (sb.length() > 2) {
            return sb.toString();
        } else {
            return null;
        }
    }

}
