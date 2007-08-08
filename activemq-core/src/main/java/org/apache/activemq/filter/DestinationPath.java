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

import java.util.ArrayList;
import java.util.List;

import javax.jms.JMSException;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;

/**
 * Helper class for decomposing a Destination into a number of paths
 *
 * @version $Revision: 1.3 $
 */
public class DestinationPath {
    protected static final char SEPARATOR = '.';

    public static String[] getDestinationPaths(String subject) {
        List list = new ArrayList();
        int previous = 0;
        int lastIndex = subject.length() - 1;
        while (true) {
            int idx = subject.indexOf(SEPARATOR, previous);
            if (idx < 0) {
                list.add(subject.substring(previous, lastIndex + 1));
                break;
            }
            list.add(subject.substring(previous, idx));
            previous = idx + 1;
        }
        String[] answer = new String[list.size()];
        list.toArray(answer);
        return answer;
    }

    public static String[] getDestinationPaths(Message message) throws JMSException {
        return getDestinationPaths(message.getDestination());
    }

    public static String[] getDestinationPaths(ActiveMQDestination destination) {
        return getDestinationPaths(destination.getPhysicalName());
    }

    /**
     * Converts the paths to a single String seperated by dots.
     *
     * @param paths
     * @return
     */
    public static String toString(String[] paths) {
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < paths.length; i++) {
            if (i > 0) {
                buffer.append(SEPARATOR);
            }
            String path = paths[i];
            if (path == null) {
                buffer.append("*");
            }
            else {
                buffer.append(path);
            }
        }
        return buffer.toString();
    }
}
