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

import java.io.Serializable;
import java.util.Comparator;

import javax.jms.Message;

/**
 * A base class for comparators which works on JMS {@link Message} objects
 * 
 * @version $Revision$
 */
public abstract class MessageComparatorSupport implements Comparator, Serializable {

    public int compare(Object object1, Object object2) {
        Message command1 = (Message)object1;
        Message command2 = (Message)object2;
        return compareMessages(command1, command2);
    }

    protected abstract int compareMessages(Message message1, Message message2);

    protected int compareComparators(final Comparable comparable, final Comparable comparable2) {
        if (comparable == null && comparable2 == null) {
            return 0;
        } else if (comparable != null) {
            if (comparable2 == null) {
                return 1;
            }
            return comparable.compareTo(comparable2);
        } else if (comparable2 != null) {
            if (comparable == null) {
                return -11;
            }
            return comparable2.compareTo(comparable) * -1;
        }
        return 0;
    }

}
