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
package org.apache.activemq.plugin;

import javax.xml.bind.JAXBElement;

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.activemq.broker.region.virtual.FilteredDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.schema.core.DtoFilteredDestination;
import org.apache.activemq.schema.core.DtoTopic;
import org.apache.activemq.schema.core.DtoQueue;
import org.apache.activemq.schema.core.DtoAuthenticationUser;
import org.apache.activemq.security.AuthenticationUser;

public class JAXBUtils {

    public static Method findSetter(Object instance, String elementName) {
        String setter = "set" + elementName;
        for (Method m : instance.getClass().getMethods()) {
            if (setter.equals(m.getName())) {
                return m;
            }
        }
        return null;
    }

    public static Object inferTargetObject(Object elementContent) {
        if (DtoTopic.class.isAssignableFrom(elementContent.getClass())) {
            return new ActiveMQTopic();
        } else if (DtoQueue.class.isAssignableFrom(elementContent.getClass())) {
            return new ActiveMQQueue();
        } else if (DtoAuthenticationUser.class.isAssignableFrom(elementContent.getClass())) {
            return new AuthenticationUser();
        } else if (DtoFilteredDestination.class.isAssignableFrom(elementContent.getClass())) {
            return new FilteredDestination();            
        } else {
            return new Object();
        }
    }

    public static Object matchType(List<Object> parameterValues, Class<?> aClass) {
        Object result = parameterValues;
        if (Set.class.isAssignableFrom(aClass)) {
            result = new HashSet(parameterValues);
        }
        return result;
    }

}
