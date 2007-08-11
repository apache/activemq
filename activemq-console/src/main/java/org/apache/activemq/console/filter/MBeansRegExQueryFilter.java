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
package org.apache.activemq.console.filter;

import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.ObjectInstance;
import javax.management.ObjectName;

public class MBeansRegExQueryFilter extends RegExQueryFilter {
    /**
     * Creates a regular expression query that is able to match the values of
     * specific mbeans
     * 
     * @param next - next query filter
     */
    public MBeansRegExQueryFilter(QueryFilter next) {
        super(next);
    }

    /**
     * Try to match the object data using the regular expression map. The regex
     * map contains a key-value mapping of an attribute key to a regular
     * expression the value of the key should match. The basic rule of matching
     * is that the data must contain a property key that is included in the
     * regex map, and that the value of the property key should match the regex
     * specified.
     * 
     * @param data - object data to match
     * @param regex - regex map
     * @return true if the data matches the regex map specified
     * @throws Exception
     */
    protected boolean matches(Object data, Map regex) throws Exception {
        // TODO why not just use instanceof?

        // Use reflection to determine where the object should go
        try {
            Method method = this.getClass().getDeclaredMethod("matches", new Class[] {
                data.getClass(), Map.class
            });
            return ((Boolean)method.invoke(this, new Object[] {
                data, regex
            })).booleanValue();
        } catch (NoSuchMethodException e) {
            return false;
        }
    }

    /**
     * Try to match the object instance using the regular expression map
     * 
     * @param data - object instance to match
     * @param regex - regex map
     * @return true if the object instance matches the regex map
     */
    protected boolean matches(ObjectInstance data, Map regex) {
        return matches(data.getObjectName(), regex);
    }

    /**
     * Try to match the object name instance using the regular expression map
     * 
     * @param data - object name to match
     * @param regex - regex map
     * @return true if the object name matches the regex map
     */
    protected boolean matches(ObjectName data, Map regex) {
        for (Iterator i = regex.keySet().iterator(); i.hasNext();) {
            String key = (String)i.next();
            String target = data.getKeyProperty(key);

            // Try to match the value of the property of the object name
            if (target != null && !((Pattern)regex.get(key)).matcher(target).matches()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Try to match the attribute list using the regular expression map
     * 
     * @param data - attribute list to match
     * @param regex - regex map
     * @return true if the attribute list matches the regex map
     */
    protected boolean matches(AttributeList data, Map regex) {
        for (Iterator i = regex.keySet().iterator(); i.hasNext();) {
            String key = (String)i.next();

            // Try to match each regex to the attributes of the mbean including
            // its ObjectName
            for (Iterator j = data.iterator(); j.hasNext();) {
                Attribute attrib = (Attribute)j.next();

                // Try to match to the properties of the ObjectName
                if (attrib.getName().equals(MBeansAttributeQueryFilter.KEY_OBJECT_NAME_ATTRIBUTE)) {
                    String target = ((ObjectName)attrib.getValue()).getKeyProperty(key);

                    if (target == null || !((Pattern)regex.get(key)).matcher(target).matches()) {
                        return false;
                    } else {
                        // If match skip to the next regex
                        break;
                    }

                    // Try to match to the mbean attributes
                } else if (attrib.getName().equals(key)) {
                    if (!((Pattern)regex.get(key)).matcher(attrib.getValue().toString()).matches()) {
                        return false;
                    } else {
                        // If match skip to the next regex
                        break;
                    }

                    // If mbean does not contain the specified attribute
                } else {
                    return false;
                }
            }
        }
        return true;
    }
}
