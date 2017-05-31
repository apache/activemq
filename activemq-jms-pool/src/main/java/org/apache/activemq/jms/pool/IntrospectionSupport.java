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
package org.apache.activemq.jms.pool;

import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import javax.net.ssl.SSLServerSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class IntrospectionSupport {

    private static final Logger LOG = LoggerFactory.getLogger(IntrospectionSupport.class);

    private IntrospectionSupport() {
    }

    public static boolean setProperties(Object target, Map props) {
        boolean rc = false;

        if (target == null) {
            throw new IllegalArgumentException("target was null.");
        }
        if (props == null) {
            throw new IllegalArgumentException("props was null.");
        }

        for (Iterator<?> iter = props.entrySet().iterator(); iter.hasNext();) {
            Entry<?,?> entry = (Entry<?,?>)iter.next();
            if (setProperty(target, (String)entry.getKey(), entry.getValue())) {
                iter.remove();
                rc = true;
            }
        }

        return rc;
    }

    public static boolean setProperty(Object target, String name, Object value) {
        try {
            Class<?> clazz = target.getClass();
            if (target instanceof SSLServerSocket) {
                // overcome illegal access issues with internal implementation class
                clazz = SSLServerSocket.class;
            }
            Method setter = findSetterMethod(clazz, name);
            if (setter == null) {
                return false;
            }

            // If the type is null or it matches the needed type, just use the
            // value directly
            if (value == null || value.getClass() == setter.getParameterTypes()[0]) {
                setter.invoke(target, value);
            } else {
                // We need to convert it
                setter.invoke(target, convert(value, setter.getParameterTypes()[0]));
            }
            return true;
        } catch (Exception e) {
            LOG.error(String.format("Could not set property %s on %s", name, target), e);
            return false;
        }
    }

    private static Object convert(Object value, Class to) {
        if (value == null) {
            // lets avoid NullPointerException when converting to boolean for null values
            if (boolean.class.isAssignableFrom(to)) {
                return Boolean.FALSE;
            }
            return null;
        }

        // eager same instance type test to avoid the overhead of invoking the type converter
        // if already same type
        if (to.isAssignableFrom(value.getClass())) {
            return to.cast(value);
        }

        if (boolean.class.isAssignableFrom(to) && value instanceof String) {
            return Boolean.valueOf((String)value);
        }

        throw new IllegalArgumentException("Cannot convert from " + value.getClass()
                    + " to " + to + " with value " + value);
    }

    private static Method findSetterMethod(Class clazz, String name) {
        // Build the method name.
        name = "set" + Character.toUpperCase(name.charAt(0)) + name.substring(1);
        Method[] methods = clazz.getMethods();
        for (Method method : methods) {
            Class<?> params[] = method.getParameterTypes();
            if (method.getName().equals(name) && params.length == 1 ) {
                return method;
            }
        }
        return null;
    }

}
