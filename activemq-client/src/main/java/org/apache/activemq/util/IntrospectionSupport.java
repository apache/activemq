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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.net.ssl.SSLServerSocket;

import org.apache.activemq.command.ActiveMQDestination;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class IntrospectionSupport {

    private static final Logger LOG = LoggerFactory.getLogger(IntrospectionSupport.class);

    private IntrospectionSupport() {
    }

    public static boolean getProperties(Object target, Map props, String optionPrefix) {

        boolean rc = false;
        if (target == null) {
            throw new IllegalArgumentException("target was null.");
        }
        if (props == null) {
            throw new IllegalArgumentException("props was null.");
        }

        if (optionPrefix == null) {
            optionPrefix = "";
        }

        Class<?> clazz = target.getClass();
        Method[] methods = clazz.getMethods();
        for (Method method : methods) {
            String name = method.getName();
            Class<?> type = method.getReturnType();
            Class<?> params[] = method.getParameterTypes();
            if ((name.startsWith("is") || name.startsWith("get")) && params.length == 0 && type != null) {

                try {

                    Object value = method.invoke(target);
                    if (value == null) {
                        continue;
                    }

                    String strValue = convertToString(value, type);
                    if (strValue == null) {
                        continue;
                    }
                    if (name.startsWith("get")) {
                        name = name.substring(3, 4).toLowerCase(Locale.ENGLISH)
                                + name.substring(4);
                    } else {
                        name = name.substring(2, 3).toLowerCase(Locale.ENGLISH)
                                + name.substring(3);
                    }
                    props.put(optionPrefix + name, strValue);
                    rc = true;

                } catch (Exception ignore) {
                }
            }
        }

        return rc;
    }

    public static boolean setProperties(Object target, Map<String, ?> props, String optionPrefix) {
        boolean rc = false;
        if (target == null) {
            throw new IllegalArgumentException("target was null.");
        }
        if (props == null) {
            throw new IllegalArgumentException("props was null.");
        }

        for (Iterator<String> iter = props.keySet().iterator(); iter.hasNext();) {
            String name = iter.next();
            if (name.startsWith(optionPrefix)) {
                Object value = props.get(name);
                name = name.substring(optionPrefix.length());
                if (setProperty(target, name, value)) {
                    iter.remove();
                    rc = true;
                }
            }
        }
        return rc;
    }

    public static Map<String, Object> extractProperties(Map props, String optionPrefix) {
        if (props == null) {
            throw new IllegalArgumentException("props was null.");
        }

        HashMap<String, Object> rc = new HashMap<String, Object>(props.size());

        for (Iterator<?> iter = props.keySet().iterator(); iter.hasNext();) {
            String name = (String)iter.next();
            if (name.startsWith(optionPrefix)) {
                Object value = props.get(name);
                name = name.substring(optionPrefix.length());
                rc.put(name, value);
                iter.remove();
            }
        }

        return rc;
    }

    public static boolean setProperties(Object target, Map<?, ?> props) {
        return setProperties(target, props, true);
    }

    public static boolean setProperties(Object target, Map<?, ?> props, boolean removeIfSet) {
        boolean rc = false;

        if (target == null) {
            throw new IllegalArgumentException("target was null.");
        }
        if (props == null) {
            throw new IllegalArgumentException("props was null.");
        }

        for (Iterator<?> iter = props.entrySet().iterator(); iter.hasNext();) {
            Map.Entry<?,?> entry = (Entry<?,?>)iter.next();
            if (setProperty(target, (String)entry.getKey(), entry.getValue())) {
                if (removeIfSet) {
                    iter.remove();
                }
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

            // JDK 11: class or setter might not be publicly accessible
            setter.setAccessible(true);

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

        // special for String[] as we do not want to use a PropertyEditor for that
        if (to.isAssignableFrom(String[].class)) {
            return StringArrayConverter.convertToStringArray(value);
        }

        // special for String to List<ActiveMQDestination> as we do not want to use a PropertyEditor for that
        if (value.getClass().equals(String.class) && to.equals(List.class)) {
            Object answer = StringToListOfActiveMQDestinationConverter.convertToActiveMQDestination(value);
            if (answer != null) {
                return answer;
            }
        }

        TypeConversionSupport.Converter converter = TypeConversionSupport.lookupConverter(value.getClass(), to);
        if (converter != null) {
            return converter.convert(value);
        } else {
            throw new IllegalArgumentException("Cannot convert from " + value.getClass()
                    + " to " + to + " with value " + value);
        }
    }

    public static String convertToString(Object value, Class to) {
        if (value == null) {
            return null;
        }

        // already a String
        if (value instanceof String) {
            return (String) value;
        }

        // special for String[] as we do not want to use a PropertyEditor for that
        if (String[].class.isInstance(value)) {
            String[] array = (String[]) value;
            return StringArrayConverter.convertToString(array);
        }

        // special for String to List<ActiveMQDestination> as we do not want to use a PropertyEditor for that
        if (List.class.isInstance(value)) {
            // if the list is a ActiveMQDestination, then return a comma list
            String answer = StringToListOfActiveMQDestinationConverter.convertFromActiveMQDestination(value);
            if (answer != null) {
                return answer;
            }
        }

        TypeConversionSupport.Converter converter = TypeConversionSupport.lookupConverter(value.getClass(), String.class);
        if (converter != null) {
            return (String) converter.convert(value);
        } else {
            throw new IllegalArgumentException("Cannot convert from " + value.getClass()
                    + " to " + to + " with value " + value);
        }
    }

    public static Method findSetterMethod(Class clazz, String name) {
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

    public static Method findGetterMethod(Class clazz, String name) {
        // Build the method name.
        name = "get" + Character.toUpperCase(name.charAt(0)) + name.substring(1);
        Method[] methods = clazz.getMethods();
        for (Method method : methods) {
            Class<?> params[] = method.getParameterTypes();
            if (method.getName().equals(name) && params.length == 0 ) {
                return method;
            }
        }
        return null;
    }

    public static String toString(Object target) {
        return toString(target, Object.class, null);
    }

    public static String toString(Object target, Class stopClass) {
        return toString(target, stopClass, null);
    }

    public static String toString(Object target, Class stopClass, Map<String, Object> overrideFields) {
        LinkedHashMap<String, Object> map = new LinkedHashMap<String, Object>();
        addFields(target, target.getClass(), stopClass, map);
        if (overrideFields != null) {
            for(String key : overrideFields.keySet()) {
                Object value = overrideFields.get(key);
                map.put(key, value);
            }

        }
        StringBuffer buffer = new StringBuffer(simpleName(target.getClass()));
        buffer.append(" {");
        Set<Entry<String, Object>> entrySet = map.entrySet();
        boolean first = true;
        for (Map.Entry<String,Object> entry : entrySet) {
            Object value = entry.getValue();
            Object key = entry.getKey();
            if (first) {
                first = false;
            } else {
                buffer.append(", ");
            }
            buffer.append(key);
            buffer.append(" = ");

            appendToString(buffer, key, value);
        }
        buffer.append("}");
        return buffer.toString();
    }

    protected static void appendToString(StringBuffer buffer, Object key, Object value) {
        if (value instanceof ActiveMQDestination) {
            ActiveMQDestination destination = (ActiveMQDestination)value;
            buffer.append(destination.getQualifiedName());
        } else if (key.toString().toLowerCase(Locale.ENGLISH).contains("password")){
            buffer.append("*****");
        } else {
            buffer.append(value);
        }
    }

    public static String simpleName(Class clazz) {
        String name = clazz.getName();
        int p = name.lastIndexOf(".");
        if (p >= 0) {
            name = name.substring(p + 1);
        }
        return name;
    }

    private static void addFields(Object target, Class startClass, Class<Object> stopClass, LinkedHashMap<String, Object> map) {

        if (startClass != stopClass) {
            addFields(target, startClass.getSuperclass(), stopClass, map);
        }

        Field[] fields = startClass.getDeclaredFields();
        for (Field field : fields) {
            if (Modifier.isStatic(field.getModifiers()) || Modifier.isTransient(field.getModifiers())
                || Modifier.isPrivate(field.getModifiers())) {
                continue;
            }

            try {
                field.setAccessible(true);
                Object o = field.get(target);
                if (o != null && o.getClass().isArray()) {
                    try {
                        o = Arrays.asList((Object[])o);
                    } catch (Exception e) {
                    }
                }
                map.put(field.getName(), o);
            } catch (Exception e) {
                LOG.debug("Error getting field " + field + " on class " + startClass + ". This exception is ignored.", e);
            }
        }
    }
}
