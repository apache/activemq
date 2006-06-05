/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.tool;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.Properties;
import java.lang.reflect.Method;
import java.lang.reflect.Field;

public final class ReflectionUtil {
    private static final Log log = LogFactory.getLog(ReflectionUtil.class);

    private ReflectionUtil() {

    }

    public static void configureClass(Object obj, String key, String val) {
        try {
            String debugInfo;

            Object target = obj;
            Class  targetClass = obj.getClass();

            // DEBUG: Debugging Info
            debugInfo = "Invoking: " + targetClass.getName();

            StringTokenizer tokenizer = new StringTokenizer(key, ".");

            // NOTE: Skip the first token, it is assume that this is an indicator for the object itself
            tokenizer.nextToken();

            // For nested settings, get the object first
            for (int j=0; j<tokenizer.countTokens()-1; j++) {
                // Find getter method first
                String name = tokenizer.nextToken();
                String getMethod = "get" + name.substring(0,1).toUpperCase() + name.substring(1);
                Method method = targetClass.getMethod(getMethod, new Class[] {});
                target = method.invoke(target, (Object[])null);
                targetClass = target.getClass();

                debugInfo += ("." + getMethod + "()");
            }

            // Property name
            String property = tokenizer.nextToken();

            // Determine data type of property
            Class propertyType = getField(targetClass, property).getType();

            // Get setter method
            String setterMethod = "set" + property.substring(0,1).toUpperCase() + property.substring(1);

            // Set primitive type
            debugInfo += ("." + setterMethod + "(" + propertyType.getName() + ": " + val + ")");
            if (propertyType.isPrimitive()) {
                if (propertyType == Boolean.TYPE) {
                    targetClass.getMethod(setterMethod, new Class[] {boolean.class}).invoke(target, new Object[] {Boolean.valueOf(val)});
                } else if (propertyType == Integer.TYPE) {
                    targetClass.getMethod(setterMethod, new Class[] {int.class}).invoke(target, new Object[] {Integer.valueOf(val)});
                } else if (propertyType == Long.TYPE) {
                    targetClass.getMethod(setterMethod, new Class[] {long.class}).invoke(target, new Object[] {Long.valueOf(val)});
                } else if (propertyType == Double.TYPE) {
                    targetClass.getMethod(setterMethod, new Class[] {double.class}).invoke(target, new Object[] {Double.valueOf(val)});
                } else if (propertyType == Float.TYPE) {
                    targetClass.getMethod(setterMethod, new Class[] {float.class}).invoke(target, new Object[] {Float.valueOf(val)});
                } else if (propertyType == Short.TYPE) {
                    targetClass.getMethod(setterMethod, new Class[] {short.class}).invoke(target, new Object[] {Short.valueOf(val)});
                } else if (propertyType == Byte.TYPE) {
                    targetClass.getMethod(setterMethod, new Class[] {byte.class}).invoke(target, new Object[] {Byte.valueOf(val)});
                } else if (propertyType == Character.TYPE) {
                    targetClass.getMethod(setterMethod, new Class[] {char.class}).invoke(target, new Object[] {val.charAt(0)});
                }
            } else {
                // Set String type
                if (propertyType == String.class) {
                    targetClass.getMethod(setterMethod, new Class[] {String.class}).invoke(target, new Object[] {val});

                // For unknown object type, try to call the valueOf method of the object
                // to convert the string to the target object type
                } else {
                    Object param = propertyType.getMethod("valueOf", new Class[] {String.class}).invoke(null, val);
                    targetClass.getMethod(setterMethod, new Class[] {propertyType}).invoke(target, new Object[] {param});
                }
            }
            log.debug(debugInfo);

        } catch (Exception e) {
            log.warn(e);
        }
    }

    public static void configureClass(Object obj, Properties props) {
        for (Iterator i = props.keySet().iterator(); i.hasNext();) {
            String key = (String)i.next();
            String val = props.getProperty(key);

            configureClass(obj, key, val);
        }
    }

    public static Field getField(Class targetClass, String fieldName) throws NoSuchFieldException {
        while (targetClass != null) {
            try {
                return targetClass.getDeclaredField(fieldName);
            } catch (NoSuchFieldException e) {
                targetClass = targetClass.getSuperclass();
            }
        }
        throw new NoSuchFieldException(fieldName);
    }
}
