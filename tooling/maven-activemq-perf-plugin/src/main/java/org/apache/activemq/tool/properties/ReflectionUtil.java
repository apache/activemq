/**
 * Copyright 2005-2006 The Apache Software Foundation
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.tool.properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.Properties;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.lang.reflect.Method;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;

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
            int tokenCount = tokenizer.countTokens();

            // For nested settings, get the object first. -2, do not count the first and last token
            for (int j=0; j<tokenCount-1; j++) {
                // Find getter method first
                String name = tokenizer.nextToken();
                String getMethod = "get" + name.substring(0,1).toUpperCase() + name.substring(1);
                Method method = targetClass.getMethod(getMethod, new Class[] {});
                target = method.invoke(target, null);
                targetClass = target.getClass();

                debugInfo += ("." + getMethod + "()");
            }

            // Property name
            String property = tokenizer.nextToken();

            // Check if the target object will accept the settings
            if (obj instanceof ReflectionConfigurable && !((ReflectionConfigurable)target).acceptConfig(property, val)) {
                return;
            }

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
                    targetClass.getMethod(setterMethod, new Class[] {char.class}).invoke(target, new Object[] {new Character(val.charAt(0))});
                }
            } else {
                // Set String type
                if (propertyType == String.class) {
                    targetClass.getMethod(setterMethod, new Class[] {String.class}).invoke(target, new Object[] {val});

                // For unknown object type, try to call the valueOf method of the object
                // to convert the string to the target object type
                } else {
                    // Note valueOf method should be public and static
                    Object param = propertyType.getMethod("valueOf", new Class[] {String.class}).invoke(null, new Object[] {val});
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
            try {
                String key = (String)i.next();
                String val = props.getProperty(key);

                configureClass(obj, key, val);
            } catch (Throwable t) {
                // Let's catch any exception as this could be cause by the foreign class
                t.printStackTrace();
            }
        }
    }

    public static Properties retrieveObjectProperties(Object obj) {
        Properties props = new Properties();
        try {
            props.putAll(retrieveClassProperties("", obj.getClass(), obj));
        } catch (Exception e) {
            log.warn(e);
        }
        return props;
    }

    protected static Properties retrieveClassProperties(String prefix, Class targetClass, Object targetObject) {
        if (targetClass == null || targetObject == null) {
            return new Properties();
        } else {
            Properties props = new Properties();
            Field[] fields = getAllFields(targetClass);
            Method getterMethod;
            for (int i=0; i<fields.length; i++) {
                try {
                    if ((getterMethod = isPropertyField(targetClass, fields[i])) != null) {
                        if (fields[i].getType().isPrimitive() || fields[i].getType() == String.class) {
                            Object val = null;
                            try {
                                val = getterMethod.invoke(targetObject, null);
                            } catch (InvocationTargetException e) {
                                e.printStackTrace();
                            } catch (IllegalAccessException e) {
                                e.printStackTrace();
                            }
                            props.setProperty(prefix + fields[i].getName(), val + "");
                        } else {
                            try {
                                Object val = getterMethod.invoke(targetObject, null);
                                if (val != null) {
                                    props.putAll(retrieveClassProperties(fields[i].getName() + ".", val.getClass(), val));
                                }
                            } catch (InvocationTargetException e) {
                                e.printStackTrace();
                            } catch (IllegalAccessException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                } catch (Throwable t) {
                    // Let's catch any exception, cause this could be cause by the foreign class
                    t.printStackTrace();
                }
            }
            return props;
        }
    }

    protected static Method isPropertyField(Class targetClass, Field targetField) {
        String fieldName = targetField.getName();
        String getMethod = "get" + fieldName.substring(0,1).toUpperCase() + fieldName.substring(1);
        String isMethod  = "is"  + fieldName.substring(0,1).toUpperCase() + fieldName.substring(1);
        String setMethod = "set" + fieldName.substring(0,1).toUpperCase() + fieldName.substring(1);

        // Check setter method
        try {
            targetClass.getMethod(setMethod, new Class[]{targetField.getType()});
        } catch (NoSuchMethodException e) {
            return null;
        }

        // Check getter method and return it if it exists
        try {
            return targetClass.getMethod(getMethod, null);
        } catch (NoSuchMethodException e1) {
            try {
                return targetClass.getMethod(isMethod, null);
            } catch (NoSuchMethodException e2) {
                return null;
            }
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

    public static Field[] getAllFields(Class targetClass) {
        List fieldList = new ArrayList();
        while (targetClass != null) {
            fieldList.addAll(Arrays.asList(targetClass.getDeclaredFields()));
            targetClass = targetClass.getSuperclass();
        }
        return (Field[])fieldList.toArray(new Field[0]);
    }
}
