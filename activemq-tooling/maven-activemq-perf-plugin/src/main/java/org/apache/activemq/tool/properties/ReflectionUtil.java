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
package org.apache.activemq.tool.properties;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ReflectionUtil {
    private static final Logger LOG = LoggerFactory.getLogger(ReflectionUtil.class);

    private ReflectionUtil() {
    }

    public static void configureClass(Object obj, String key, String val) {
        try {
            String debugInfo;

            Object target = obj;
            Class targetClass = obj.getClass();

            // DEBUG: Debugging Info
            debugInfo = "Invoking: " + targetClass.getName();

            StringTokenizer tokenizer = new StringTokenizer(key, ".");
            String keySubString = key;
            int tokenCount = tokenizer.countTokens();

            // For nested settings, get the object first. -1, do not count the
            // last token
            for (int j = 0; j < tokenCount - 1; j++) {
                // Find getter method first
                String name = tokenizer.nextToken();

                // Check if the target object will accept the settings
                if (target instanceof ReflectionConfigurable && !((ReflectionConfigurable)target).acceptConfig(keySubString, val)) {
                    return;
                } else {
                    // This will reduce the key, so that it will be recognize by
                    // the next object. i.e.
                    // Property name: factory.prefetchPolicy.queuePrefetch
                    // Calling order:
                    // this.getFactory().prefetchPolicy().queuePrefetch();
                    // If factory does not accept the config, it should be given
                    // prefetchPolicy.queuePrefetch as the key
                    // +1 to account for the '.'
                    keySubString = keySubString.substring(name.length() + 1);
                }

                String getMethod = "get" + name.substring(0, 1).toUpperCase() + name.substring(1);
                Method method = targetClass.getMethod(getMethod, new Class[] {});
                target = method.invoke(target, null);
                targetClass = target.getClass();

                debugInfo += "." + getMethod + "()";
            }

            // Property name
            String property = tokenizer.nextToken();
            // Check if the target object will accept the settings
            if (target instanceof ReflectionConfigurable && !((ReflectionConfigurable)target).acceptConfig(property, val)) {
                return;
            }

            // Find setter method
            Method setterMethod = findSetterMethod(targetClass, property);

            // Get the first parameter type. This assumes that there is only one
            // parameter.
            if (setterMethod == null) {
                throw new IllegalAccessException("Unable to find appropriate setter method signature for property: " + property);
            }
            Class paramType = setterMethod.getParameterTypes()[0];

            // Set primitive type
            debugInfo += "." + setterMethod + "(" + paramType.getName() + ": " + val + ")";
            if (paramType.isPrimitive()) {
                if (paramType == Boolean.TYPE) {
                    setterMethod.invoke(target, new Object[] {
                        Boolean.valueOf(val)
                    });
                } else if (paramType == Integer.TYPE) {
                    setterMethod.invoke(target, new Object[] {
                        Integer.valueOf(val)
                    });
                } else if (paramType == Long.TYPE) {
                    setterMethod.invoke(target, new Object[] {
                        Long.valueOf(val)
                    });
                } else if (paramType == Double.TYPE) {
                    setterMethod.invoke(target, new Object[] {
                        Double.valueOf(val)
                    });
                } else if (paramType == Float.TYPE) {
                    setterMethod.invoke(target, new Object[] {
                        Float.valueOf(val)
                    });
                } else if (paramType == Short.TYPE) {
                    setterMethod.invoke(target, new Object[] {
                        Short.valueOf(val)
                    });
                } else if (paramType == Byte.TYPE) {
                    setterMethod.invoke(target, new Object[] {
                        Byte.valueOf(val)
                    });
                } else if (paramType == Character.TYPE) {
                    setterMethod.invoke(target, new Object[] {
                        new Character(val.charAt(0))
                    });
                }
            } else {
                // Set String type
                if (paramType == String.class) {
                    setterMethod.invoke(target, new Object[] {
                        val
                    });

                    // For unknown object type, try to create an instance of the
                    // object using a String constructor
                } else {
                    Constructor c = paramType.getConstructor(new Class[] {
                        String.class
                    });
                    Object paramObject = c.newInstance(new Object[] {
                        val
                    });

                    setterMethod.invoke(target, new Object[] {
                        paramObject
                    });
                }
            }
            LOG.debug(debugInfo);

        } catch (Exception e) {
            LOG.warn(e.toString());
        }
    }

    public static void configureClass(Object obj, Properties props) {
        for (Iterator i = props.keySet().iterator(); i.hasNext();) {
            try {
                String key = (String)i.next();
                String val = props.getProperty(key);

                configureClass(obj, key, val);
            } catch (Throwable t) {
                // Let's catch any exception as this could be cause by the
                // foreign class
                t.printStackTrace();
            }
        }
    }

    public static Properties retrieveObjectProperties(Object obj) {
        Properties props = new Properties();
        try {
            props.putAll(retrieveClassProperties("", obj.getClass(), obj));
        } catch (Exception e) {
            LOG.warn(e.toString());
        }
        return props;
    }

    protected static Properties retrieveClassProperties(String prefix, Class targetClass, Object targetObject) {
        if (targetClass == null || targetObject == null) {
            return new Properties();
        } else {
            Properties props = new Properties();
            Method[] getterMethods = findAllGetterMethods(targetClass);
            for (int i = 0; i < getterMethods.length; i++) {
                try {
                    String propertyName = getPropertyName(getterMethods[i].getName());
                    Class retType = getterMethods[i].getReturnType();

                    // If primitive or string type, return it
                    if (retType.isPrimitive() || retType == String.class) {
                        // Check for an appropriate setter method to consider it
                        // as a property
                        if (findSetterMethod(targetClass, propertyName) != null) {
                            Object val = null;
                            try {
                                val = getterMethods[i].invoke(targetObject, null);
                            } catch (InvocationTargetException e) {
                                e.printStackTrace();
                            } catch (IllegalAccessException e) {
                                e.printStackTrace();
                            }
                            props.setProperty(prefix + propertyName, val + "");
                        }
                    } else {
                        try {
                            Object val = getterMethods[i].invoke(targetObject, null);
                            if (val != null) {
                                props.putAll(retrieveClassProperties(propertyName + ".", val.getClass(), val));
                            }
                        } catch (InvocationTargetException e) {
                            e.printStackTrace();
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        }
                    }
                } catch (Throwable t) {
                    // Let's catch any exception, cause this could be cause by
                    // the foreign class
                    t.printStackTrace();
                }
            }
            return props;
        }
    }

    private static Method findSetterMethod(Class targetClass, String propertyName) {
        String methodName = "set" + propertyName.substring(0, 1).toUpperCase() + propertyName.substring(1);

        Method[] methods = targetClass.getMethods();
        for (int i = 0; i < methods.length; i++) {
            if (methods[i].getName().equals(methodName) && isSetterMethod(methods[i])) {
                return methods[i];
            }
        }
        return null;
    }

    private static Method findGetterMethod(Class targetClass, String propertyName) {
        String methodName1 = "get" + propertyName.substring(0, 1).toUpperCase() + propertyName.substring(1);
        String methodName2 = "is" + propertyName.substring(0, 1).toUpperCase() + propertyName.substring(1);

        Method[] methods = targetClass.getMethods();
        for (int i = 0; i < methods.length; i++) {
            if ((methods[i].getName().equals(methodName1) || methods[i].getName().equals(methodName2)) && isGetterMethod(methods[i])) {
                return methods[i];
            }
        }
        return null;
    }

    private static Method[] findAllGetterMethods(Class targetClass) {
        List getterMethods = new ArrayList();
        Method[] methods = targetClass.getMethods();

        for (int i = 0; i < methods.length; i++) {
            if (isGetterMethod(methods[i])) {
                getterMethods.add(methods[i]);
            }
        }

        return (Method[])getterMethods.toArray(new Method[] {});
    }

    private static boolean isGetterMethod(Method method) {
        // Check method signature first
        // If 'get' method, must return a non-void value
        // If 'is' method, must return a boolean value
        // Both must have no parameters
        // Method must not belong to the Object class to prevent infinite loop
        return ((method.getName().startsWith("is") && method.getReturnType() == Boolean.TYPE) || (method.getName().startsWith("get") && method.getReturnType() != Void.TYPE))
               && (method.getParameterTypes().length == 0) && method.getDeclaringClass() != Object.class;
    }

    private static boolean isSetterMethod(Method method) {
        // Check method signature first
        if (method.getName().startsWith("set") && method.getReturnType() == Void.TYPE) {
            Class[] paramType = method.getParameterTypes();
            // Check that it can only accept one parameter
            if (paramType.length == 1) {
                // Check if parameter is a primitive or can accept a String
                // parameter
                if (paramType[0].isPrimitive() || paramType[0] == String.class) {
                    return true;
                } else {
                    // Check if object can accept a string as a constructor
                    try {
                        if (paramType[0].getConstructor(new Class[] {
                            String.class
                        }) != null) {
                            return true;
                        }
                    } catch (NoSuchMethodException e) {
                        // Do nothing
                    }
                }
            }
        }
        return false;
    }

    private static String getPropertyName(String methodName) {
        String name;
        if (methodName.startsWith("get")) {
            name = methodName.substring(3);
        } else if (methodName.startsWith("set")) {
            name = methodName.substring(3);
        } else if (methodName.startsWith("is")) {
            name = methodName.substring(2);
        } else {
            name = "";
        }

        return name.substring(0, 1).toLowerCase() + name.substring(1);
    }
}
