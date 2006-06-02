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

import java.util.Map;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.lang.reflect.Method;

public final class ReflectionUtil {
    private static final Log log = LogFactory.getLog(ReflectionUtil.class);

    private ReflectionUtil() {

    }

    public static void configureClass(Object obj, String key, Object val) {
        try {
            Object target = obj;
            Class  targetClass = obj.getClass();

            //System.out.print("Invoking: " + targetClass);

            StringTokenizer tokenizer = new StringTokenizer(key, ".");
            for (int j=0; j<tokenizer.countTokens()-1; j++) {
                // Find getter method first
                String name = tokenizer.nextToken();
                String getMethod = "get" + name.substring(0,1).toUpperCase() + name.substring(1);
                Method method = targetClass.getMethod(getMethod, new Class[] {});
                target = method.invoke(target, null);
                targetClass = target.getClass();

                //System.out.print("." + getMethod + "()");
            }

            // Invoke setter method of last class
            String name = tokenizer.nextToken();
            String methodName = "set" + name.substring(0,1).toUpperCase() + name.substring(1);

            Method method = getPrimitiveMethod(targetClass, methodName, val);
            Object[] objVal = {val};
            method.invoke(target, objVal);
            //method.invoke(target, val);
            //System.out.println("." + methodName + "(" + val + ")");
        } catch (Exception e) {
            log.warn("", e);
        }
    }

    public static void configureClass(Object obj, Map props) {
        for (Iterator i = props.keySet().iterator(); i.hasNext();) {
            String key = (String)i.next();
            Object val = props.get(key);

            configureClass(obj, key, val);
        }
    }

    private static Method getPrimitiveMethod(Class objClass, String methodName, Object param) throws NoSuchMethodException {
        if (param instanceof Boolean) {
            try {
                // Try using the primitive type first
                return objClass.getMethod(methodName, new Class[] {boolean.class});
            } catch (NoSuchMethodException e) {
                // Try using the wrapper class next
                return objClass.getMethod(methodName, new Class[] {Boolean.class});
            }
        } else if (param instanceof Integer) {
            try {
                // Try using the primitive type first
                return objClass.getMethod(methodName, new Class[] {int.class});
            } catch (NoSuchMethodException e) {
                // Try using the wrapper class next
                return objClass.getMethod(methodName, new Class[] {Integer.class});
            }
        } else if (param instanceof Long) {
            try {
                // Try using the primitive type first
                return objClass.getMethod(methodName, new Class[] {long.class});
            } catch (NoSuchMethodException e) {
                // Try using the wrapper class next
                return objClass.getMethod(methodName, new Class[] {Long.class});
            }
        } else if (param instanceof Short) {
            try {
                // Try using the primitive type first
                return objClass.getMethod(methodName, new Class[] {short.class});
            } catch (NoSuchMethodException e) {
                // Try using the wrapper class next
                return objClass.getMethod(methodName, new Class[] {Short.class});
            }
        } else if (param instanceof Byte) {
            try {
                // Try using the primitive type first
                return objClass.getMethod(methodName, new Class[] {byte.class});
            } catch (NoSuchMethodException e) {
                // Try using the wrapper class next
                return objClass.getMethod(methodName, new Class[] {Byte.class});
            }
        } else if (param instanceof Character) {
            try {
                // Try using the primitive type first
                return objClass.getMethod(methodName, new Class[] {char.class});
            } catch (NoSuchMethodException e) {
                // Try using the wrapper class next
                return objClass.getMethod(methodName, new Class[] {Character.class});
            }
        } else if (param instanceof Double) {
            try {
                // Try using the primitive type first
                return objClass.getMethod(methodName, new Class[] {double.class});
            } catch (NoSuchMethodException e) {
                // Try using the wrapper class next
                return objClass.getMethod(methodName, new Class[] {Double.class});
            }
        } else if (param instanceof Float) {
            try {
                // Try using the primitive type first
                return objClass.getMethod(methodName, new Class[] {float.class});
            } catch (NoSuchMethodException e) {
                // Try using the wrapper class next
                return objClass.getMethod(methodName, new Class[] {Float.class});
            }
        } else {
            // parameter is not a primitive
            return objClass.getMethod(methodName, new Class[] {param.getClass()});
        }
    }
}
