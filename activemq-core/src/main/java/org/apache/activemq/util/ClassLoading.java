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

import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.Map;

/**
 * Utilities for loading classes.
 * 
 * @version $Rev: 109957 $ $Date$
 */
public class ClassLoading {

    /**
     * Load a class for the given name. <p/>
     * <p>
     * Handles loading primitive types as well as VM class and array syntax.
     * 
     * @param className
     *            The name of the Class to be loaded.
     * @param classLoader
     *            The class loader to load the Class object from.
     * @return The Class object for the given name.
     * @throws ClassNotFoundException
     *             Failed to load Class object.
     */
    public static Class loadClass(final String className, final ClassLoader classLoader) throws ClassNotFoundException {
        if (className == null) {
            throw new IllegalArgumentException("className is null");
        }

        // First just try to load
        try {
            return load(className, classLoader);
        } catch (ClassNotFoundException ignore) {
            // handle special cases below
        }

        Class type = null;

        // Check if it is a primitive type
        type = getPrimitiveType(className);
        if (type != null)
            return type;

        // Check if it is a vm primitive
        type = getVMPrimitiveType(className);
        if (type != null)
            return type;

        // Handle VM class syntax (Lclassname;)
        if (className.charAt(0) == 'L' && className.charAt(className.length() - 1) == ';') {
            String name = className.substring(1, className.length() - 1);
            return load(name, classLoader);
        }

        // Handle VM array syntax ([type)
        if (className.charAt(0) == '[') {
            int arrayDimension = className.lastIndexOf('[') + 1;
            String componentClassName = className.substring(arrayDimension, className.length());
            type = loadClass(componentClassName, classLoader);

            int dim[] = new int[arrayDimension];
            java.util.Arrays.fill(dim, 0);
            return Array.newInstance(type, dim).getClass();
        }

        // Handle user friendly type[] syntax
        if (className.endsWith("[]")) {
            // get the base component class name and the arrayDimensions
            int arrayDimension = 0;
            String componentClassName = className;
            while (componentClassName.endsWith("[]")) {
                componentClassName = componentClassName.substring(0, componentClassName.length() - 2);
                arrayDimension++;
            }

            // load the base type
            type = loadClass(componentClassName, classLoader);

            // return the array type
            int[] dim = new int[arrayDimension];
            java.util.Arrays.fill(dim, 0);
            return Array.newInstance(type, dim).getClass();
        }

        // Else we can not load (give up)
        throw new ClassNotFoundException(className);
    }

    private static Class load(final String className, final ClassLoader classLoader) throws ClassNotFoundException {
        if (classLoader == null)
            return Class.forName(className);
        else
            return classLoader.loadClass(className);
    }

    public static String getClassName(Class clazz) {
        StringBuffer rc = new StringBuffer();
        while (clazz.isArray()) {
            rc.append('[');
            clazz = clazz.getComponentType();
        }
        if (!clazz.isPrimitive()) {
            rc.append('L');
            rc.append(clazz.getName());
            rc.append(';');
        } else {
            rc.append(VM_PRIMITIVES_REVERSE.get(clazz));
        }
        return rc.toString();
    }

    /**
     * Primitive type name -> class map.
     */
    private static final Map PRIMITIVES = new HashMap();

    /** Setup the primitives map. */
    static {
        PRIMITIVES.put("boolean", Boolean.TYPE);
        PRIMITIVES.put("byte", Byte.TYPE);
        PRIMITIVES.put("char", Character.TYPE);
        PRIMITIVES.put("short", Short.TYPE);
        PRIMITIVES.put("int", Integer.TYPE);
        PRIMITIVES.put("long", Long.TYPE);
        PRIMITIVES.put("float", Float.TYPE);
        PRIMITIVES.put("double", Double.TYPE);
        PRIMITIVES.put("void", Void.TYPE);
    }

    /**
     * Get the primitive type for the given primitive name.
     * 
     * @param name
     *            Primitive type name (boolean, byte, int, ...)
     * @return Primitive type or null.
     */
    private static Class getPrimitiveType(final String name) {
        return (Class) PRIMITIVES.get(name);
    }

    /**
     * VM primitive type name -> primitive type
     */
    private static final HashMap VM_PRIMITIVES = new HashMap();

    /** Setup the vm primitives map. */
    static {
        VM_PRIMITIVES.put("B", byte.class);
        VM_PRIMITIVES.put("C", char.class);
        VM_PRIMITIVES.put("D", double.class);
        VM_PRIMITIVES.put("F", float.class);
        VM_PRIMITIVES.put("I", int.class);
        VM_PRIMITIVES.put("J", long.class);
        VM_PRIMITIVES.put("S", short.class);
        VM_PRIMITIVES.put("Z", boolean.class);
        VM_PRIMITIVES.put("V", void.class);
    }

    /**
     * VM primitive type primitive type -> name
     */
    private static final HashMap VM_PRIMITIVES_REVERSE = new HashMap();

    /** Setup the vm primitives reverse map. */
    static {
        VM_PRIMITIVES_REVERSE.put(byte.class, "B");
        VM_PRIMITIVES_REVERSE.put(char.class, "C");
        VM_PRIMITIVES_REVERSE.put(double.class, "D");
        VM_PRIMITIVES_REVERSE.put(float.class, "F");
        VM_PRIMITIVES_REVERSE.put(int.class, "I");
        VM_PRIMITIVES_REVERSE.put(long.class, "J");
        VM_PRIMITIVES_REVERSE.put(short.class, "S");
        VM_PRIMITIVES_REVERSE.put(boolean.class, "Z");
        VM_PRIMITIVES_REVERSE.put(void.class, "V");
    }

    /**
     * Get the primitive type for the given VM primitive name. <p/>
     * <p>
     * Mapping:
     * 
     * <pre>
     * 
     *    B - byte
     *    C - char
     *    D - double
     *    F - float
     *    I - int
     *    J - long
     *    S - short
     *    Z - boolean
     *    V - void
     *  
     * </pre>
     * 
     * @param name
     *            VM primitive type name (B, C, J, ...)
     * @return Primitive type or null.
     */
    private static Class getVMPrimitiveType(final String name) {
        return (Class) VM_PRIMITIVES.get(name);
    }

    /**
     * Map of primitive types to their wrapper classes
     */
    private static final Map PRIMITIVE_WRAPPERS = new HashMap();

    /** Setup the wrapper map. */
    static {
        PRIMITIVE_WRAPPERS.put(Boolean.TYPE, Boolean.class);
        PRIMITIVE_WRAPPERS.put(Byte.TYPE, Byte.class);
        PRIMITIVE_WRAPPERS.put(Character.TYPE, Character.class);
        PRIMITIVE_WRAPPERS.put(Double.TYPE, Double.class);
        PRIMITIVE_WRAPPERS.put(Float.TYPE, Float.class);
        PRIMITIVE_WRAPPERS.put(Integer.TYPE, Integer.class);
        PRIMITIVE_WRAPPERS.put(Long.TYPE, Long.class);
        PRIMITIVE_WRAPPERS.put(Short.TYPE, Short.class);
        PRIMITIVE_WRAPPERS.put(Void.TYPE, Void.class);
    }
}
