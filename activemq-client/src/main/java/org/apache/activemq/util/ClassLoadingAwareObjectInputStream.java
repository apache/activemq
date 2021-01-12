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

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.lang.reflect.Proxy;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClassLoadingAwareObjectInputStream extends ObjectInputStream {

    private static final Logger LOG = LoggerFactory.getLogger(ClassLoadingAwareObjectInputStream.class);
    private static final ClassLoader FALLBACK_CLASS_LOADER =
        ClassLoadingAwareObjectInputStream.class.getClassLoader();

    public static final String[] serializablePackages;

    private List<String> trustedPackages = new ArrayList<String>();
    private boolean trustAllPackages = false;

    private final ClassLoader inLoader;

    static {
        serializablePackages = System.getProperty("org.apache.activemq.SERIALIZABLE_PACKAGES","java.lang,org.apache.activemq,org.fusesource.hawtbuf,com.thoughtworks.xstream.mapper").split(",");
    }

    public ClassLoadingAwareObjectInputStream(InputStream in) throws IOException {
        super(in);
        inLoader = in.getClass().getClassLoader();
        trustedPackages.addAll(Arrays.asList(serializablePackages));
    }

    @Override
    protected Class<?> resolveClass(ObjectStreamClass classDesc) throws IOException, ClassNotFoundException {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        Class clazz = load(classDesc.getName(), cl, inLoader);
        checkSecurity(clazz);
        return clazz;
    }

    @Override
    protected Class<?> resolveProxyClass(String[] interfaces) throws IOException, ClassNotFoundException {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        Class[] cinterfaces = new Class[interfaces.length];
        for (int i = 0; i < interfaces.length; i++) {
            cinterfaces[i] = load(interfaces[i], cl);
        }

        Class clazz = null;
        try {
            clazz = Proxy.getProxyClass(cl, cinterfaces);
        } catch (IllegalArgumentException e) {
            try {
                clazz = Proxy.getProxyClass(inLoader, cinterfaces);
            } catch (IllegalArgumentException e1) {
                // ignore
            }
            try {
                clazz = Proxy.getProxyClass(FALLBACK_CLASS_LOADER, cinterfaces);
            } catch (IllegalArgumentException e2) {
                // ignore
            }
        }

        if (clazz != null) {
            checkSecurity(clazz);
            return clazz;
        } else {
            throw new ClassNotFoundException(null);
        }
    }

    public static boolean isAllAllowed() {
        return serializablePackages.length == 1 && serializablePackages[0].equals("*");
    }

    private boolean trustAllPackages() {
        return trustAllPackages || (trustedPackages.size() == 1 && trustedPackages.get(0).equals("*"));
    }

    private void checkSecurity(Class clazz) throws ClassNotFoundException {
        if (trustAllPackages() || clazz.isPrimitive()) {
            return;
        }

        boolean found = false;
        Package thePackage = clazz.getPackage();
        if (thePackage != null) {
            for (String trustedPackage : getTrustedPackages()) {
                if (thePackage.getName().equals(trustedPackage) || thePackage.getName().startsWith(trustedPackage + ".")) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                throw new ClassNotFoundException("Forbidden " + clazz + "! This class is not trusted to be serialized as ObjectMessage payload. Please take a look at http://activemq.apache.org/objectmessage.html for more information on how to configure trusted classes.");
            }
        }
    }

    private Class<?> load(String className, ClassLoader... cl) throws ClassNotFoundException {
        // check for simple types first
        final Class<?> clazz = loadSimpleType(className);
        if (clazz != null) {
            LOG.trace("Loaded class: {} as simple type -> {}", className, clazz);
            return clazz;
        }

        // try the different class loaders
        for (ClassLoader loader : cl) {
            LOG.trace("Attempting to load class: {} using classloader: {}", className, cl);
            try {
                Class<?> answer = Class.forName(className, false, loader);
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Loaded class: {} using classloader: {} -> {}", className, cl, answer);
                }
                return answer;
            } catch (ClassNotFoundException e) {
                LOG.trace("Class not found: {} using classloader: {}", className, cl);
                // ignore
            }
        }

        // and then the fallback class loader
        return Class.forName(className, false, FALLBACK_CLASS_LOADER);
    }

    /**
     * Load a simple type
     *
     * @param name the name of the class to load
     * @return the class or <tt>null</tt> if it could not be loaded
     */
    public static Class<?> loadSimpleType(String name) {
        // code from ObjectHelper.loadSimpleType in Apache Camel

        // special for byte[] or Object[] as its common to use
        if ("java.lang.byte[]".equals(name) || "byte[]".equals(name)) {
            return byte[].class;
        } else if ("java.lang.Byte[]".equals(name) || "Byte[]".equals(name)) {
            return Byte[].class;
        } else if ("java.lang.Object[]".equals(name) || "Object[]".equals(name)) {
            return Object[].class;
        } else if ("java.lang.String[]".equals(name) || "String[]".equals(name)) {
            return String[].class;
            // and these is common as well
        } else if ("java.lang.String".equals(name) || "String".equals(name)) {
            return String.class;
        } else if ("java.lang.Boolean".equals(name) || "Boolean".equals(name)) {
            return Boolean.class;
        } else if ("boolean".equals(name)) {
            return boolean.class;
        } else if ("java.lang.Integer".equals(name) || "Integer".equals(name)) {
            return Integer.class;
        } else if ("int".equals(name)) {
            return int.class;
        } else if ("java.lang.Long".equals(name) || "Long".equals(name)) {
            return Long.class;
        } else if ("long".equals(name)) {
            return long.class;
        } else if ("java.lang.Short".equals(name) || "Short".equals(name)) {
            return Short.class;
        } else if ("short".equals(name)) {
            return short.class;
        } else if ("java.lang.Byte".equals(name) || "Byte".equals(name)) {
            return Byte.class;
        } else if ("byte".equals(name)) {
            return byte.class;
        } else if ("java.lang.Float".equals(name) || "Float".equals(name)) {
            return Float.class;
        } else if ("float".equals(name)) {
            return float.class;
        } else if ("java.lang.Double".equals(name) || "Double".equals(name)) {
            return Double.class;
        } else if ("double".equals(name)) {
            return double.class;
        } else if ("void".equals(name)) {
            return void.class;
        }

        return null;
    }

    public List<String> getTrustedPackages() {
        return trustedPackages;
    }

    public void setTrustedPackages(List<String> trustedPackages) {
        this.trustedPackages = trustedPackages;
    }

    public void addTrustedPackage(String trustedPackage) {
        this.trustedPackages.add(trustedPackage);
    }

    public boolean isTrustAllPackages() {
        return trustAllPackages;
    }

    public void setTrustAllPackages(boolean trustAllPackages) {
        this.trustAllPackages = trustAllPackages;
    }
}
