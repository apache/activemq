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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.UnknownServiceException;
import java.nio.file.Path;
import java.security.Security;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 *
 */
public class FactoryFinder<T> {

    /**
     * The strategy that the FactoryFinder uses to find load and instantiate Objects
     * can be changed out by calling the
     * {@link org.apache.activemq.util.FactoryFinder#setObjectFactory(org.apache.activemq.util.FactoryFinder.ObjectFactory)}
     * method with a custom implementation of ObjectFactory.
     *
     * The default ObjectFactory is typically changed out when running in a specialized container
     * environment where service discovery needs to be done via the container system.  For example,
     * in an OSGi scenario.
     */
    public interface ObjectFactory {
        /**
         * @param path the full service path
         * @return
         */
        Object create(String path) throws IllegalAccessException, InstantiationException, IOException, ClassNotFoundException;

        /**
         * This method loads objects by searching for classes in the given path.
         * A requiredType and Set of allowed implementations are provided for
         * implementations to use for validation. Note is up to the actual implementations
         * that implement {@link ObjectFactory} to decide how to use both parameters.
         * By default, the method just delegates to {@link #create(String)}
         *
         * @param path the full service path
         * @param requiredType the requiredType any objects must implement
         * @param allowedImpls The set of allowed impls
         * @return
         */
        @SuppressWarnings("unchecked")
        default <T> T create(String path, Class<T> requiredType, Set<Class<? extends T>> allowedImpls)
                throws IllegalAccessException, InstantiationException, IOException, ClassNotFoundException {
            return (T) create(path);
        }
    }

    /**
     * The default implementation of Object factory which works well in standalone applications.
     */
    protected static class StandaloneObjectFactory implements ObjectFactory {
        final ConcurrentMap<String, Class<?>> classMap = new ConcurrentHashMap<>();

        @Override
        public Object create(final String path)
                throws IllegalAccessException, InstantiationException, IOException, ClassNotFoundException {
            throw new UnsupportedOperationException("Create is not supported without requiredType and allowed impls");
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> T create(String path, Class<T> requiredType, Set<Class<? extends T>> allowedImpls) throws InstantiationException, IllegalAccessException, ClassNotFoundException, IOException {
            Class<?> clazz = classMap.get(path);
            if (clazz == null) {
                clazz = loadClass(loadProperties(path));
                // no reason to cache if invalid so validate before caching
                validateClass(clazz, requiredType, allowedImpls);
                classMap.put(path, clazz);
            } else {
                // Validate again (even for previously cached classes) in case
                // a path is re-used with a different requiredType.
                // This object factory is static and shared by all factory finder instances by default,
                // so it would be possible (although probably a mistake) to use the same
                // path again with a different requiredType in a different FactoryFinder
                validateClass(clazz, requiredType, allowedImpls);
            }

            try {
               return (T) clazz.getConstructor().newInstance();
            } catch (NoSuchMethodException | InvocationTargetException e) {
               throw new InstantiationException(e.getMessage());
            }
        }

        static Class<?> loadClass(Properties properties) throws ClassNotFoundException, IOException {
            return FactoryFinder.loadClass(properties.getProperty("class"));
        }

        static public Properties loadProperties(String uri) throws IOException {
            // lets try the thread context class loader first
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            if (classLoader == null) {
                classLoader = StandaloneObjectFactory.class.getClassLoader();
            }
            InputStream in = classLoader.getResourceAsStream(uri);
            if (in == null) {
                in = FactoryFinder.class.getClassLoader().getResourceAsStream(uri);
                if (in == null) {
                    throw new IOException("Could not find factory class for resource: " + uri);
                }
            }

            // lets load the file
            BufferedInputStream reader = null;
            try {
                reader = new BufferedInputStream(in);
                Properties properties = new Properties();
                properties.load(reader);
                return properties;
            } finally {
                try {
                    reader.close();
                } catch (Exception e) {
                }
            }
        }
    }

    // ================================================================
    // Class methods and properties
    // ================================================================
    private static ObjectFactory objectFactory = new StandaloneObjectFactory();

    public static ObjectFactory getObjectFactory() {
        return objectFactory;
    }

    public static void setObjectFactory(ObjectFactory objectFactory) {
        FactoryFinder.objectFactory = objectFactory;
    }

    // ================================================================
    // Instance methods and properties
    // ================================================================
    private final String path;
    private final Class<T> requiredType;
    private final Set<Class<? extends T>> allowedImpls;

    /**
     *
     * @param path The path to search for impls
     * @param requiredType Required interface type that any impl must implement
     * @param allowedImpls The list of allowed implementations. If null or asterisk
     * then all impls of the requiredType are allowed.
     */
    public FactoryFinder(String path, Class<T> requiredType, String allowedImpls) {
        this.path = Objects.requireNonNull(path);
        this.requiredType = Objects.requireNonNull(requiredType);
        this.allowedImpls = loadAllowedImpls(requiredType, allowedImpls);
    }

    @SuppressWarnings("unchecked")
    private static <T> Set<Class<? extends T>> loadAllowedImpls(Class<T> requiredType, String allowedImpls) {
        // If allowedImpls is either null or an asterisk (allow all wild card) then set to null so we don't filter
        // If allowedImpls is only an empty string we return an empty set meaning allow none
        // Otherwise split/trim all values
        return allowedImpls != null && !allowedImpls.equals("*") ?
            Arrays.stream(allowedImpls.split("\\s*,\\s*"))
                .filter(s -> !s.isEmpty())
                .map(s -> {
                    try {
                        final Class<?> clazz = FactoryFinder.loadClass(s);
                        if (!requiredType.isAssignableFrom(clazz)) {
                            throw new IllegalArgumentException(
                                    "Class " + clazz + " is not assignable to " + requiredType);
                        }
                        return (Class<? extends T>)clazz;
                    } catch (ClassNotFoundException | IOException e) {
                        throw new IllegalArgumentException(e);
                    }
                }).collect(Collectors.toUnmodifiableSet()) : null;
    }


    /**
     * Creates a new instance of the given key
     *
     * @param key is the key to add to the path to find a text file containing
     *                the factory name
     * @return a newly created instance
     */
    public T newInstance(String key) throws IllegalAccessException, InstantiationException, IOException, ClassNotFoundException {
        return objectFactory.create(resolvePath(key), requiredType, allowedImpls);
    }

    Set<Class<? extends T>> getAllowedImpls() {
        return allowedImpls;
    }

    Class<T> getRequiredType() {
        return requiredType;
    }

    private String resolvePath(final String key) throws InstantiationException {
        // Normalize the base path with the given key. This
        // will resolve/remove any relative ".." sections of the path.
        // Example: "/dir1/dir2/dir3/../file" becomes "/dir1/dir2/file"
        final Path resolvedPath = Path.of(path).resolve(key).normalize();

        // Validate the resolved path is still within the original defined
        // root path and throw an error of it is not.
        if (!resolvedPath.startsWith(path)) {
            throw new InstantiationException("Provided key escapes the FactoryFinder configured directory");
        }

        return resolvedPath.toString();
    }

    public static String buildAllowedImpls(Class<?>...classes) {
        return Arrays.stream(Objects.requireNonNull(classes, "List of allowed classes may not be null"))
                .map(Class::getName).collect(Collectors.joining(","));
    }

    public static <T> void validateClass(Class<?> clazz, Class<T> requiredType,
            Set<Class<? extends T>> allowedImpls) throws InstantiationException {
        // Validate the loaded class is an allowed impl
        if (allowedImpls != null && !allowedImpls.contains(clazz)) {
            throw new InstantiationException("Class " + clazz + " is not an allowed implementation "
                    + "of type " + requiredType);
        }
        // Validate the loaded class is a subclass of the right type
        // The allowedImpls may not be used so also check requiredType. Even if set
        // generics can be erased and this is an extra safety check
        if (!requiredType.isAssignableFrom(clazz)) {
            throw new InstantiationException("Class " + clazz + " is not assignable to " + requiredType);
        }
    }

    static Class<?> loadClass(String className) throws ClassNotFoundException, IOException {
        if (className == null) {
            throw new IOException("Expected property is missing: class");
        }
        Class<?> clazz = null;
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        if (loader != null) {
            try {
                clazz = loader.loadClass(className);
            } catch (ClassNotFoundException e) {
                // ignore
            }
        }
        if (clazz == null) {
            clazz = FactoryFinder.class.getClassLoader().loadClass(className);
        }

        return clazz;
    }

}
