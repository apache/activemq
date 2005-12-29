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
package org.apache.activeio.command;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.lang.reflect.Proxy;

/**
 * An input stream which uses the {@link org.apache.activeio.command.ClassLoading} helper class
 *
 * @version $Revision: 1.1 $
 */
public class ClassLoadingAwareObjectInputStream extends ObjectInputStream {

    private static final ClassLoader myClassLoader = DefaultWireFormat.class.getClassLoader();

    public ClassLoadingAwareObjectInputStream(InputStream in) throws IOException {
        super(in);
    }

    protected Class resolveClass(ObjectStreamClass classDesc) throws IOException, ClassNotFoundException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        return loadClass(classDesc.getName(), classLoader);
    }

    protected Class resolveProxyClass(String[] interfaces) throws IOException, ClassNotFoundException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        Class[] interfaceClasses = new Class[interfaces.length];
        for (int i = 0; i < interfaces.length; i++) {
            interfaceClasses[i] = loadClass(interfaces[i], classLoader);
        }

        try {
            return Proxy.getProxyClass(interfaceClasses[0].getClassLoader(), interfaceClasses);
        }
        catch (IllegalArgumentException e) {
            throw new ClassNotFoundException(null, e);
        }
    }

    protected Class loadClass(String className, ClassLoader classLoader) throws ClassNotFoundException {
        try {
            return ClassLoading.loadClass(className, classLoader);
        }
        catch (ClassNotFoundException e) {
            return ClassLoading.loadClass(className, myClassLoader);
        }
    }

}
