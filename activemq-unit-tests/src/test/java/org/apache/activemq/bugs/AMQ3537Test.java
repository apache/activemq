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
package org.apache.activemq.bugs;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.util.ClassLoadingAwareObjectInputStream;
import org.junit.Before;
import org.junit.Test;

/**
 * Quick port to java to support AMQ build.
 *
 * This test demonstrates the classloader problem in the
 * ClassLoadingAwareObjectInputStream impl. If the first interface in the proxy
 * interfaces list is JDK and there are any subsequent interfaces that are NOT
 * JDK interfaces the ClassLoadingAwareObjectInputStream will ignore their
 * respective classloaders and cause the Proxy to throw an
 * IllegalArgumentException because the core JDK classloader can't load the
 * interfaces that are not JDK interfaces.
 *
 * See AMQ-3537
 *
 * @author jason.yankus
 *
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class AMQ3537Test implements InvocationHandler, Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * If the first and second element in this array are swapped, the test will
     * fail.
     */
    public static final Class[] TEST_CLASSES = new Class[] { List.class, NonJDKList.class, Serializable.class };

    /** Underlying list */
    private final List l = new ArrayList<String>();

    @Before
    public void setUp() throws Exception {
        l.add("foo");
    }

    @Test
    public void testDeserializeProxy() throws Exception {
        // create the proxy
        List proxy = (List) java.lang.reflect.Proxy.newProxyInstance(this.getClass().getClassLoader(), TEST_CLASSES, this);

        // serialize it
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(proxy);
        byte[] serializedProxy = baos.toByteArray();
        oos.close();
        baos.close();

        // deserialize the proxy
        ClassLoadingAwareObjectInputStream claois =
            new ClassLoadingAwareObjectInputStream(new ByteArrayInputStream(serializedProxy));

        // this is where it fails due to the rudimentary classloader selection
        // in ClassLoadingAwareObjectInputStream
        List deserializedProxy = (List) claois.readObject();

        claois.close();

        // assert the invocation worked
        assertEquals("foo", deserializedProxy.get(0));
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        return method.invoke(l, args);
    }

    public interface NonJDKList {
        int size();
    }
}
