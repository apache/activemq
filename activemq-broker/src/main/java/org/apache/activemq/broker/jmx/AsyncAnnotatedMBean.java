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
package org.apache.activemq.broker.jmx;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.ReflectionException;

/**
 * MBean that invokes the requested operation using an async operation and waits for the result
 * if the operation times out then an exception is thrown.
 */
public class AsyncAnnotatedMBean extends AnnotatedMBean {

    private ExecutorService executor;
    private long timeout = 0;

    public <T> AsyncAnnotatedMBean(ExecutorService executor, long timeout, T impl, Class<T> mbeanInterface, ObjectName objectName) throws NotCompliantMBeanException {
        super(impl, mbeanInterface, objectName);

        this.executor = executor;
        this.timeout = timeout;
    }

    protected AsyncAnnotatedMBean(Class<?> mbeanInterface, ObjectName objectName) throws NotCompliantMBeanException {
        super(mbeanInterface, objectName);
    }

    protected Object asyncInvole(String s, Object[] objects, String[] strings) throws MBeanException, ReflectionException {
        return super.invoke(s, objects, strings);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static ObjectInstance registerMBean(ExecutorService executor, long timeout, ManagementContext context, Object object, ObjectName objectName) throws Exception {

        if (timeout < 0 && executor != null) {
            throw new IllegalArgumentException("async timeout cannot be negative.");
        }

        if (timeout > 0 && executor == null) {
            throw new NullPointerException("timeout given but no ExecutorService instance given.");
        }

        String mbeanName = object.getClass().getName() + "MBean";

        for (Class c : object.getClass().getInterfaces()) {
            if (mbeanName.equals(c.getName())) {
                if (timeout == 0) {
                    return context.registerMBean(new AnnotatedMBean(object, c, objectName), objectName);
                } else {
                    return context.registerMBean(new AsyncAnnotatedMBean(executor, timeout, object, c, objectName), objectName);
                }
            }
        }

        return context.registerMBean(object, objectName);
    }

    @Override
    public Object invoke(String s, Object[] objects, String[] strings) throws MBeanException, ReflectionException {

        final String action = s;
        final Object[] params = objects;
        final String[] signature = strings;

        Future<Object> task = executor.submit(new Callable<Object>() {

            @Override
            public Object call() throws Exception {
                return asyncInvole(action, params, signature);
            }
        });

        try {
            return task.get(timeout, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof MBeanException) {
                throw (MBeanException) e.getCause();
            }

            throw new MBeanException(e);
        } catch (Exception e) {
            throw new MBeanException(e);
        } finally {
            if (!task.isDone()) {
                task.cancel(true);
            }
        }
    }
}
