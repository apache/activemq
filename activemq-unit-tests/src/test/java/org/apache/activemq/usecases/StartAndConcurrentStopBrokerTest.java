/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.usecases;

import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.InvalidAttributeValueException;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.NotCompliantMBeanException;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.OperationsException;
import javax.management.QueryExp;
import javax.management.ReflectionException;
import javax.management.loading.ClassLoaderRepository;
import org.apache.activemq.ConfigurationException;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerStoppedException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


public class StartAndConcurrentStopBrokerTest {
    private static final Logger LOG = LoggerFactory.getLogger(StartAndConcurrentStopBrokerTest.class);


    @Test(timeout = 30000)
    public void testConcurrentStop() throws Exception {

        final AtomicReference<Throwable> error = new AtomicReference<Throwable>();
        final CountDownLatch gotBrokerMbean = new CountDownLatch(1);
        final CountDownLatch gotPaMBean = new CountDownLatch(1);
        final AtomicBoolean checkPaMBean = new AtomicBoolean(false);

        final HashMap mbeans = new HashMap();
        final MBeanServer mBeanServer = new MBeanServer() {
            @Override
            public ObjectInstance createMBean(String className, ObjectName name) throws ReflectionException, InstanceAlreadyExistsException, MBeanRegistrationException, MBeanException, NotCompliantMBeanException {
                return null;
            }

            @Override
            public ObjectInstance createMBean(String className, ObjectName name, ObjectName loaderName) throws ReflectionException, InstanceAlreadyExistsException, MBeanRegistrationException, MBeanException, NotCompliantMBeanException, InstanceNotFoundException {
                return null;
            }

            @Override
            public ObjectInstance createMBean(String className, ObjectName name, Object[] params, String[] signature) throws ReflectionException, InstanceAlreadyExistsException, MBeanRegistrationException, MBeanException, NotCompliantMBeanException {
                return null;
            }

            @Override
            public ObjectInstance createMBean(String className, ObjectName name, ObjectName loaderName, Object[] params, String[] signature) throws ReflectionException, InstanceAlreadyExistsException, MBeanRegistrationException, MBeanException, NotCompliantMBeanException, InstanceNotFoundException {
                return null;
            }

            @Override
            public ObjectInstance registerMBean(Object object, ObjectName name) throws InstanceAlreadyExistsException, MBeanRegistrationException, NotCompliantMBeanException {
                if (mbeans.containsKey(name)) {
                    throw new InstanceAlreadyExistsException("Got one already: " + name);
                }
                LOG.info("register:" + name);

                try {
                    if (name.compareTo(new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost")) == 0) {
                        gotBrokerMbean.countDown();
                    }

                    if (checkPaMBean.get()) {
                        if (new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost,service=PersistenceAdapter,instanceName=*").apply(name)) {
                            gotPaMBean.countDown();
                        }
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                    error.set(e);
                }
                mbeans.put(name, object);
                return new ObjectInstance(name, object.getClass().getName());

            }

            @Override
            public void unregisterMBean(ObjectName name) throws InstanceNotFoundException, MBeanRegistrationException {
                mbeans.remove(name);
            }

            @Override
            public ObjectInstance getObjectInstance(ObjectName name) throws InstanceNotFoundException {
                return null;
            }

            @Override
            public Set<ObjectInstance> queryMBeans(ObjectName name, QueryExp query) {
                return null;
            }

            @Override
            public Set<ObjectName> queryNames(ObjectName name, QueryExp query) {
                return null;
            }

            @Override
            public boolean isRegistered(ObjectName name) {
                return mbeans.containsKey(name);
            }

            @Override
            public Integer getMBeanCount() {
                return null;
            }

            @Override
            public Object getAttribute(ObjectName name, String attribute) throws MBeanException, AttributeNotFoundException, InstanceNotFoundException, ReflectionException {
                return null;
            }

            @Override
            public AttributeList getAttributes(ObjectName name, String[] attributes) throws InstanceNotFoundException, ReflectionException {
                return null;
            }

            @Override
            public void setAttribute(ObjectName name, Attribute attribute) throws InstanceNotFoundException, AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException {

            }

            @Override
            public AttributeList setAttributes(ObjectName name, AttributeList attributes) throws InstanceNotFoundException, ReflectionException {
                return null;
            }

            @Override
            public Object invoke(ObjectName name, String operationName, Object[] params, String[] signature) throws InstanceNotFoundException, MBeanException, ReflectionException {
                return null;
            }

            @Override
            public String getDefaultDomain() {
                return null;
            }

            @Override
            public String[] getDomains() {
                return new String[0];
            }

            @Override
            public void addNotificationListener(ObjectName name, NotificationListener listener, NotificationFilter filter, Object handback) throws InstanceNotFoundException {

            }

            @Override
            public void addNotificationListener(ObjectName name, ObjectName listener, NotificationFilter filter, Object handback) throws InstanceNotFoundException {

            }

            @Override
            public void removeNotificationListener(ObjectName name, ObjectName listener) throws InstanceNotFoundException, ListenerNotFoundException {

            }

            @Override
            public void removeNotificationListener(ObjectName name, ObjectName listener, NotificationFilter filter, Object handback) throws InstanceNotFoundException, ListenerNotFoundException {

            }

            @Override
            public void removeNotificationListener(ObjectName name, NotificationListener listener) throws InstanceNotFoundException, ListenerNotFoundException {

            }

            @Override
            public void removeNotificationListener(ObjectName name, NotificationListener listener, NotificationFilter filter, Object handback) throws InstanceNotFoundException, ListenerNotFoundException {

            }

            @Override
            public MBeanInfo getMBeanInfo(ObjectName name) throws InstanceNotFoundException, IntrospectionException, ReflectionException {
                return null;
            }

            @Override
            public boolean isInstanceOf(ObjectName name, String className) throws InstanceNotFoundException {
                return false;
            }

            @Override
            public Object instantiate(String className) throws ReflectionException, MBeanException {
                return null;
            }

            @Override
            public Object instantiate(String className, ObjectName loaderName) throws ReflectionException, MBeanException, InstanceNotFoundException {
                return null;
            }

            @Override
            public Object instantiate(String className, Object[] params, String[] signature) throws ReflectionException, MBeanException {
                return null;
            }

            @Override
            public Object instantiate(String className, ObjectName loaderName, Object[] params, String[] signature) throws ReflectionException, MBeanException, InstanceNotFoundException {
                return null;
            }

            @Override
            public ObjectInputStream deserialize(ObjectName name, byte[] data) throws InstanceNotFoundException, OperationsException {
                return null;
            }

            @Override
            public ObjectInputStream deserialize(String className, byte[] data) throws OperationsException, ReflectionException {
                return null;
            }

            @Override
            public ObjectInputStream deserialize(String className, ObjectName loaderName, byte[] data) throws InstanceNotFoundException, OperationsException, ReflectionException {
                return null;
            }

            @Override
            public ClassLoader getClassLoaderFor(ObjectName mbeanName) throws InstanceNotFoundException {
                return null;
            }

            @Override
            public ClassLoader getClassLoader(ObjectName loaderName) throws InstanceNotFoundException {
                return null;
            }

            @Override
            public ClassLoaderRepository getClassLoaderRepository() {
                return null;
            }
        };


        final BrokerService broker = new BrokerService();
        broker.setDeleteAllMessagesOnStartup(true);

        ExecutorService executor = Executors.newFixedThreadPool(4);
        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    broker.getManagementContext().setMBeanServer(mBeanServer);
                    broker.start();
                } catch (BrokerStoppedException expected) {
                } catch (ConfigurationException expected) {
                } catch (Exception e) {
                    e.printStackTrace();
                    // lots of possible errors depending on progress
                }
            }
        });


        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    assertTrue("broker has registered mbean", gotBrokerMbean.await(10, TimeUnit.SECONDS));
                    broker.stop();
                } catch (Exception e) {
                    e.printStackTrace();
                    error.set(e);
                }
            }
        });

        executor.shutdown();
        assertTrue("stop tasks done", executor.awaitTermination(20, TimeUnit.SECONDS));

        BrokerService sanityBroker = new BrokerService();
        sanityBroker.getManagementContext().setMBeanServer(mBeanServer);
        sanityBroker.start();
        sanityBroker.stop();

        assertNull("No error", error.get());

        // again, after Persistence adapter mbean
        final BrokerService brokerTwo = new BrokerService();
        broker.setDeleteAllMessagesOnStartup(true);

        checkPaMBean.set(true);
        executor = Executors.newFixedThreadPool(4);
        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    brokerTwo.getManagementContext().setMBeanServer(mBeanServer);
                    brokerTwo.start();
                } catch (BrokerStoppedException expected) {
                } catch (ConfigurationException expected) {
                } catch (Exception e) {
                    e.printStackTrace();
                    // lots of possible errors depending on progress
                }
            }
        });

        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    assertTrue("broker has registered persistence adapter mbean", gotPaMBean.await(10, TimeUnit.SECONDS));
                    brokerTwo.stop();
                } catch (Exception e) {
                    e.printStackTrace();
                    error.set(e);
                }
            }
        });

        executor.shutdown();
        assertTrue("stop tasks done", executor.awaitTermination(20, TimeUnit.SECONDS));

        assertTrue("broker has registered persistence adapter mbean", gotPaMBean.await(0, TimeUnit.SECONDS));

        sanityBroker = new BrokerService();
        sanityBroker.getManagementContext().setMBeanServer(mBeanServer);
        sanityBroker.start();
        sanityBroker.stop();

        assertNull("No error", error.get());

    }

}
