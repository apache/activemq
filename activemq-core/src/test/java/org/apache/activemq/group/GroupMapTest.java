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
package org.apache.activemq.group;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;


public class GroupMapTest extends TestCase {
    protected BrokerService broker;
    protected Connection connection;
    protected String bindAddress = ActiveMQConnectionFactory.DEFAULT_BROKER_BIND_URL;

    /**
     * Test method for
     * {@link org.apache.activemq.group.GroupMap#addMemberChangedListener(org.apache.activemq.group.MemberChangedListener)}.
     * @throws Exception 
     */
    public void testAddMemberChangedListener() throws Exception {
        final AtomicInteger counter = new AtomicInteger();
        GroupMap map1 = new GroupMap(connection,"map1");
        map1.addMemberChangedListener(new MemberChangedListener(){

            public void memberStarted(Member member) {
                synchronized(counter) {
                    counter.incrementAndGet();
                    counter.notifyAll();
                }
                
            }

            public void memberStopped(Member member) {
                synchronized(counter) {
                    counter.decrementAndGet();
                    counter.notifyAll();
                }
            }
            
        });
        map1.start();
        synchronized(counter) {
            if (counter.get()<1) {
                counter.wait(5000);
            }
        }
        assertEquals(1, counter.get());
        ConnectionFactory factory = createConnectionFactory();
        Connection connection2 = factory.createConnection();
        connection2.start();
        GroupMap map2 = new GroupMap(connection,"map2");
        map2.start();
        synchronized(counter) {
            if (counter.get()<2) {
                counter.wait(5000);
            }
        }
        assertEquals(2, counter.get());
        map2.stop();
        synchronized(counter) {
            if (counter.get()>=2) {
                counter.wait(5000);
            }
        }
        assertEquals(1, counter.get());
        map1.stop();
        connection2.close();
    }

    /**
     * Test method for
     * {@link org.apache.activemq.group.GroupMap#addMapChangedListener(org.apache.activemq.group.MapChangedListener)}.
     * @throws Exception 
     */
    public void testAddMapChangedListener() throws Exception {
        GroupMap map = new GroupMap(connection,"map");
        final AtomicBoolean called = new AtomicBoolean();
        map.addMapChangedListener(new MapChangedListener(){   
            public void mapChanged(Member owner, Object key, Object oldValue,
                    Object newValue) {       
                synchronized(called) {
                    called.set(true);
                    called.notifyAll();
                }
            }
            
        });
        map.start();
        map.put("test", "blob");
        synchronized(called) {
            if (!called.get()) {
               called.wait(5000); 
            }
        }
        assertTrue(called.get());
        map.stop();
    }

    /**
     * Test method for {@link org.apache.activemq.group.GroupMap#clear()}.
     * @throws Exception 
     */
    public void testClear() throws Exception {
        GroupMap map1 = new GroupMap(connection,"map1");
        final AtomicBoolean called = new AtomicBoolean();
        map1.addMapChangedListener(new MapChangedListener(){   
            public void mapChanged(Member owner, Object key, Object oldValue,
                    Object newValue) {       
                synchronized(called) {
                    called.set(true);
                    called.notifyAll();
                }
            }
            
        });
        map1.start();
        GroupMap map2 = new GroupMap(connection,"map2");
        map2.start();
        map2.put("test","foo");
        synchronized(called) {
            if (!called.get()) {
               called.wait(5000); 
            }
        }
        assertTrue(called.get());
        called.set(false);
        assertTrue(map1.isEmpty()==false);
        map2.clear();
        synchronized(called) {
            if (!called.get()) {
               called.wait(5000); 
            }
        }
        assertTrue(map1.isEmpty());
        map1.stop();
        map2.stop();
    }

    /**
     * Test a new map is populated for existing values
     */
    public void testMapUpdatedOnStart() throws Exception {
        GroupMap map1 = new GroupMap(connection,"map1");
        final AtomicBoolean called = new AtomicBoolean();
        
        map1.start();
        map1.put("test", "foo");
        GroupMap map2 = new GroupMap(connection,"map2");
        map2.addMapChangedListener(new MapChangedListener(){   
            public void mapChanged(Member owner, Object key, Object oldValue,
                    Object newValue) {       
                synchronized(called) {
                    called.set(true);
                    called.notifyAll();
                }
            }   
        });
        map2.start();
       
        synchronized(called) {
            if (!called.get()) {
               called.wait(5000); 
            }
        }
        assertTrue(called.get());
        called.set(false);
        assertTrue(map2.containsKey("test"));
        assertTrue(map2.containsValue("foo"));
        map1.stop();
        map2.stop();
    }
    
    public void testContainsKey() throws Exception {
        GroupMap map1 = new GroupMap(connection,"map1");
        final AtomicBoolean called = new AtomicBoolean();
        map1.addMapChangedListener(new MapChangedListener(){   
            public void mapChanged(Member owner, Object key, Object oldValue,
                    Object newValue) {       
                synchronized(called) {
                    called.set(true);
                    called.notifyAll();
                }
            }
            
        });
        map1.start();
        GroupMap map2 = new GroupMap(connection,"map2");
        map2.start();
        map2.put("test","foo");
        synchronized(called) {
            if (!called.get()) {
               called.wait(5000); 
            }
        }
        assertTrue(called.get());
        called.set(false);
        assertTrue(map1.containsKey("test"));
        map1.stop();
        map2.stop();
    }


    /**
     * Test method for
     * {@link org.apache.activemq.group.GroupMap#containsValue(java.lang.Object)}.
     * @throws Exception 
     */
    public void testContainsValue() throws Exception {
        GroupMap map1 = new GroupMap(connection,"map1");
        final AtomicBoolean called = new AtomicBoolean();
        map1.addMapChangedListener(new MapChangedListener(){   
            public void mapChanged(Member owner, Object key, Object oldValue,
                    Object newValue) {       
                synchronized(called) {
                    called.set(true);
                    called.notifyAll();
                }
            }
            
        });
        map1.start();
        GroupMap map2 = new GroupMap(connection,"map2");
        map2.start();
        map2.put("test","foo");
        synchronized(called) {
            if (!called.get()) {
               called.wait(5000); 
            }
        }
        assertTrue(called.get());
        called.set(false);
        assertTrue(map1.containsValue("foo"));
        map1.stop();
        map2.stop();
    }

    /**
     * Test method for {@link org.apache.activemq.group.GroupMap#entrySet()}.
     * @throws Exception 
     */
    

    /**
     * Test method for
     * {@link org.apache.activemq.group.GroupMap#get(java.lang.Object)}.
     * @throws Exception 
     */
    public void testGet() throws Exception {
        GroupMap map1 = new GroupMap(connection,"map1");
        final AtomicBoolean called = new AtomicBoolean();
        map1.addMapChangedListener(new MapChangedListener(){   
            public void mapChanged(Member owner, Object key, Object oldValue,
                    Object newValue) {       
                synchronized(called) {
                    called.set(true);
                    called.notifyAll();
                }
            }
            
        });
        map1.start();
        GroupMap map2 = new GroupMap(connection,"map2");
        map2.start();
        map2.put("test","foo");
        synchronized(called) {
            if (!called.get()) {
               called.wait(5000); 
            }
        }
        assertTrue(called.get());
        assertTrue(map1.get("test").equals("foo"));
        map1.stop();
        map2.stop();
    }

    
    
    /**
     * Test method for
     * {@link org.apache.activemq.group.GroupMap#remove(java.lang.Object)}.
     */
    public void testRemove() throws Exception{
        GroupMap map1 = new GroupMap(connection,"map1");
        final AtomicBoolean called = new AtomicBoolean();
        map1.addMapChangedListener(new MapChangedListener(){   
            public void mapChanged(Member owner, Object key, Object oldValue,
                    Object newValue) {       
                synchronized(called) {
                    called.set(true);
                    called.notifyAll();
                }
            }
            
        });
        map1.start();
        GroupMap map2 = new GroupMap(connection,"map2");
        map2.start();
        map2.put("test","foo");
        synchronized(called) {
            if (!called.get()) {
               called.wait(5000); 
            }
        }
        assertTrue(called.get());
        called.set(false);
        assertTrue(map1.isEmpty()==false);
        map2.remove("test");
        synchronized(called) {
            if (!called.get()) {
               called.wait(5000); 
            }
        }
        assertTrue(map1.isEmpty());
        map1.stop();
        map2.stop();
    }

    protected void setUp() throws Exception {
        if (broker == null) {
            broker = createBroker();
        }
        ConnectionFactory factory = createConnectionFactory();
        connection = factory.createConnection();
        connection.start();
        super.setUp();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        connection.close();
        if (broker != null) {
            broker.stop();
        }
    }

    protected ActiveMQConnectionFactory createConnectionFactory()throws Exception {
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(
                ActiveMQConnection.DEFAULT_BROKER_URL);
        return cf;
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService answer = new BrokerService();
        configureBroker(answer);
        answer.start();
        return answer;
    }

    protected void configureBroker(BrokerService answer) throws Exception {
        answer.setPersistent(false);
        answer.addConnector(bindAddress);
        answer.setDeleteAllMessagesOnStartup(true);
    }
}
