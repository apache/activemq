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
package org.objectweb.jtests.jms.admin;

import javax.naming.Context;
import javax.naming.NamingException;

/**
 * Simple Administration interface.
 * <br />
 * JMS Provider has to implement this 
 * simple interface to be able to use the test suite.
 */
public interface Admin
{

   /**
    * Returns the name of the JMS Provider.
    *
    * @return name of the JMS Provider
    */
   public String getName();

   /** 
    * Returns an <code>Context</code> for the JMS Provider.
    *
    * @return an <code>Context</code> for the JMS Provider.
    */
   public Context createContext() throws NamingException;

   /** 
    * Creates a <code>ConnectionFactory</code> and makes it available 
    *from JNDI with name <code>name</code>.
    *
    * @since JMS 1.1
    * @param name JNDI name of the <code>ConnectionFactory</code>
    */
   public void createConnectionFactory(String name);

   /** 
    * Creates a <code>QueueConnectionFactory</code> and makes it available 
    *from JNDI with name <code>name</code>.
    *
    * @param name JNDI name of the <code>QueueConnectionFactory</code>
    */
   public void createQueueConnectionFactory(String name);

   /** 
    * Creates a <code>TopicConnectionFactory</code> and makes it available 
    *from JNDI with name <code>name</code>.
    *
    * @param name JNDI name of the <code>TopicConnectionFactory</code>
    */
   public void createTopicConnectionFactory(String name);

   /** 
    * Creates a <code>Queue</code> and makes it available 
    *from JNDI with name <code>name</code>.
    *
    * @param name JNDI name of the <code>Queue</code>
    */
   public void createQueue(String name);

   /** 
    * Creates a <code>Topic</code> and makes it available 
    *from JNDI with name <code>name</code>.
    *
    * @param name JNDI name of the <code>Topic</code>
    */
   public void createTopic(String name);

   /** 
    * Removes the <code>Queue</code> of name <code>name</code> from JNDI and deletes it
    *
    * @param name JNDI name of the <code>Queue</code>
    */
   public void deleteQueue(String name);

   /** 
    * Removes the <code>Topic</code> of name <code>name</code> from JNDI and deletes it
    *
    * @param name JNDI name of the <code>Topic</code>
    */
   public void deleteTopic(String name);

   /** 
    * Removes the <code>ConnectionFactory</code> of name <code>name</code> from JNDI and deletes it
    *
    * @since JMS 1.1
    * @param name JNDI name of the <code>ConnectionFactory</code>
    */
   public void deleteConnectionFactory(String name);

   /** 
    * Removes the <code>QueueConnectionFactory</code> of name <code>name</code> from JNDI and deletes it
    *
    * @param name JNDI name of the <code>QueueConnectionFactory</code>
    */
   public void deleteQueueConnectionFactory(String name);

   /** 
    * Removes the <code>TopicConnectionFactory</code> of name <code>name</code> from JNDI and deletes it
    *
    * @param name JNDI name of the <code>TopicConnectionFactory</code>
    */
   public void deleteTopicConnectionFactory(String name);

   /**
    * Optional method to start the server embedded (instead of running an external server)
    */
   public void startServer() throws Exception;

   /**
    * Optional method to stop the server embedded (instead of running an external server)
    */
   public void stopServer() throws Exception;

   /**
    * Optional method for processing to be made after the Admin is instantiated and before
    * it is used to create the administrated objects
    */
   void start() throws Exception;

   /**
    * Optional method for processing to be made after the administrated objects have been cleaned up
    */
   void stop() throws Exception;

}
