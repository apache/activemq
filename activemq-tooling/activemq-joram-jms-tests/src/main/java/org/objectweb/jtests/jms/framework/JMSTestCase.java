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
package org.objectweb.jtests.jms.framework;

import java.io.IOException;
import java.util.Properties;

import jakarta.jms.JMSException;

import junit.framework.TestCase;

import jakarta.jms.Connection;

import org.objectweb.jtests.jms.admin.Admin;
import org.objectweb.jtests.jms.admin.AdminFactory;

/**
 * Class extending <code>junit.framework.TestCase</code> to
 * provide a new <code>fail()</code> method with an <code>Exception</code>
 * as parameter.
 *<br />
 * Every Test Case for JMS should extend this class instead of <code>junit.framework.TestCase</code>
 *
 *
 * @version $Id: JMSTestCase.java,v 1.2 2007/07/19 21:20:08 csuconic Exp $
 */
public abstract class JMSTestCase extends TestCase
{
   public static final String PROP_FILE_NAME = "provider.properties";

   public static boolean startServer = true;

   protected Admin admin;

   /**
    * Fails a test with an exception which will be used for a message.
    *
    * If the exception is an instance of <code>jakarta.jms.JMSException</code>, the
    * message of the failure will contained both the JMSException and its linked exception
    * (provided there's one).
    */
   public void fail(final Exception e)
   {
      if (e instanceof jakarta.jms.JMSException)
      {
         JMSException exception = (JMSException)e;
         String message = e.toString();
         Exception linkedException = exception.getLinkedException();
         if (linkedException != null)
         {
            message += " [linked exception: " + linkedException + "]";
         }
         super.fail(message);
      }
      else
      {
         super.fail(e.getMessage());
      }
   }

   public JMSTestCase(final String name)
   {
      super(name);
   }

   /**
    * Should be overriden
    * @return
    */
   protected Properties getProviderProperties() throws IOException
   {
      Properties props = new Properties();
      props.load(getClass().getClassLoader().getResourceAsStream(System.getProperty("joram.jms.test.file", JMSTestCase.PROP_FILE_NAME)));
      return props;
   }

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      // Admin step
      // gets the provider administration wrapper...
      Properties props = getProviderProperties();
      admin = AdminFactory.getAdmin(props);

      if (startServer)
      {
         admin.startServer();
      }
      admin.start();
   }

   /**
    * A clean up step. Each step runs on its own so one failure does not skip the rest.
    */
   protected interface CleanUpStep
   {
      void run() throws Exception;
   }

   /**
    * Runs a clean up step and reports a failure without letting it stop the other steps.
    */
   protected void runQuietly(final CleanUpStep step)
   {
      try
      {
         step.run();
      }
      catch (Exception e)
      {
         System.err.println(getName() + " clean up step failed: " + e);
      }
   }

   /**
    * Closes a connection if it was created, reporting but not propagating a failure.
    */
   protected void closeQuietly(final Connection connection)
   {
      if (connection != null)
      {
         runQuietly(connection::close);
      }
   }

   @Override
   protected void tearDown() throws Exception
   {
      try
      {
         admin.stop();

         if (startServer)
         {
            admin.stopServer();
         }
      }
      finally
      {
         super.tearDown();
      }
   }

}
