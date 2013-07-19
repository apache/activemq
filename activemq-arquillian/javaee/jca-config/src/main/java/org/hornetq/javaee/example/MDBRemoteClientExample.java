/*
 * Copyright 2009 Red Hat, Inc.
 *  Red Hat licenses this file to you under the Apache License, version
 *  2.0 (the "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied.  See the License for the specific language governing
 *  permissions and limitations under the License.
 */
package org.hornetq.javaee.example;

import org.hornetq.javaee.example.server2.StatelessSenderService;

import javax.jms.Connection;
import javax.naming.Context;
import javax.naming.InitialContext;
import java.util.Properties;

/**
 *
 * MDB Remote & JCA Configuration Example.
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public class MDBRemoteClientExample
{
   public static void main(String[] args) throws Exception
   {
      Connection connection = null;
      InitialContext initialContext = null;
      try
      {
         final Properties env = new Properties();

         env.put(Context.INITIAL_CONTEXT_FACTORY, "org.jboss.naming.remote.client.InitialContextFactory");

         env.put(Context.PROVIDER_URL, "remote://localhost:4547");

         env.put(Context.SECURITY_PRINCIPAL, "guest");

         env.put(Context.SECURITY_CREDENTIALS, "password");
         env.put("jboss.naming.client.ejb.context", true);

env.put("jboss.naming.client.connect.options.org.xnio.Options.SASL_POLICY_NOPLAINTEXT", "false");
         //Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = new InitialContext(env);

         //Step 2. Getting a reference to the Stateless Bean
         StatelessSenderService sender = (StatelessSenderService)initialContext.lookup("mdb2/StatelessSender!org.hornetq.javaee.example.server2.StatelessSenderService");

         //Step 3. Calling a Stateless Session Bean. You will have more steps on the SessionBean
         sender.sendHello("Hello there MDB!");

         System.out.println("Step 3: Invoking the Stateless Bean");

         initialContext.close();
      }
      finally
      {
         //Step 11. Be sure to close our JMS resources!
         if (initialContext != null)
         {
            initialContext.close();
         }
         if(connection != null)
         {
            connection.close();
         }
      }
   }
}
