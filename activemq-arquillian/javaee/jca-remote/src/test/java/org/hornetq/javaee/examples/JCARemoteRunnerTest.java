/*
* JBoss, Home of Professional Open Source.
* Copyright 2010, Red Hat, Inc., and individual contributors
* as indicated by the @author tags. See the copyright.txt file in the
* distribution for a full listing of individual contributors.
*
* This is free software; you can redistribute it and/or modify it
* under the terms of the GNU Lesser General Public License as
* published by the Free Software Foundation; either version 2.1 of
* the License, or (at your option) any later version.
*
* This software is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
* Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public
* License along with this software; if not, write to the Free
* Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
* 02110-1301 USA, or see the FSF site: http://www.fsf.org.
*/
package org.hornetq.javaee.examples;

import org.apache.activemq.broker.BrokerService;
import org.hornetq.javaee.example.MDBRemoteServerClientExample;
import org.hornetq.javaee.example.server.MDBQueue;
import org.jboss.arquillian.container.test.api.ContainerController;
import org.jboss.arquillian.container.test.api.Deployer;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.RunAsClient;
import org.jboss.arquillian.container.test.api.TargetsContainer;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.junit.InSequence;
import org.jboss.arquillian.test.api.ArquillianResource;
//import org.jboss.osgi.testing.ManifestBuilder;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.Asset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.InputStream;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         5/21/12
 */
@RunAsClient
@RunWith(Arquillian.class)
//@ServerSetup({ExampleRunner2Test.JmsQueueSetup.class})
public class JCARemoteRunnerTest
{
   BrokerService brokerService = null;
   @ArquillianResource
   private ContainerController controller;
   @ArquillianResource
   private Deployer deployer;

   @Deployment(name = "deploy-0", managed = false)
   @TargetsContainer("node-0")
   public static Archive getDeployment()
   {

      final JavaArchive ejbJar = ShrinkWrap.create(JavaArchive.class, "mdb.jar");
      ejbJar.addClass(MDBQueue.class);
//      ejbJar.setManifest(new Asset()
//      {
//         public InputStream openStream()
//         {
//            ManifestBuilder builder = ManifestBuilder.newInstance();
//            StringBuffer dependencies = new StringBuffer();
//            dependencies.append("org.hornetq");
//            builder.addManifestHeader("Dependencies", dependencies.toString());
//            return builder.openStream();
//         }
//      });
      System.out.println(ejbJar.toString(true));
      return ejbJar;
   }

   @Test
   public void runExample() throws Exception
   {
      MDBRemoteServerClientExample.main(null);
   }

   @Test
   @InSequence(-1)
   public void startServer()
   {
      startAmq();
      System.out.println("*****************************************************************************************************************************************************************");
      controller.start("node-1");
      System.out.println("*****************************************************************************************************************************************************************");
      controller.start("node-0");
      System.out.println("*****************************************************************************************************************************************************************");
      deployer.deploy("deploy-0");
   }

    // let the remote instance be one we start here
    private void startAmq() {
        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.setUseJmx(false);
        try {
            brokerService.addConnector("tcp://0.0.0.0:61616");
            brokerService.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

   @Test
   @InSequence(1)
   public void stopServer()
   {
      deployer.undeploy("deploy-0");
      controller.stop("node-0");
      controller.stop("node-1");
       try {
           brokerService.stop();
       } catch (Exception e) {
           e.printStackTrace();
       }
   }

}
