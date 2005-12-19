/**
 * 
 * Copyright 2004 Hiram Chirino
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
 * 
 **/
package org.activeio.oneport.openorb;

import java.rmi.RemoteException;
import java.util.Properties;

import javax.naming.NamingException;

import org.activeio.oneport.OnePortAsyncChannelServerTest;
import org.omg.CORBA.ORB;
import org.omg.CORBA.Object;
import org.omg.PortableServer.POA;
import org.omg.PortableServer.POAHelper;

import edu.emory.mathcs.backport.java.util.concurrent.BlockingQueue;

/**
 * 
 */
public class OpenORBOnePortSocketFactoryTest extends OnePortAsyncChannelServerTest {

    static public BlockingQueue staticResultSlot;
    private ORB orb;
    private String serverRef;
    private TestIIOPServerImpl testIIOPServer;
    private POA rootPOA;

    protected void startIIOPServer() throws Exception {
        staticResultSlot = resultSlot;
        
        Properties props = new Properties();        
        props.setProperty("org.omg.PortableInterceptor.ORBInitializerClass."+OpenORBOpenPortFeatureInitializer.class.getName(), "");
        props.setProperty("org.omg.CORBA.ORBClass", "org.openorb.orb.core.ORB");
        props.setProperty("org.omg.CORBA.ORBSingletonClass", "org.openorb.orb.core.ORBSingleton"); 

        OpenORBOpenPortFeatureInitializer.setContextSocketFactory(new OpenORBOpenPortSocketFactory(server));	        
        orb = ORB.init( new String[]{}, props );
        OpenORBOpenPortFeatureInitializer.setContextSocketFactory(null);
        
        rootPOA = POAHelper.narrow( orb.resolve_initial_references( "RootPOA" ) );

        TestIIOPServerImpl srv = new TestIIOPServerImpl();
        serverRef = orb.object_to_string( srv._this( orb ) );
        rootPOA.the_POAManager().activate();
        new Thread(){
            public void run() {
	            orb.run();
            }
        }.start();
    }
    
    protected void stopIIOPServer() throws Exception {
        orb.shutdown(true);
    }
    
    protected void hitIIOPServer( ) throws NamingException, RemoteException
    {
        // Create a client side orb.
        Properties props = new Properties();        
        props.setProperty("org.omg.CORBA.ORBClass", "org.openorb.orb.core.ORB");
        props.setProperty("org.omg.CORBA.ORBSingletonClass", "org.openorb.orb.core.ORBSingleton"); 
        ORB orb = ORB.init( new String[]{}, props );
        
        Object obj = orb.string_to_object( serverRef );
        TestIIOPServer srv = TestIIOPServerHelper.narrow( obj );
        try {
            srv.test();
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            orb.shutdown(true);
        }
    }
                    
}
