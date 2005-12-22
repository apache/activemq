/**
 *
 * Copyright 2004 The Apache Software Foundation
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
package org.activecluster;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import javax.jms.JMSException;
import org.activecluster.impl.ActiveMQClusterFactory;

/**
 * @version $Revision: 1.2 $
 */
public class ChatDemo implements ClusterListener {
    private Cluster cluster;
    private String name = "unknown";

    public static void main(String[] args) {
        try {
            ChatDemo test = new ChatDemo();
            test.run();
        }
        catch (JMSException e) {
            System.out.println("Caught: " + e);
            e.printStackTrace();
            Exception c = e.getLinkedException();
            if (c != null) {
                System.out.println("Cause: " + c);
                c.printStackTrace();
            }
        }
        catch (Exception e) {
            System.out.println("Caught: " + e);
            e.printStackTrace();
        }
    }

    public void run() throws Exception {
        cluster = createCluster();
        cluster.addClusterListener(this);
        cluster.start();


        System.out.println();
        System.out.println();
        System.out.println("Welcome to the ActiveCluster Chat Demo!");
        System.out.println();
        System.out.println("Enter text to talk or type");
        System.out.println("  /quit      to terminate the application");
        System.out.println("  /name foo  to change your name to be 'foo'");

        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        boolean running = true;
        while (running) {
            String line = reader.readLine();
            if (line == null || line.trim().equalsIgnoreCase("quit")) {
                break;
            }
            else {
                running = processCommand(line.trim());
            }
        }
        stop();
    }

    protected boolean processCommand(String text) throws JMSException {
        if (text.equals("/quit")) {
            return false;
        }
        else {
            if (text.startsWith("/name")) {
                name = text.substring(5).trim();
                System.out.println("* name now changed to: " + name);
            }
            else {
                // lets talk
                Map map = new HashMap();
                map.put("text", text);
                map.put("name", name);
                cluster.getLocalNode().setState(map);
            }
            return true;
        }
    }


    public void onNodeAdd(ClusterEvent event) {
        System.out.println("* " + getName(event) + " has joined the room");
    }

    public void onNodeUpdate(ClusterEvent event) {
        System.out.println(getName(event) + "> " + getText(event));
    }

    public void onNodeRemoved(ClusterEvent event) {
        System.out.println("* " + getName(event) + " has left the room");
    }

    public void onNodeFailed(ClusterEvent event) {
        System.out.println("* " + getName(event) + " has failed unexpectedly");
    }
    
    public void onCoordinatorChanged(ClusterEvent event){
        
    }

    protected Object getName(ClusterEvent event) {
        return event.getNode().getState().get("name");
    }

    protected Object getText(ClusterEvent event) {
        return event.getNode().getState().get("text");
    }

    protected void stop() throws JMSException {
        cluster.stop();
    }

    protected Cluster createCluster() throws JMSException, ClusterException {
        ClusterFactory factory = new ActiveMQClusterFactory();
        return factory.createCluster("ORG.CODEHAUS.ACTIVEMQ.TEST.CLUSTER");
    }

}
