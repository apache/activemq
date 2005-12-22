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

import org.activecluster.impl.ActiveMQClusterFactory;
import org.activecluster.impl.DefaultClusterFactory;
import org.activecluster.election.ElectionStrategy;
import org.activecluster.election.impl.BullyElectionStrategy;

import javax.jms.JMSException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * @version $Revision: 1.2 $
 */
public class ClusterDemo {
    protected Cluster cluster;
    private String name;
    private ElectionStrategy electionStrategy;

    public static void main(String[] args) {
        try {
            ClusterDemo test = new ClusterDemo();
            if (args.length > 0) {
                test.name = args[0];
            }
            test.demo();
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

    public void demo() throws Exception {
        start();

        cluster.addClusterListener(new TestingClusterListener(cluster));

        System.out.println("Enter 'quit' to terminate");
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            String line = reader.readLine();
            if (line == null || line.trim().equalsIgnoreCase("quit")) {
                break;
            }
            else {
                Map map = new HashMap();
                map.put("text", line);
                cluster.getLocalNode().setState(map);
            }
        }
        stop();
    }


    protected void start() throws JMSException, ClusterException {
        cluster = createCluster();
        if (name != null) {
            System.out.println("Starting node: " + name);

            // TODO could we do cluster.setName() ?
            Map state = new HashMap();
            state.put("name", name);
            cluster.getLocalNode().setState(state);
        }
        cluster.start();
        if (electionStrategy == null) {
            electionStrategy = new BullyElectionStrategy();
        }
        electionStrategy.doElection(cluster);
    }

    protected void stop() throws JMSException {
        cluster.stop();
    }

    protected Cluster createCluster() throws JMSException, ClusterException {
        ClusterFactory factory = new ActiveMQClusterFactory();
        return factory.createCluster("ORG.CODEHAUS.ACTIVEMQ.TEST.CLUSTER");
    }
}
