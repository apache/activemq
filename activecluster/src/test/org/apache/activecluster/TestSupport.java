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
 * 
 **/
package org.apache.activecluster;

import junit.framework.TestCase;

import javax.jms.JMSException;
import java.util.Iterator;
import java.util.Map;

import org.apache.activecluster.Cluster;
import org.apache.activecluster.ClusterException;
import org.apache.activecluster.ClusterFactory;
import org.apache.activecluster.Node;
import org.apache.activecluster.impl.ActiveMQClusterFactory;

/**
 * @version $Revision: 1.2 $
 */
public class TestSupport extends TestCase {
    protected String dumpConnectedNodes(Map nodes) {
        String result = "";
        for (Iterator i = nodes.values().iterator(); i.hasNext();) {

            Object value = i.next();
            if (value instanceof Node) {
                Node node = (Node) value;
                result += node.getName() + ",";
            }
            else {
                System.out.println("Got node of type: " + value.getClass());
                result += value + ",";
            }
        }
        return result;
    }

    protected Cluster createCluster() throws JMSException, ClusterException {
        ClusterFactory factory = new ActiveMQClusterFactory();
        return factory.createCluster("ORG.CODEHAUS.ACTIVEMQ.TEST.CLUSTER");
    }

    protected Cluster createCluster(String name) throws JMSException, ClusterException {
        ClusterFactory factory = new ActiveMQClusterFactory();
        return factory.createCluster(name,"ORG.CODEHAUS.ACTIVEMQ.TEST.CLUSTER");
    }
}
