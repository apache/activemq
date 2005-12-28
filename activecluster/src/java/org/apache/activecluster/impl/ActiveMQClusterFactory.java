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
package org.apache.activecluster.impl;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activecluster.impl.DefaultClusterFactory;

/**
 * An implementation of {@link org.apache.activecluster.ClusterFactory} using
 * <a href="http://activemq.codehaus.org/">ActiveMQ</a>
 *
 * @version $Revision: 1.4 $
 */
public class ActiveMQClusterFactory extends DefaultClusterFactory {
 
    public static String DEFAULT_CLUSTER_URL = "peer://org.apache.activecluster?persistent=false";

    public ActiveMQClusterFactory() {
        super(new ActiveMQConnectionFactory(DEFAULT_CLUSTER_URL));
    }

    public ActiveMQClusterFactory(String brokerURL) {
        super(new ActiveMQConnectionFactory(brokerURL));
    }

    public ActiveMQClusterFactory(ActiveMQConnectionFactory connectionFactory) {
        super(connectionFactory);
    }

    public ActiveMQClusterFactory(boolean transacted, int acknowledgeMode, String dataTopicPrefix, long inactiveTime) {
        super(new ActiveMQConnectionFactory(DEFAULT_CLUSTER_URL), transacted, acknowledgeMode, dataTopicPrefix, inactiveTime);
    }

    public ActiveMQClusterFactory(ActiveMQConnectionFactory connectionFactory, boolean transacted, int acknowledgeMode, String dataTopicPrefix, long inactiveTime) {
        super(connectionFactory, transacted, acknowledgeMode, dataTopicPrefix, inactiveTime);
    }

    public ActiveMQConnectionFactory getActiveMQConnectionFactory() {
        return (ActiveMQConnectionFactory) getConnectionFactory();
    }

    public void setActiveMQConnectionFactory(ActiveMQConnectionFactory connectionFactory) {
        setConnectionFactory(connectionFactory);
    }

}
