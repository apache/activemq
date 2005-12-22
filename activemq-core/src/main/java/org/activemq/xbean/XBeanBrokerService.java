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
package org.activemq.xbean;

import org.activemq.broker.BrokerService;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

/**
 * Represents a running broker service which consists of a number of transport
 * connectors, network connectors and a bunch of properties which can be used to
 * configure the broker as its lazily created.
 * 
 * @org.xbean.XBean element="broker" rootElement="true" description="An ActiveMQ
 *                  Message Broker which consists of a number of transport
 *                  connectors, network connectors and a persistence adaptor"
 * 
 * @version $Revision: 1.1 $
 */
public class XBeanBrokerService extends BrokerService implements InitializingBean, DisposableBean {

    private boolean start = true;

    public XBeanBrokerService() {
    }

    public void afterPropertiesSet() throws Exception {
        if (start) {
            start();
        }
    }

    public void destroy() throws Exception {
        stop();
    }

    public boolean isStart() {
        return start;
    }

    /**
     * Sets whether or not the broker is started along with the ApplicationContext it is defined within.
     * Normally you would want the broker to start up along with the ApplicationContext but sometimes when working
     * with JUnit tests you may wish to start and stop the broker explicitly yourself.
     */
    public void setStart(boolean start) {
        this.start = start;
    }
}
