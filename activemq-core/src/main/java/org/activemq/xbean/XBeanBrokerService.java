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
