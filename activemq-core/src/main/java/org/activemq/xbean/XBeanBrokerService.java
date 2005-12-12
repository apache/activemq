package org.activemq.xbean;

import org.activemq.broker.BrokerService;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.support.AbstractApplicationContext;

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
public class XBeanBrokerService extends BrokerService implements InitializingBean {

    private boolean start=false;
    private AbstractApplicationContext applicationContext;

    public XBeanBrokerService() {        
    }
    
    public void setAbstractApplicationContext(AbstractApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
    
    public void afterPropertiesSet() throws Exception {
        if( start ) {  
            start();
        }
    }

    public void stop() throws Exception {
        super.stop();
        if( applicationContext!=null ) {
            applicationContext.destroy();
            applicationContext=null;
        }
    }

    public boolean isStart() {
        return start;
    }
    public void setStart(boolean start) {
        this.start = start;
    }
    
}
