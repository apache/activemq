package org.apache.activemq.spring;

import java.util.Map;

import org.apache.activemq.broker.BrokerContext;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

public class SpringBrokerContext implements BrokerContext, ApplicationContextAware {

    ApplicationContext applicationContext;
    
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    public Object getBean(String name) {
        try {
            return applicationContext.getBean(name);
        } catch (BeansException ex) {
            return null;
        }
    }

    public Map getBeansOfType(Class type) {
        return applicationContext.getBeansOfType(type);
    }

}
