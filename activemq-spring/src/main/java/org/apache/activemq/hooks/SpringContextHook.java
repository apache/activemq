package org.apache.activemq.hooks;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.osgi.framework.BundleException;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.osgi.context.support.OsgiBundleXmlApplicationContext;

public class SpringContextHook implements Runnable, ApplicationContextAware {

    private static final transient Log LOG = LogFactory.getLog(SpringContextHook.class);
    ApplicationContext applicationContext;
    
    public void run() {
        if (applicationContext instanceof ConfigurableApplicationContext) {
            ((ConfigurableApplicationContext) applicationContext).close();
        }
        if (applicationContext instanceof OsgiBundleXmlApplicationContext){
            try {
                ((OsgiBundleXmlApplicationContext)applicationContext).getBundle().stop();
            } catch (BundleException e) {
                LOG.info("Error stopping OSGi bundle " + e, e);
            }
        }

    }

    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

}
