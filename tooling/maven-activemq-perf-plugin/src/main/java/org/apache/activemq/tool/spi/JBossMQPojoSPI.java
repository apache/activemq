package org.apache.activemq.tool.spi;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.naming.InitialContext;
import javax.naming.Context;
import javax.naming.NamingException;
import java.util.Properties;


public class JBossMQPojoSPI extends ClassLoaderSPIConnectionFactory {
    public static final String KEY_BROKER_URL = "brokerUrl";
    public static final String DEFAULT_URL = "jnp://localhost:1099";
    public static final String NAMING_CONTEXT = "org.jnp.interfaces.NamingContextFactory";
    public static final String JNP_INTERFACES = "org.jnp.interfaces";


    protected ConnectionFactory instantiateConnectionFactory(Properties settings) throws Exception {
        InitialContext context = getInitialContext(settings);
        ConnectionFactory factory = (ConnectionFactory) context.lookup("ConnectionFactory");
        return factory;
    }

    public void configureConnectionFactory(ConnectionFactory jmsFactory, Properties settings) throws Exception {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public InitialContext getInitialContext(Properties settings) throws Exception {
        String url = settings.getProperty(KEY_BROKER_URL);

        Properties properties = new Properties();
        properties.put(Context.INITIAL_CONTEXT_FACTORY, NAMING_CONTEXT);
        properties.put(Context.URL_PKG_PREFIXES, JNP_INTERFACES);

        if (url != null && url.length() > 0) {
            properties.put(Context.PROVIDER_URL, url);
        } else {
            properties.put(Context.PROVIDER_URL, DEFAULT_URL);
        }

        try {
            return new InitialContext(properties);
        } catch (NamingException e) {
            throw new JMSException("Error creating InitialContext ", e.toString());
        }
    }
}
