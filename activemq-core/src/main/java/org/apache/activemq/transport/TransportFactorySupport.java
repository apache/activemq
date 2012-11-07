package org.apache.activemq.transport;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerServiceAware;
import org.apache.activemq.broker.SslContext;

import java.io.IOException;
import java.net.URI;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class TransportFactorySupport {

    public static TransportServer bind(BrokerService brokerService, URI location) throws IOException {
        TransportFactory tf = TransportFactory.findTransportFactory(location);
        if( brokerService!=null && tf instanceof BrokerServiceAware) {
            ((BrokerServiceAware)tf).setBrokerService(brokerService);
        }
        try {
            if( brokerService!=null ) {
                SslContext.setCurrentSslContext(brokerService.getSslContext());
            }
            return tf.doBind(location);
        } finally {
            SslContext.setCurrentSslContext(null);
        }
    }

}
