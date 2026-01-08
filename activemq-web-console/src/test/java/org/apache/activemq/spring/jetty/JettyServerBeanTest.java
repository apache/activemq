package org.apache.activemq.spring.jetty;

import org.apache.xbean.spring.context.ClassPathXmlApplicationContext;
import org.eclipse.jetty.server.Server;
import org.junit.Test;

public class JettyServerBeanTest {

    @Test
    public void testJettyServerBean() throws Exception {
        var context = new ClassPathXmlApplicationContext("conf/jetty-spring.xml");
        context.start();

        var jettyServer = (Server) context.getBean("jettyServer");
        // assertNotNull(jettyServer);
        context.stop();
    }
}
