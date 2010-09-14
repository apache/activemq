package org.apache.activemq.web;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.servlet.http.HttpSessionEvent;
import javax.servlet.http.HttpSessionListener;


public class SessionListener implements HttpSessionListener {
    private static final Log LOG = LogFactory.getLog(SessionListener.class);

    public void sessionCreated(HttpSessionEvent se) {
    }

    public void sessionDestroyed(HttpSessionEvent se) {
        WebClient client = WebClient.getWebClient(se.getSession());
        if (client != null) {
            client.close();
        }
    }
}
