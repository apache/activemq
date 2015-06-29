package org.apache.activemq.transport.util;

import javax.servlet.http.HttpServletRequest;

public class HttpTransportUtils {

    public static String generateWsRemoteAddress(HttpServletRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("HttpServletRequest must not be null.");
        }

        StringBuilder remoteAddress = new StringBuilder();
        String scheme = request.getScheme();
        remoteAddress.append(scheme != null && scheme.toLowerCase().equals("https") ? "wss://" : "ws://");
        remoteAddress.append(request.getRemoteAddr());
        remoteAddress.append(":");
        remoteAddress.append(request.getRemotePort());
        return remoteAddress.toString();
    }
}
