package org.apache.activemq.transport.util;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import javax.servlet.http.HttpServletRequest;

import org.junit.Test;

public class HttpTransportUtilsTest {

    @Test
    public void testGenerateWsRemoteAddress() {
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getScheme()).thenReturn("http");
        when(request.getRemoteAddr()).thenReturn("localhost");
        when(request.getRemotePort()).thenReturn(8080);

        assertEquals("ws://localhost:8080", HttpTransportUtils.generateWsRemoteAddress(request));
    }

    @Test
    public void testGenerateWssRemoteAddress() {
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getScheme()).thenReturn("https");
        when(request.getRemoteAddr()).thenReturn("localhost");
        when(request.getRemotePort()).thenReturn(8443);

        assertEquals("wss://localhost:8443", HttpTransportUtils.generateWsRemoteAddress(request));
    }

    @Test(expected=IllegalArgumentException.class)
    public void testNullHttpServleRequest() {
        HttpTransportUtils.generateWsRemoteAddress(null);
    }
}
