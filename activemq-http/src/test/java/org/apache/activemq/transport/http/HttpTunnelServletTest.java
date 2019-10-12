package org.apache.activemq.transport.http;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import javax.servlet.ReadListener;
import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.transport.TransportAcceptListener;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HttpTunnelServletTest {

    @Rule
    public final MockitoRule rule = MockitoJUnit.rule();

    @Mock
    private HttpServletRequest request;
    @Mock
    private HttpServletResponse response;

    private final MockServletOutputStream servletOutputStream = new MockServletOutputStream();

    @Before
    public void setup() throws IOException {
        when(response.getOutputStream()).thenReturn(servletOutputStream);
    }

    @Test
    public void testPreservesAsymmetricalMarshalling() throws Exception {
        final AtomicReference<Object> commandRef = new AtomicReference<>();

        final BlockingQueueTransport transportChannel = newTransportChannel(commandRef);

        final HttpTunnelServlet httpTunnelServlet = newServlet(transportChannel);

        httpTunnelServlet.doGet(request, response); //marshall

        final String wireFormatMessage = servletOutputStream.getContent();
        assertThat(wireFormatMessage, not(CoreMatchers.startsWith("<")));

        final String message = toTextMessage(wireFormatMessage);
        when(request.getInputStream()).thenReturn(new MockServletInputStream(message));
        httpTunnelServlet.doPost(request, response); //unmarshallText
        assertThat(commandRef.get(), CoreMatchers.instanceOf(ConsumerInfo.class));
    }

    private HttpTunnelServlet newServlet(final BlockingQueueTransport transportChannel) throws ServletException {
        final HttpTunnelServlet httpTunnelServlet = new HttpTunnelServlet() {
            @Override
            protected BlockingQueueTransport getTransportChannel(final HttpServletRequest request, final HttpServletResponse response) {
                return transportChannel;
            }
        };
        final ServletConfig servletConfig = mock(ServletConfig.class);

        final ServletContext servletContext = mockServletContext();
        when(servletConfig.getServletContext()).thenReturn(servletContext);
        httpTunnelServlet.init(servletConfig);
        return httpTunnelServlet;
    }

    private BlockingQueueTransport newTransportChannel(final AtomicReference<Object> commandRef) {
        final BlockingQueueTransport transportChannel = new BlockingQueueTransport(new ArrayBlockingQueue<>(10)) {
            @Override
            public void doConsume(final Object command) {
                commandRef.set(command);
            }
        };
        transportChannel.getQueue().offer(new ConsumerInfo());
        return transportChannel;
    }

    private ServletContext mockServletContext() {
        final ServletContext servletContext = mock(ServletContext.class);
        final TransportAcceptListener acceptListener = mock(TransportAcceptListener.class);
        when(servletContext.getAttribute(eq("acceptListener"))).thenReturn(acceptListener);
        when(servletContext.getAttribute(eq("transportFactory"))).thenReturn(new HttpTransportFactory());
        return servletContext;
    }

    private String toTextMessage(final String message) {
        return message.substring(message.indexOf('<'));
    }

    private static class MockServletOutputStream extends ServletOutputStream {
        private final StringBuilder sb = new StringBuilder();

        @Override
        public boolean isReady() {
            return false;
        }

        @Override
        public void setWriteListener(final WriteListener writeListener) {
        }

        @Override
        public void write(final int b) throws IOException {
            sb.append((char)b);
        }

        public String getContent() {
            final String s = sb.toString();
            sb.setLength(0);
            return s;
        }
    }

    private class MockServletInputStream extends ServletInputStream {
        private final String string;
        private int pos;

        private MockServletInputStream(final String message) {
            string = message;
        }

        @Override
        public boolean isFinished() {
            return pos==string.length();
        }

        @Override
        public boolean isReady() {
            return false;
        }

        @Override
        public void setReadListener(final ReadListener readListener) {
        }

        @Override
        public int read() throws IOException {
            return isFinished() ? -1 : string.charAt(pos++);
        }
    }
}