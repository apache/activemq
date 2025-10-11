package org.apache.activemq.web.filter;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpFilter;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.activemq.web.SessionFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CSRFFilter extends HttpFilter {

    private static final Logger LOG = LoggerFactory.getLogger(CSRFFilter.class);

    @Override
    public void doFilter(HttpServletRequest request, HttpServletResponse response, FilterChain chain) throws IOException, ServletException {
        final Object sessionSecret = request.getSession().getAttribute(SessionFilter.SESSION_SECRET_ATTRIBUTE);
        if (sessionSecret == null || !sessionSecret.equals(request.getParameter("secret"))) {
            throw new UnsupportedOperationException("Possible CSRF attack");
        }

        LOG.info("CSRF Token validated successfully");

        chain.doFilter(request, response);
    }
}
