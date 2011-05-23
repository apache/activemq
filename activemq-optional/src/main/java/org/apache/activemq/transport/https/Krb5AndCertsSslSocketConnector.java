/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.activemq.transport.https;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.security.Principal;
import java.util.List;
import java.util.Collections;
import java.util.Random;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSocket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.eclipse.jetty.http.HttpSchemes;
import org.eclipse.jetty.io.EndPoint;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.ssl.ServletSSL;
import org.eclipse.jetty.server.ssl.SslSocketConnector;

/**
 * Extend Jetty's {@link SslSocketConnector} to optionally also provide 
 * Kerberos5ized SSL sockets.  The only change in behavior from superclass
 * is that we no longer honor requests to turn off NeedAuthentication when
 * running with Kerberos support.
 */
public class Krb5AndCertsSslSocketConnector extends SslSocketConnector {
  public static final List<String> KRB5_CIPHER_SUITES = 
    Collections.unmodifiableList(Collections.singletonList(
          "TLS_KRB5_WITH_3DES_EDE_CBC_SHA"));
  static {
    System.setProperty("https.cipherSuites", KRB5_CIPHER_SUITES.get(0));
  }
  
  private static final Logger LOG = LoggerFactory.getLogger(Krb5AndCertsSslSocketConnector.class);

  private static final String REMOTE_PRINCIPAL = "remote_principal";

  public enum MODE {KRB, CERTS, BOTH} // Support Kerberos, certificates or both?

  private boolean useKrb;
  private boolean useCerts;

  public Krb5AndCertsSslSocketConnector() {
	  // By default, stick to cert based authentication
    super();
    useKrb = false;
    useCerts = true;
    setPasswords();
  }
  
  public void setMode(String mode) {
    useKrb = mode == MODE.KRB.toString() || mode == MODE.BOTH.toString();
    useCerts = mode == MODE.CERTS.toString() || mode == MODE.BOTH.toString();
    logIfDebug("useKerb = " + useKrb + ", useCerts = " + useCerts);
  }

  // If not using Certs, set passwords to random gibberish or else
  // Jetty will actually prompt the user for some.
  private void setPasswords() {
   if(!useCerts) {
     Random r = new Random();
     System.setProperty("jetty.ssl.password", String.valueOf(r.nextLong()));
     System.setProperty("jetty.ssl.keypassword", String.valueOf(r.nextLong()));
   }
  }
  
  @Override
  protected SSLServerSocketFactory createFactory() throws Exception {
    if(useCerts)
      return super.createFactory();
    
    SSLContext context = super.getProvider()==null
       ? SSLContext.getInstance(super.getProtocol())
        :SSLContext.getInstance(super.getProtocol(), super.getProvider());
    context.init(null, null, null);
    
    System.err.println("Creating socket factory");
    return context.getServerSocketFactory();
  }
  
  /* (non-Javadoc)
   * @see org.mortbay.jetty.security.SslSocketConnector#newServerSocket(java.lang.String, int, int)
   */
  @Override
  protected ServerSocket newServerSocket(String host, int port, int backlog)
      throws IOException {
	  System.err.println("Creating new KrbServerSocket for: " + host);
    logIfDebug("Creating new KrbServerSocket for: " + host);
    SSLServerSocket ss = null;
    
    if(useCerts) // Get the server socket from the SSL super impl
      ss = (SSLServerSocket)super.newServerSocket(host, port, backlog);
    else { // Create a default server socket
      try {
        ss = (SSLServerSocket)(host == null 
         ? createFactory().createServerSocket(port, backlog) :
           createFactory().createServerSocket(port, backlog, InetAddress.getByName(host)));
      } catch (Exception e)
      {
        LOG.warn("Could not create KRB5 Listener", e);
        throw new IOException("Could not create KRB5 Listener: " + e.toString());
      }
    }
    
    // Add Kerberos ciphers to this socket server if needed.
    if(useKrb) {
      ss.setNeedClientAuth(true);
      String [] combined;
      if(useCerts) { // combine the cipher suites
        String[] certs = ss.getEnabledCipherSuites();
        combined = new String[certs.length + KRB5_CIPHER_SUITES.size()];
        System.arraycopy(certs, 0, combined, 0, certs.length);
        System.arraycopy(KRB5_CIPHER_SUITES.toArray(new String[0]), 0, combined,
              certs.length, KRB5_CIPHER_SUITES.size());
      } else { // Just enable Kerberos auth
        combined = KRB5_CIPHER_SUITES.toArray(new String[0]);
      }
      
      ss.setEnabledCipherSuites(combined);
    }
    System.err.println("New socket created");
    return ss;
  };

  @Override
  public void customize(EndPoint endpoint, Request request) throws IOException {
    if(useKrb) { // Add Kerberos-specific info
      SSLSocket sslSocket = (SSLSocket)endpoint.getTransport();
      Principal remotePrincipal = sslSocket.getSession().getPeerPrincipal();
      logIfDebug("Remote principal = " + remotePrincipal);
      request.setScheme(HttpSchemes.HTTPS);
      request.setAttribute(REMOTE_PRINCIPAL, remotePrincipal);
      
      if(!useCerts) { // Add extra info that would have been added by super
        String cipherSuite = sslSocket.getSession().getCipherSuite();
        Integer keySize = Integer.valueOf(ServletSSL.deduceKeyLength(cipherSuite));;
        
        request.setAttribute("javax.servlet.request.cipher_suite", cipherSuite);
        request.setAttribute("javax.servlet.request.key_size", keySize);
      } 
    }
    
    if(useCerts) super.customize(endpoint, request);
    System.err.println();
  }
  
  private void logIfDebug(String s) {
    if(LOG.isDebugEnabled())
      LOG.debug(s);
  }
}
