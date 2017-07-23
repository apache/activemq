/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.transport.nio;


import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerTestSupport;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.transport.tcp.Krb5BrokerTestSupport;
import org.apache.directory.api.ldap.model.constants.SupportedSaslMechanisms;
import org.apache.directory.server.annotations.CreateKdcServer;
import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.annotations.SaslMechanism;
import org.apache.directory.server.core.annotations.ContextEntry;
import org.apache.directory.server.core.annotations.CreateDS;
import org.apache.directory.server.core.annotations.CreateIndex;
import org.apache.directory.server.core.annotations.CreatePartition;
import org.apache.directory.server.core.kerberos.KeyDerivationInterceptor;
import org.apache.directory.server.ldap.handlers.sasl.cramMD5.CramMd5MechanismHandler;
import org.apache.directory.server.ldap.handlers.sasl.digestMD5.DigestMd5MechanismHandler;
import org.apache.directory.server.ldap.handlers.sasl.gssapi.GssapiMechanismHandler;
import org.apache.directory.server.ldap.handlers.sasl.ntlm.NtlmMechanismHandler;
import org.apache.directory.server.ldap.handlers.sasl.plain.PlainMechanismHandler;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;

@CreateDS(name = "SaslGssapiBindITest-class",
        partitions =
                {
                        @CreatePartition(
                                name = "example",
                                suffix = "dc=example,dc=com",
                                contextEntry = @ContextEntry(
                                        entryLdif =
                                                "dn: dc=example,dc=com\n" +
                                                        "dc: example\n" +
                                                        "objectClass: top\n" +
                                                        "objectClass: domain\n\n"),
                                indexes =
                                        {
                                                @CreateIndex(attribute = "objectClass"),
                                                @CreateIndex(attribute = "dc"),
                                                @CreateIndex(attribute = "ou")
                                        })
                },
        additionalInterceptors =
                {
                        KeyDerivationInterceptor.class
                })
@CreateLdapServer(
        transports =
                {
                        @CreateTransport(protocol = "LDAP")
                },
        saslHost = "localhost",
        saslPrincipal = "ldap/localhost@EXAMPLE.COM",
        saslMechanisms =
                {
                        @SaslMechanism(name = SupportedSaslMechanisms.PLAIN, implClass = PlainMechanismHandler.class),
                        @SaslMechanism(name = SupportedSaslMechanisms.CRAM_MD5, implClass = CramMd5MechanismHandler.class),
                        @SaslMechanism(name = SupportedSaslMechanisms.DIGEST_MD5, implClass = DigestMd5MechanismHandler.class),
                        @SaslMechanism(name = SupportedSaslMechanisms.GSSAPI, implClass = GssapiMechanismHandler.class),
                        @SaslMechanism(name = SupportedSaslMechanisms.NTLM, implClass = NtlmMechanismHandler.class),
                        @SaslMechanism(name = SupportedSaslMechanisms.GSS_SPNEGO, implClass = NtlmMechanismHandler.class)
                })
@CreateKdcServer(
        transports =
                {
                        @CreateTransport(protocol = "UDP", port = 6088),
                        @CreateTransport(protocol = "TCP", port = 6088)
                })
public class NIOSASLSTransportConnectionTest extends BrokerTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(NIOSASLSTransportConnectionTest.class);
//    private static final Oid KRB5OID;

    static {
        //Initialize ApacheDS processing annotations on given class
        new Krb5BrokerTestSupport(NIOSASLSTransportConnectionTest.class).processApacheDSAnnotations();
//        try {
//            KRB5OID = new Oid("1.2.840.113554.1.2.2");
//        } catch (GSSException e) {
//            throw new RuntimeException();
//        }
    }

    private TransportConnector connectorDefault;

    @Test
    public void testDefault() throws Exception {
        String mechanism = makeNIOSASLConnection("User1", connectorDefault);

        Assert.assertEquals("GSSAPI", mechanism);
    }

//    @Test
//    public void testSupportedNonDefault() throws Exception {
//        //0x00,0x1F  TLS_KRB5_WITH_3DES_EDE_CBC_SHA            [RFC2712]
//        Principal peerPrincipal = makeSSLConnection("User1", "TLS_KRB5_WITH_3DES_EDE_CBC_SHA", connectorWithTLS_KRB5_WITH_3DES_EDE_CBC_SHA);
//
//        Assert.assertNotNull("No peer principal", peerPrincipal);
//        Assert.assertTrue(peerPrincipal.getName().contains("host/"));
//        Assert.assertTrue(peerPrincipal.getName().contains("@EXAMPLE.COM"));
//    }
//
//    @Test
//    public void testUnsupportedNonDefault() throws Exception {
//        Exception expected = null;
//
//        try {
//            //One of defaults: 0x00,0x3C  TLS_RSA_WITH_AES_128_CBC_SHA256           [RFC5246]
//            makeSSLConnection("User1", "TLS_RSA_WITH_AES_128_CBC_SHA256", connectorDefault);
//        } catch (Exception e) {
//            expected = e;
//        }
//
//        assertNotNull(expected);
//        assertTrue(expected.getMessage().contains("handshake_failure"));
//    }
//
//    @Test
//    public void testUnsupportedDefault() throws Exception {
//        Exception expected = null;
//
//        try {
//            //0x00,0x1E  TLS_KRB5_WITH_DES_CBC_SHA                 [RFC2712]
//            makeSSLConnection("User1", "TLS_KRB5_WITH_DES_CBC_SHA", connectorWithTLS_KRB5_WITH_3DES_EDE_CBC_SHA);
//        } catch (Exception e) {
//            expected = e;
//        }
//
//        assertNotNull(expected);
//        assertTrue(expected.getMessage().contains("handshake_failure"));
//    }

    private String makeNIOSASLConnection(String krb5ConfigName, final TransportConnector connector) throws Exception, SocketException {
        final SSLContext context = SSLContext.getInstance("TLS");
        context.init(null, null, null);

        LoginContext loginCtx = new LoginContext(krb5ConfigName);
        loginCtx.login();
        Subject subject = loginCtx.getSubject();


        return (String) Subject.doAs(subject, new PrivilegedAction<String>() {
            public String run() {
                try {

                    Map<String, Object> props = new HashMap<>();
                    props.put(Sasl.QOP, "auth-conf");
                    SaslClient saslClient = Sasl.createSaslClient(new String[] {"GSSAPI"}, null, "host", InetAddress.getLocalHost().getHostName(), props, new NIOSASLTransportServer.TestCallbackHandler());

                    Socket socket = new Socket("localhost", connector.getUri().getPort());
                    socket.setSoTimeout(20000);
                    DataInputStream inStream = new DataInputStream(socket.getInputStream());
                    DataOutputStream outStream = new DataOutputStream(socket.getOutputStream());


                    byte[] token = new byte[0];
                    token = saslClient.hasInitialResponse() ? saslClient.evaluateChallenge(token) : token;

                    while (!saslClient.isComplete()) {

                        // Send a token to the server if one was generated
                        // by
                        // initSecContext
                        if (token != null) {
                            System.out.println("CLIENT: Will send token of size " + token.length + " from initSecContext.");
                            outStream.writeInt(token.length);
                            if (token.length > 0) {
                                outStream.write(token);
                            }
                            outStream.flush();
                        }

                        if (inStream.available() > 0) {
                            token = new byte[inStream.readInt()];
                            System.out.println("CLIENT: Will read input token of size " + token.length + " for processing by initSecContext");
                            inStream.readFully(token);

                            token = saslClient.evaluateChallenge(token);
                        } else {
                            token = null;
                        }
                    }

                    System.out.println("CLIENT: FINAL AUTHENTICATION " + token.length + " from initSecContext.");
                    outStream.writeInt(token.length);
                    if (token.length > 0) {
                        outStream.write(token);
                    }
                    outStream.flush();

                    return saslClient.getMechanismName();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    @Override
    protected BrokerService createBroker() throws Exception {
        BrokerService service = super.createBroker();

        connectorDefault = service.addConnector("nio+sasl://localhost:0?transport.soWriteTimeout=20000&krb5ConfigName=Broker");
        return service;
    }

}
