/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activeio.xnet.hba;

import junit.framework.TestCase;

import org.apache.activeio.xnet.ServerService;
import org.apache.activeio.xnet.ServiceException;
import org.apache.activeio.xnet.hba.IPAddressPermission;
import org.apache.activeio.xnet.hba.IPAddressPermissionFactory;
import org.apache.activeio.xnet.hba.ServiceAccessController;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Properties;

public class ServiceAccessControllerTest extends TestCase {

    public void testWrongExactIPAddressPermission1() throws Exception {
        try {
            IPAddressPermissionFactory.getIPAddressMask("121.122.123.a");
            fail();
        } catch (IllegalArgumentException e) {
        }
    }

    public void testWrongExactIPAddressPermission2() throws Exception {
        try {
            IPAddressPermissionFactory.getIPAddressMask("121.122.123.256");
            fail();
        } catch (IllegalArgumentException e) {
        }
    }

    public void testExactIPAddressPermission() throws Exception {
        IPAddressPermission permission = IPAddressPermissionFactory.getIPAddressMask("121.122.123.124");
        assertTrue(permission.implies(InetAddress.getByAddress(new byte[]{121, 122, 123, 124})));
        assertFalse(permission.implies(InetAddress.getByAddress(new byte[]{121, 122, 123, 125})));
    }

    public void testWrongStartWithIPAddressPermission1() throws Exception {
        try {
            IPAddressPermissionFactory.getIPAddressMask("121.0.123.0");
            fail();
        } catch (IllegalArgumentException e) {
        }
    }

    public void testStartWithIPAddressPermission() throws Exception {
        IPAddressPermission permission = IPAddressPermissionFactory.getIPAddressMask("121.122.0.0");
        assertTrue(permission.implies(InetAddress.getByAddress(new byte[]{121, 122, 123, 124})));
        assertFalse(permission.implies(InetAddress.getByAddress(new byte[]{121, 123, 123, 124})));
    }

    public void testFactorizedIPAddressPermission() throws Exception {
        IPAddressPermission permission = IPAddressPermissionFactory.getIPAddressMask("121.122.123.{1,2,3}");
        assertTrue(permission.implies(InetAddress.getByAddress(new byte[]{121, 122, 123, 1})));
        assertTrue(permission.implies(InetAddress.getByAddress(new byte[]{121, 122, 123, 2})));
        assertTrue(permission.implies(InetAddress.getByAddress(new byte[]{121, 122, 123, 3})));
        assertFalse(permission.implies(InetAddress.getByAddress(new byte[]{121, 122, 123, 4})));

        permission = IPAddressPermissionFactory.getIPAddressMask("121.122.{1,2,3}");
        assertTrue(permission.implies(InetAddress.getByAddress(new byte[]{121, 122, 1, 1})));
        assertTrue(permission.implies(InetAddress.getByAddress(new byte[]{121, 122, 2, 2})));
        assertTrue(permission.implies(InetAddress.getByAddress(new byte[]{121, 122, 3, 3})));
        assertFalse(permission.implies(InetAddress.getByAddress(new byte[]{121, 122, 4, 3})));
    }

    public void testNetmaskIPAddressPermission() throws Exception {
        IPAddressPermission permission = IPAddressPermissionFactory.getIPAddressMask("121.122.123.254/31");
        assertTrue(permission.implies(InetAddress.getByAddress(new byte[]{121, 122, 123, (byte) 254})));
        assertTrue(permission.implies(InetAddress.getByAddress(new byte[]{121, 122, 123, (byte) 255})));
        assertFalse(permission.implies(InetAddress.getByAddress(new byte[]{121, 122, 123, (byte) 253})));

        permission = IPAddressPermissionFactory.getIPAddressMask("121.122.123.254/255.255.255.254");
        assertTrue(permission.implies(InetAddress.getByAddress(new byte[]{121, 122, 123, (byte) 254})));
        assertTrue(permission.implies(InetAddress.getByAddress(new byte[]{121, 122, 123, (byte) 255})));
        assertFalse(permission.implies(InetAddress.getByAddress(new byte[]{121, 122, 123, (byte) 253})));
    }

    public void testExactIPv6AddressPermission() throws Exception {
        IPAddressPermission permission = IPAddressPermissionFactory.getIPAddressMask("101:102:103:104:105:106:107:108");
        assertTrue(permission.implies(InetAddress.getByAddress(new byte[]{1, 1, 1, 2, 1, 3, 1, 4, 1, 5, 1, 6, 1, 7, 1, 8})));
        assertFalse(permission.implies(InetAddress.getByAddress(new byte[]{1, 1, 1, 2, 1, 3, 1, 4, 1, 5, 1, 6, 1, 7, 1, 9})));
    }

    public void testNetmaskIPv6AddressPermission() throws Exception {
        IPAddressPermission permission = IPAddressPermissionFactory.getIPAddressMask("101:102:103:104:105:106:107:FFFE/127");
        assertTrue(permission.implies(InetAddress.getByAddress(new byte[]{1, 1, 1, 2, 1, 3, 1, 4, 1, 5, 1, 6, 1, 7, (byte) 255, (byte) 254})));
        assertTrue(permission.implies(InetAddress.getByAddress(new byte[]{1, 1, 1, 2, 1, 3, 1, 4, 1, 5, 1, 6, 1, 7, (byte) 255, (byte) 255})));
        assertFalse(permission.implies(InetAddress.getByAddress(new byte[]{1, 1, 1, 2, 1, 3, 1, 4, 1, 5, 1, 6, 1, 7, (byte) 255, (byte) 253})));

        permission = IPAddressPermissionFactory.getIPAddressMask("101:102:103:104:105:106:107:FFFE/FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFE");
        assertTrue(permission.implies(InetAddress.getByAddress(new byte[]{1, 1, 1, 2, 1, 3, 1, 4, 1, 5, 1, 6, 1, 7, (byte) 255, (byte) 254})));
        assertTrue(permission.implies(InetAddress.getByAddress(new byte[]{1, 1, 1, 2, 1, 3, 1, 4, 1, 5, 1, 6, 1, 7, (byte) 255, (byte) 255})));
        assertFalse(permission.implies(InetAddress.getByAddress(new byte[]{1, 1, 1, 2, 1, 3, 1, 4, 1, 5, 1, 6, 1, 7, (byte) 255, (byte) 253})));
    }

    public void testServiceOKWithConstructor() throws Exception {
        IPAddressPermission[] masks = new IPAddressPermission[]{
            IPAddressPermissionFactory.getIPAddressMask("121.122.{56,57}")
        };

        MockServerService mockServerService = new MockServerService();
        ServiceAccessController controller = new ServiceAccessController(null, mockServerService, masks);

        executeTestServiceOK(mockServerService, controller);
    }

    public void testServiceNOK() throws Exception {
        IPAddressPermission[] masks = new IPAddressPermission[]{
            IPAddressPermissionFactory.getIPAddressMask("121.122.{56,57}")
        };

        MockServerService mockServerService = new MockServerService();
        ServiceAccessController controller = new ServiceAccessController(null, mockServerService, masks);

        executeTestServiceNOK(controller);
    }

    public void testServiceOKWithInit() throws Exception {
        Properties properties = new Properties();
        properties.put("only_from", "121.122.{56,57}");

        MockServerService mockServerService = new MockServerService();
        ServiceAccessController controller = new ServiceAccessController(mockServerService);
        controller.init(properties);

        executeTestServiceOK(mockServerService, controller);
    }

    public void testServiceNOKWithInit() throws Exception {
        Properties properties = new Properties();
        properties.put("only_from", "121.122.{56,57}");

        MockServerService mockServerService = new MockServerService();
        ServiceAccessController controller = new ServiceAccessController(mockServerService);
        controller.init(properties);

        executeTestServiceOK(mockServerService, controller);
    }

    private void executeTestServiceOK(MockServerService mockServerService, ServiceAccessController controller) throws UnknownHostException, ServiceException, IOException {
        MockSocket mockSocket = new MockSocket(InetAddress.getByAddress(new byte[]{121, 122, 56, 123}));
        controller.service(mockSocket);
        assertSame(mockSocket, mockServerService.socket);

        mockSocket = new MockSocket(InetAddress.getByAddress(new byte[]{121, 122, 57, 123}));
        controller.service(mockSocket);
        assertSame(mockSocket, mockServerService.socket);
    }

    private void executeTestServiceNOK(ServiceAccessController controller) throws UnknownHostException, ServiceException, IOException {
        MockSocket mockSocket = new MockSocket(InetAddress.getByAddress(new byte[]{121, 122, 58, 123}));
        try {
            controller.service(mockSocket);
            fail();
        } catch (SecurityException e) {
        }
    }

    private static class MockSocket extends Socket {
        private final InetAddress address;

        private MockSocket(InetAddress address) {
            this.address = address;
        }

        public InetAddress getInetAddress() {
            return address;
        }
    }

    private static class MockServerService implements ServerService {
        private Socket socket;

        public void init(Properties props) throws Exception {
        }

        public void start() throws ServiceException {
            throw new AssertionError();
        }

        public void stop() throws ServiceException {
            throw new AssertionError();
        }

        public String getIP() {
            throw new AssertionError();
        }

        public int getPort() {
            throw new AssertionError();
        }

        public void service(Socket socket) throws ServiceException, IOException {
            this.socket = socket;
        }

        public String getName() {
            throw new AssertionError();
        }
    }
}