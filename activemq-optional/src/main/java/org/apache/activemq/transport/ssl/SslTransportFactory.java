/**
 * 
 * Copyright 2005 LogicBlaze, Inc. http://www.logicblaze.com
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
 * 
 **/
package org.apache.activemq.transport.ssl;

import org.apache.activemq.transport.tcp.TcpTransportFactory;

import javax.net.ServerSocketFactory;
import javax.net.SocketFactory;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSocketFactory;

/**
 * An SSL version of the TCP transport
 * 
 * @version $Revision: 1.1 $
 */
public class SslTransportFactory extends TcpTransportFactory {

    protected SocketFactory createSocketFactory() {
        return SSLSocketFactory.getDefault();
    }

    protected ServerSocketFactory createServerSocketFactory() {
        return SSLServerSocketFactory.getDefault();
    }
}
