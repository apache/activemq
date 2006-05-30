/*
* Copyright 2006 The Apache Software Foundation or its licensors, as
* applicable.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

using System;
using System.Net;
using System.Net.Sockets;
using ActiveMQ.Transport;

namespace ActiveMQ.Transport.Tcp
{
	public class TcpTransportFactory : ITransportFactory
    {
		public ITransport CreateTransport(Uri location) {
			
			// Console.WriteLine("Opening socket to: " + host + " on port: " + port);
			Socket socket = Connect(location.Host, location.Port);
			ITransport rc = new TcpTransport(socket);
			// TODO: use URI query string to enable the LoggingTransport
			 rc = new LoggingTransport(rc);
			rc = new ResponseCorrelator(rc);
			rc = new MutexTransport(rc);
			return rc;
			
		}
	
		protected Socket Connect(string host, int port)
        {
            // Looping through the AddressList allows different type of connections to be tried
            // (IPv4, IPv6 and whatever else may be available).
            IPHostEntry hostEntry = Dns.Resolve(host);
            foreach (IPAddress address in hostEntry.AddressList)
            {
                Socket socket = new Socket(
                    address.AddressFamily,
                    SocketType.Stream,
                    ProtocolType.Tcp);
                socket.Connect(new IPEndPoint(address, port));
                if (socket.Connected)
                {
                    return socket;
                }
            }
            throw new SocketException();
        }
		
	}
	
}
