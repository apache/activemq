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
using ActiveMQ.Commands;
using ActiveMQ.Transport;
using JMS;
using System;

namespace ActiveMQ
{
    /// <summary>
    /// Represents a connection with a message broker
    /// </summary>
    public class ConnectionFactory : IConnectionFactory
    {
        private string host = "localhost";
        private int port = 61616;
        private string userName;
        private string password;
        private string clientId;
        
        public ConnectionFactory()
        {
        }
        
        public ConnectionFactory(string host, int port)
        {
            this.host = host;
            this.port = port;
        }
        
        public IConnection CreateConnection()
        {
            return CreateConnection(userName, password);
        }
        
        public IConnection CreateConnection(string userName, string password)
        {
            ConnectionInfo info = CreateConnectionInfo(userName, password);
            ITransport transport = CreateTransport();
            Connection connection = new Connection(transport, info);
            connection.ClientId = info.ClientId;
            return connection;
        }
        
        // Properties
        
        public string Host
        {
            get { return host; }
            set { host = value; }
        }
        
        public int Port
        {
            get { return port; }
            set { port = value; }
        }
        
        public string UserName
        {
            get { return userName; }
            set { userName = value; }
        }
        
        public string Password
        {
            get { return password; }
            set { password = value; }
        }
        
        public string ClientId
        {
            get { return clientId; }
            set { clientId = value; }
        }
        
        // Implementation methods
        
        protected virtual ConnectionInfo CreateConnectionInfo(string userName, string password)
        {
            ConnectionInfo answer = new ConnectionInfo();
            ConnectionId connectionId = new ConnectionId();
            connectionId.Value = CreateNewGuid();
            
            answer.ConnectionId = connectionId;
            answer.UserName = userName;
            answer.Password = password;
            answer.ClientId = clientId;
            if (clientId == null)
            {
                answer.ClientId = CreateNewGuid();
            }
            return answer;
        }
        
        protected string CreateNewGuid()
        {
            return Guid.NewGuid().ToString();
        }
        
        protected ITransport CreateTransport()
        {
            return new SocketTransport(host, port);
        }
    }
}
