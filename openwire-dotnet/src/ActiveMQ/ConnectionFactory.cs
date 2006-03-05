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
using ActiveMQ.Transport.Tcp;
using JMS;
using System;

namespace ActiveMQ
{
    /// <summary>
    /// Represents a connection with a message broker
    /// </summary>
    public class ConnectionFactory : IConnectionFactory
    {
        private Uri brokerUri = new Uri("tcp://localhost:61616");
        private string userName;
        private string password;
        private string clientId;
        
        public ConnectionFactory()
        {
        }
        
        public ConnectionFactory(Uri brokerUri)
        {
			this.brokerUri=brokerUri;
        }
        
        public IConnection CreateConnection()
        {
            return CreateConnection(userName, password);
        }
        
        public IConnection CreateConnection(string userName, string password)
        {
            ConnectionInfo info = CreateConnectionInfo(userName, password);
            ITransport transport = new TcpTransportFactory().CreateTransport(brokerUri);
            Connection connection = new Connection(transport, info);
            connection.ClientId = info.ClientId;
            return connection;
        }
        
        // Properties
        
        public Uri BrokerUri
        {
            get { return brokerUri; }
            set { brokerUri = value; }
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
        
    }
}
