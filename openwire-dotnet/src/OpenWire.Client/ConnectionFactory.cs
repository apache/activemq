using System;
using System.Collections;
using OpenWire.Client.Commands;
using OpenWire.Client.Core;

namespace OpenWire.Client {
        /// <summary>
        /// Represents a connection with a message broker
        /// </summary>
        public class ConnectionFactory : IConnectionFactory {
                private string host = "localhost";
                private int port = 61616;
                private string userName;
                private string password;
                private string clientId;

                public IConnection CreateConnection() {
                        return CreateConnection(userName, password); 
                }

                public IConnection CreateConnection(string userName, string password) {
                        ConnectionInfo info = CreateConnectionInfo(userName, password);
                        Transport transport = CreateTransport();
                        Connection connection = new Connection(transport, info);
                        connection.ClientId = clientId;
                        return connection; 
                }

                // Properties

                public string Host {
                        get { return host; }
                        set { host = value; } 
                }

                public int Port {
                        get { return port; }
                        set { port = value; } 
                }

                public string UserName {
                        get { return userName; }
                        set { userName = value; } 
                }

                public string Password {
                        get { return password; }
                        set { password = value; } 
                }

                public string ClientId {
                        get { return clientId; }
                        set { clientId = value; } 
                }

                // Implementation methods

                protected ConnectionInfo CreateConnectionInfo(string userName, string password) {
                        ConnectionInfo answer = new ConnectionInfo();
                        ConnectionId connectionId = new ConnectionId();
                        connectionId.Value = CreateNewConnectionID();
                        answer.ConnectionId = connectionId;
                        answer.UserName = userName;
                        answer.Password = password;
                        return answer; 
                }

                protected string CreateNewConnectionID() {
                        return Guid.NewGuid().ToString(); 
                }

                protected Transport CreateTransport() {
                        // TODO
                        return null; 
                } 
        } 
}
