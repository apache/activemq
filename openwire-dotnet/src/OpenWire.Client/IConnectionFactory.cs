using System;
using OpenWire.Client.Commands;

namespace OpenWire.Client {

        /// <summary>
        /// A Factory of IConnection objects
        /// </summary>
        public interface IConnectionFactory {

                /// <summary>
                /// Creates a new connection
                /// </summary>
                IConnection CreateConnection();

                /// <summary>
                /// Creates a new connection with the given user name and password
                /// </summary>
                IConnection CreateConnection(string userName, string password);
        } 
}
