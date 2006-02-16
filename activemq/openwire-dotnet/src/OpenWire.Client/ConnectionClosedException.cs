using System;
using System.Collections;
using OpenWire.Client.Commands;
using OpenWire.Client.Core;

namespace OpenWire.Client {
        /// <summary>
        /// Exception thrown when a connection is used that it already closed
        /// </summary>
        public class ConnectionClosedException : OpenWireException {
                public ConnectionClosedException() : base("The connection is already closed!") {
                }
        } 
}
