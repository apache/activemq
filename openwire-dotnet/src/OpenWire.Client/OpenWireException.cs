using System;
using System.Collections;
using OpenWire.Client.Commands;
using OpenWire.Client.Core;

namespace OpenWire.Client {
        /// <summary>
        /// Represents an OpenWire exception
        /// </summary>
        public class OpenWireException : Exception {
                public OpenWireException(string message) : base(message) {
                }
        } 
}
