using System;
using OpenWire.Client.Core;

namespace OpenWire.Client.Core {
        /// <summary>
        /// Represents an exception on the broker
        /// </summary>
        public class BrokerError : AbstractCommand {
                private string message;
                private string exceptionClass;
                private string stackTrace;

                public string Message {
                        get { return message; }
                        set { message = value; } 
                } 
                
                public string ExceptionClass {
                        get { return exceptionClass; }
                        set { exceptionClass = value; } 
                }

                public string StackTrace {
                        get { return stackTrace; }
                        set { stackTrace = value; } 
                } 
        } 
}
