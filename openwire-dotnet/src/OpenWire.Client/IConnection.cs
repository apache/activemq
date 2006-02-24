using System;
using OpenWire.Client.Commands;

namespace OpenWire.Client {

        public enum AcknowledgementMode {
                Unknown, AutoAcknowledge, ClientAcknowledge, Transactional 
        }


        /// <summary>
        /// Represents a connection with a message broker
        /// </summary>
        public interface IConnection : IDisposable, IStartable, IStopable {

                /// <summary>
                /// Creates a new session to work on this connection
                /// </summary>
                ISession CreateSession();

                /// <summary>
                /// Creates a new session to work on this connection
                /// </summary>
                ISession CreateSession(bool transacted, AcknowledgementMode acknowledgementMode);


                // Properties

                bool Transacted {
                        get;
                        set; 
                }

                AcknowledgementMode AcknowledgementMode {
                        get;
                        set; 
                }

				String ClientId
				{
					get;
					set; 
				}
 
				
		}
}
