using System;
using System.Collections;
using OpenWire.Client.Commands;
using OpenWire.Client.Core;

namespace OpenWire.Client {
        /// <summary>
        /// Represents a connection with a message broker
        /// </summary>
        public class Connection : IConnection {

                private Transport transport;
                IList sessions = new ArrayList();
                private bool transacted;
                private AcknowledgementMode acknowledgementMode;

                /// <summary>
                /// Creates a new session to work on this connection
                /// </summary>
                public ISession CreateSession() {
                        return CreateSession(transacted, acknowledgementMode); 
                }

                /// <summary>
                /// Creates a new session to work on this connection
                /// </summary>
                public ISession CreateSession(bool transacted, AcknowledgementMode acknowledgementMode) {
                        Session session = new Session(this, acknowledgementMode);
                        sessions.Add(session);
                        return session; 
                }

                public void Dispose() {
                        foreach (Session session in sessions) {
                                session.Dispose();
                        }
                }

                // Properties

                public Transport Transport {
                        get { return transport; }
                        set { this.transport = value; } 
                }

                public bool Transacted {
                        get { return transacted; }
                        set { this.transacted = value; } 
                }

                public AcknowledgementMode AcknowledgementMode {
                        get { return acknowledgementMode; }
                        set { this.acknowledgementMode = value; } 
                } 
        } 
}
