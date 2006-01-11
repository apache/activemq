using System;
using System.Collections;
using OpenWire.Client.Commands;
using OpenWire.Client.Core;

namespace OpenWire.Client {
        /// <summary>
        /// Represents a connection with a message broker
        /// </summary>
        public class Connection : IConnection {

                private ConnectionInfo info;
                private ITransport transport;
                IList sessions = new ArrayList();
                private bool transacted;
                private bool connected;
                private bool closed;
                private AcknowledgementMode acknowledgementMode;
                private long sessionCounter;

                public Connection(ITransport transport, ConnectionInfo info) {
                        this.transport = transport;
                        this.info = info; 
                }

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
                        CheckConnected();
                        SessionInfo info = CreateSessionInfo(transacted, acknowledgementMode);
                        SyncRequest(info);
                        Session session = new Session(this, info);
                        sessions.Add(session);
                        return session; 
                }

                public void Dispose() {
                        foreach (Session session in sessions) {
                                session.Dispose(); 
                        }
                        sessions.Clear();
                        transport.Dispose();
                        closed = true; 
                }

                // Properties

                public ITransport ITransport {
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

                public string ClientId {
                        get { return info.ClientId; }
                        set {
                                if (connected) {
                                        throw new OpenWireException("You cannot change the ClientId once the Connection is connected"); 
                                }
                                info.ClientId = value;
                        }
                }

                // Implementation methods

                /// <summary>
                /// Performs a synchronous request-response with the broker
                /// </summary>
                public Response SyncRequest(Command command) {
                        CheckConnected();
                        Response response = ITransport.Request(command);
                        if (response is ExceptionResponse) {
                                ExceptionResponse exceptionResponse = (ExceptionResponse) response;
                                // TODO include stack trace
                                throw new OpenWireException("Request failed: " + exceptionResponse); 
                        }
                        return response; 
                }


                protected SessionInfo CreateSessionInfo(bool transacted, AcknowledgementMode acknowledgementMode) {
                        SessionInfo answer = new SessionInfo();
                        SessionId sessionId = new SessionId();
                        sessionId.ConnectionId = info.ConnectionId.Value;
                        lock (this) {
                                sessionId.Value = ++sessionCounter; 
                        }
                        answer.SessionId = sessionId;
                        return answer; 
                }

                protected void CheckConnected() {
                        if (closed) {
                                throw new ConnectionClosedException(); 
                        }
                        if (!connected) {
                                SyncRequest(info);
                                connected = true; 
                        } 
                } 
        } 
}
