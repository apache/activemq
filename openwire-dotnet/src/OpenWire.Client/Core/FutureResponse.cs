using System;
using System.Threading;

using OpenWire.Client;
using OpenWire.Client.Commands;

namespace OpenWire.Client.Core {
        /// <summary>
        /// Handles asynchronous responses
        /// </summary>
        public class FutureResponse : IAsyncResult {

                private Response response;
                private Mutex asyncWaitHandle = new Mutex();
                private bool isCompleted;

                public WaitHandle AsyncWaitHandle {
                        get { return asyncWaitHandle; } 
                }

                public object AsyncState {
                        get { return response; }
                        set { response = (Response) value; } 
                }

                public bool IsCompleted {
                        get { return isCompleted; } 
                }

                public bool CompletedSynchronously {
                        get { return false; } 
                }

                public Response Response {
                        get {
                                // TODO use the proper .Net version of notify/wait()
                                while (response == null) {
                                        Thread.Sleep(100);
                                }
                                return response;
                        }
                        set {
                                asyncWaitHandle.WaitOne();
                                response = value;
                                isCompleted = true;
                                asyncWaitHandle.ReleaseMutex(); 
                        }
                } 
        } 
}
