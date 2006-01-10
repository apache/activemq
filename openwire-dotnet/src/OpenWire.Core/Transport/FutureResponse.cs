using System;
using System.Threading;

using OpenWire.Core;
using OpenWire.Core.Commands;

namespace OpenWire.Core.Transport {
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
                        get { return response; }
                        set {
                                asyncWaitHandle.WaitOne();
                                response = value;
                                isCompleted = true;
                                asyncWaitHandle.ReleaseMutex();
                        }
                } 
        } 
}
