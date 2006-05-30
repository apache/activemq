/*
 * Copyright 2006 The Apache Software Foundation or its licensors, as
 * applicable.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "TestListener.hpp"

/*
 * 
 */
TestListener::TestListener()
{
}

/*
 * 
 */
TestListener::~TestListener()
{
}

/*
 * 
 */
void TestListener::onMessage(p<IMessage> message)
{
    try
    {
        p<IBytesMessage> msg = p_dyncast<IBytesMessage> (message) ;

        if( msg == NULL )
            cout << "No message received!" << endl ;
        else
        {
            cout << "Received message with ID: " << msg->getJMSMessageID()->c_str() << endl ;
            cout << "                 boolean: " << (msg->readBoolean() ? "true" : "false") << endl ;
            cout << "                 integer: " << msg->readInt() << endl ;
            cout << "                  string: " << msg->readUTF()->c_str() << endl ;
        }
    }
    catch( exception& e )
    {
        cout << "OnMessage caught: " << e.what() << endl ;
    }
}
