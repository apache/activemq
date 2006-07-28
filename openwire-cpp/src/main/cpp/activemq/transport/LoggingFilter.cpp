/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "activemq/transport/LoggingFilter.hpp"

using namespace apache::activemq::transport;


// --- Constructors -------------------------------------------------

/*
 * 
 */
LoggingFilter::LoggingFilter(p<ITransport> next) :
    TransportFilter(next)
{
    this->next = next ;
}


// --- Operation methods --------------------------------------------

/*
 * 
 */
void LoggingFilter::oneway(p<ICommand> command)
{
    int cmdid  = command->getCommandId(),
        corrid = -1 ;

    // Get correlation id if a response
    if( command->getDataStructureType() == Response::TYPE )
        corrid = p_cast<Response>(command)->getCorrelationId() ;

    // Dump log entry
    printf("Sending command: cmd.id = %d, corr.id = %d, type = %s\n",
           cmdid, corrid, 
           AbstractCommand::getDataStructureTypeAsString(command->getDataStructureType())->c_str() ) ;

/*    cout << "Sending command: id = " <<
            command->getCommandId() <<
            ", type = " <<
            AbstractCommand::getDataStructureTypeAsString(command->getDataStructureType())->c_str() <<
            endl ;*/

    this->next->oneway(command) ;
}


// --- Event methods ------------------------------------------------

/*
 * 
 */
void LoggingFilter::onCommand(p<ITransport> transport, p<ICommand> command)
{
    if( command == NULL )
        cout << "Received NULL command" << endl ;
    else
    {
        int cmdid  = command->getCommandId(),
            corrid = -1 ;

        // Get correlation id if a response
        if( command->getDataStructureType() == Response::TYPE )
            corrid = p_cast<Response>(command)->getCorrelationId() ;

        // Dump log entry
        printf("Received command: cmd.id = %d, corr.id = %d, type = %s\n",
               cmdid, corrid, 
               AbstractCommand::getDataStructureTypeAsString(command->getDataStructureType())->c_str() ) ;

/*        cout << "Recived command: id = " <<
                command->getCommandId() <<
                ", type = " <<
                AbstractCommand::getDataStructureTypeAsString(command->getDataStructureType())->c_str() <<
                endl ;*/
    }

    // Forward incoming command to "real" listener
    this->listener->onCommand(transport, command) ;
}

/*
 * 
 */
void LoggingFilter::onError(p<ITransport> transport, exception& error)
{
    cout << "Received exception = '" << error.what() << "'" << endl ;

    // Forward incoming exception to "real" listener
    this->listener->onError(transport, error) ;
}
