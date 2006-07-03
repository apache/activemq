#ifndef ACTIVEMQ_CONNECTOR_STOMP_STOMPRESPONSEBUILDER_H_
#define ACTIVEMQ_CONNECTOR_STOMP_STOMPRESPONSEBUILDER_H_

#include <activemq/transport/DummyTransport.h>
#include <activemq/connector/stomp/commands/ConnectCommand.h>
#include <activemq/connector/stomp/commands/ConnectedCommand.h>

namespace activemq{
namespace connector{
namespace stomp{
    
    class StompResponseBuilder : public transport::DummyTransport::ResponseBuilder{
        
    private:
    
        std::string sessionId;
        
    public:
    
        StompResponseBuilder( const std::string& sessionId ){
            this->sessionId = sessionId;
        }
        
        virtual ~StompResponseBuilder(){}
        
        virtual transport::Response* buildResponse( const transport::Command* cmd ){
            
            const commands::ConnectCommand* connectCommand = 
                dynamic_cast<const commands::ConnectCommand*>(cmd);
                
            if( connectCommand != NULL ){
                commands::ConnectedCommand* resp = new commands::ConnectedCommand();
                resp->setCorrelationId( connectCommand->getCommandId() );
                resp->setSessionId( sessionId );
                return resp;                
            }
            
            throw transport::CommandIOException( __FILE__, __LINE__,
                "StompResponseBuilder - unrecognized command" );
        }
    };
    
}}}

#endif /*ACTIVEMQ_CONNECTOR_STOMP_STOMPRESPONSEBUILDER_H_*/
