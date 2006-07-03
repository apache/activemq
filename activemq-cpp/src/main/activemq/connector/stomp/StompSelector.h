#ifndef ACTIVEMQ_CONNECTOR_STOMP_STOMPSELECTOR_H_
#define ACTIVEMQ_CONNECTOR_STOMP_STOMPSELECTOR_H_

#include <cms/Message.h>
#include <string>

namespace activemq{
namespace connector{
namespace stomp{
    
    /**
     * Since the stomp protocol doesn't have a consumer-based selector
     * mechanism, we have to do the selector logic on the client
     * side.  This class provides the selector algorithm that is
     * needed to determine if a given message is to be selected for
     * a given consumer's selector string.
     */
    class StompSelector{
    public:
    
        static bool isSelected( const std::string& selector,
            cms::Message* msg )
        {
            return true;
        }
        
    };
    
}}}

#endif /*ACTIVEMQ_CONNECTOR_STOMP_STOMPSELECTOR_H_*/
