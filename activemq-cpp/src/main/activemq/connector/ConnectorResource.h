#ifndef ACTIVEMQ_CONNECTOR_CONNECTORRESOURCE_H_
#define ACTIVEMQ_CONNECTOR_CONNECTORRESOURCE_H_

namespace activemq{
namespace connector{

    /**
     * An object who's lifetime is determined by
     * the connector that created it.  All ConnectorResources
     * should be given back to the connector rather than
     * deleting explicitly.
     */
    class ConnectorResource
    {
    public:

        /**
         * Destructor
         */
        virtual ~ConnectorResource() {}
    };

}}

#endif /*ACTIVEMQ_CONNECTOR_CONNECTORRESOURCE_H_*/
