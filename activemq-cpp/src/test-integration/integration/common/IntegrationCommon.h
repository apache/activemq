#ifndef _INTEGRATION_COMMON_INTEGRATIONCOMMON_H_
#define _INTEGRATION_COMMON_INTEGRATIONCOMMON_H_

#include <string>

namespace integration{
namespace common{

    class IntegrationCommon
    {
    public:
    
    	virtual ~IntegrationCommon();
    
        static const std::string  defaultURL;
        static const int          defaultDelay;
        static const unsigned int defaultMsgCount;
        
    };

}}

#endif /*_INTEGRATION_COMMON_INTEGRATIONCOMMON_H_*/
