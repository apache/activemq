#ifndef _INTEGRATION_COMMON_TESTER_H_
#define _INTEGRATION_COMMON_TESTER_H_

#include <cms/MessageListener.h>
#include <cms/ExceptionListener.h>


namespace integration{
namespace common{

    class Tester : public cms::ExceptionListener,
                   public cms::MessageListener
    {
    public:
    
    	virtual ~Tester() {}
        
        virtual void test(void) = 0;

    };

}}

#endif /*_INTEGRATION_COMMON_TESTER_H_*/
