#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>
#include <cppunit/BriefTestProgressListener.h>
#include <cppunit/TestResult.h>

int main( int argc, char **argv)
{
    CppUnit::TextUi::TestRunner runner;
    CppUnit::TestFactoryRegistry &registry = CppUnit::TestFactoryRegistry::getRegistry();
    runner.addTest( registry.makeTest() );

    // Shows a message as each test starts
    CppUnit::BriefTestProgressListener listener;
    runner.eventManager().addListener( &listener );
    
    bool wasSuccessful = runner.run( "", false );
    return !wasSuccessful;
}

