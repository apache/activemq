#ifndef ACTIVEMQ_COMMANDS_RESPONSECORRELATORTEST_H_
#define ACTIVEMQ_COMMANDS_RESPONSECORRELATORTEST_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <activemq/transport/ResponseCorrelator.h>
#include <activemq/transport/ExceptionResponse.h>
#include <activemq/concurrent/Thread.h>
#include <activemq/concurrent/Concurrent.h>
#include <activemq/exceptions/UnsupportedOperationException.h>
#include <queue>

namespace activemq{
namespace transport{

    class ResponseCorrelatorTest : public CppUnit::TestFixture {

        CPPUNIT_TEST_SUITE( ResponseCorrelatorTest );
        CPPUNIT_TEST( testBasics );
        CPPUNIT_TEST( testOneway );
        CPPUNIT_TEST( testExceptionResponse );
        CPPUNIT_TEST( testTransportException );
        CPPUNIT_TEST( testMultiRequests );
        CPPUNIT_TEST_SUITE_END();    
    
    public:
    
        class MyCommand : public Command{
        private:
        
            unsigned int commandId;
            bool responseRequired;
            
        public:
        
            virtual void setCommandId( const unsigned int id ){
                commandId = id;
            }
            virtual unsigned int getCommandId() const{
                return commandId;
            }
            
            virtual void setResponseRequired( const bool required ){
                responseRequired = required;
            }
            virtual bool isResponseRequired() const{
                return responseRequired;
            }
        };
        
        class MyResponse : public Response{
        private:
        
            unsigned int commandId;
            bool responseRequired;
            unsigned int corrId;
            
        public:
        
            virtual void setCommandId( const unsigned int id ){
                commandId = id;
            }
            virtual unsigned int getCommandId() const{
                return commandId;
            }
            
            virtual void setResponseRequired( const bool required ){
                responseRequired = required;
            }
            virtual bool isResponseRequired() const{
                return responseRequired;
            }
        
            virtual unsigned int getCorrelationId() const{
                return corrId;
            }
            virtual void setCorrelationId( const unsigned int corrId ){
                this->corrId = corrId;
            }
        };
        
        class MyExceptionResponse : public ExceptionResponse{
        public:
        
            unsigned int commandId;
            bool responseRequired;
            unsigned int corrId;
            BrokerError error;
            
        public:
        
            virtual void setCommandId( const unsigned int id ){
                commandId = id;
            }
            virtual unsigned int getCommandId() const{
                return commandId;
            }
            
            virtual void setResponseRequired( const bool required ){
                responseRequired = required;
            }
            virtual bool isResponseRequired() const{
                return responseRequired;
            }
        
            virtual unsigned int getCorrelationId() const{
                return corrId;
            }
            virtual void setCorrelationId( const unsigned int corrId ){
                this->corrId = corrId;
            }
            virtual const BrokerError* getException() const{
                return &error;
            }
        };
        
        class MyTransport 
        : 
            public Transport,
            public concurrent::Runnable{
        public:
            CommandReader* reader;
            CommandWriter* writer;
            CommandListener* listener;
            TransportExceptionListener* exListener;
            concurrent::Thread* thread;
            concurrent::Mutex mutex;
            concurrent::Mutex startedMutex;
            bool done;
            std::queue<Command*> requests;
            
        public:
        
            MyTransport(){
                reader = NULL;
                writer = NULL;
                listener = NULL;
                exListener = NULL;
                thread = NULL;
                done = false;
            }
            
            virtual ~MyTransport(){
                
                close();
            }
            
            virtual void oneway( Command* command ) 
                throw(CommandIOException, exceptions::UnsupportedOperationException)
            {
                synchronized( &mutex ){
                    requests.push( command );
                    mutex.notifyAll();
                }
            }
            
            virtual Response* request( Command* command ) 
                throw(CommandIOException, exceptions::UnsupportedOperationException)
            {
                throw exceptions::UnsupportedOperationException( 
                    __FILE__, 
                    __LINE__, 
                    "stuff" );
            }
            
            virtual void setCommandListener( CommandListener* listener ){
                this->listener = listener;
            }
            
            virtual void setCommandReader( CommandReader* reader ){
                this->reader = reader;
            }
            
            virtual void setCommandWriter( CommandWriter* writer ){
                this->writer = writer;
            }
            
            virtual void setTransportExceptionListener( 
                TransportExceptionListener* listener )
            {
                this->exListener = listener;
            }
            
            virtual void start() throw( cms::CMSException ){
                close();
                
                done = false;
                
                thread = new concurrent::Thread( this );
                thread->start();
            }
            
            virtual void close() throw( cms::CMSException ){
                
                done = true;
                
                if( thread != NULL ){                    
                    synchronized( &mutex ){
                        mutex.notifyAll();
                    }
                    thread->join();
                    delete thread;
                    thread = NULL;
                }
            }
            
            virtual Response* createResponse( Command* command ){
                
                MyResponse* resp = new MyResponse();
                resp->setCorrelationId( command->getCommandId() );
                resp->setResponseRequired( false );
                return resp;
            }
            
            virtual void run(){
                
                try{

                    synchronized(&startedMutex)
                    {
                       startedMutex.notifyAll();
                    }

                    synchronized( &mutex ){
                        
                        while( !done ){
                            
                            if( requests.empty() ){
                                mutex.wait();
                            }else{
                                
                                Command* cmd = requests.front();
                                requests.pop();
                                
                                // Only send a response if one is required.
                                Response* resp = NULL;
                                if( cmd->isResponseRequired() ){
                                    resp = createResponse( cmd );
                                }
                                
                                mutex.unlock();
                                
                                // Send both the response and the original
                                // command back to the correlator.
                                if( listener != NULL ){
                                    if( resp != NULL ){
                                        listener->onCommand( resp );
                                    }
                                    listener->onCommand( cmd );
                                }
                                
                                mutex.lock();
                            }
                        }
                    }
                }catch( exceptions::ActiveMQException& ex ){
                    if( exListener ){
                        exListener->onTransportException( this, ex );
                    }
                }
                catch( ... ){                    
                    if( exListener ){
                        exceptions::ActiveMQException ex( __FILE__, __LINE__, "stuff" );
                        exListener->onTransportException( this, ex );
                    }
                }
            }
        };
        
        class MyExceptionResponseTransport : public MyTransport{
        public:
        
            MyExceptionResponseTransport(){}            
            virtual ~MyExceptionResponseTransport(){}
            
            virtual Response* createResponse( Command* command ){
                
                MyExceptionResponse* resp = new MyExceptionResponse();
                resp->setCorrelationId( command->getCommandId() );
                resp->setResponseRequired( false );
                resp->error = BrokerError( __FILE__, __LINE__, 
                    "some bad broker stuff" );
                return resp;
            }
        };
        
        class MyBrokenTransport : public MyTransport{
        public:
        
            MyBrokenTransport(){}            
            virtual ~MyBrokenTransport(){}
            
            virtual Response* createResponse( Command* command ){                
                throw exceptions::ActiveMQException( __FILE__, __LINE__,
                    "bad stuff" );
            }
        };
        
        class MyListener 
        : 
            public CommandListener,
            public TransportExceptionListener{
                
        public:
            
            int exCount;
            std::set<int> commands;
            concurrent::Mutex mutex;
            
        public:
        
            MyListener(){
                exCount = 0;
            }
            virtual ~MyListener(){}
            virtual void onCommand( Command* command ){
                
                synchronized( &mutex ){
                    commands.insert( command->getCommandId() );

                    mutex.notify(); 
                }
            }
            
            virtual void onTransportException( 
                Transport* source, 
                const exceptions::ActiveMQException& ex )
            {
                synchronized( &mutex ){
                    exCount++;
                }
            }
        };
        
        class RequestThread : public concurrent::Thread{
        public:
        
            Transport* transport;
            MyCommand cmd;
            Response* resp;
        public:
        
            RequestThread(){
                transport = NULL;
                resp = NULL;
            }
            virtual ~RequestThread(){
                join();
                
                if( resp != NULL ){
                    delete resp;
                    resp = NULL;
                }
            }
            
            void setTransport( Transport* transport ){
                this->transport = transport;
            }
            
            void run(){
                
                try{                    
                    resp = transport->request(&cmd);                    
                }catch( ... ){
                    CPPUNIT_ASSERT( false );
                }
            }
        };
        
    public:

        virtual void setUp(){}; 
        virtual void tearDown(){};
        
        void testBasics(){
            
            try{
                
                MyListener listener;
                MyTransport transport;
                ResponseCorrelator correlator( &transport, false );
                correlator.setCommandListener( &listener );
                correlator.setTransportExceptionListener( &listener );
                CPPUNIT_ASSERT( transport.listener == &correlator );
                CPPUNIT_ASSERT( transport.exListener == &correlator );
                
                // Give the thread a little time to get up and running.
                synchronized(&transport.startedMutex)
                {
                    // Start the transport.
                    correlator.start();
                    transport.startedMutex.wait();
                }
                
                // Send one request.
                MyCommand cmd;
                Response* resp = correlator.request( &cmd );
                CPPUNIT_ASSERT( resp != NULL );
                CPPUNIT_ASSERT( dynamic_cast<ExceptionResponse*>(resp) == NULL );
                CPPUNIT_ASSERT( resp->getCorrelationId() == cmd.getCommandId() );
                
                // Wait to get the message back asynchronously.
                concurrent::Thread::sleep( 100 );
                
                // Since our transport relays our original command back at us as a
                // non-response message, check to make sure we received it and that
                // it is the original command.
                CPPUNIT_ASSERT( listener.commands.size() == 1 );                
                CPPUNIT_ASSERT( listener.exCount == 0 );
                
                correlator.close();
                
                // Destroy the response.
                delete resp;
            }
            AMQ_CATCH_RETHROW( exceptions::ActiveMQException )
            AMQ_CATCHALL_THROW( exceptions::ActiveMQException )
        }
        
        void testOneway(){
            
            try{
                
                MyListener listener;
                MyTransport transport;
                ResponseCorrelator correlator( &transport, false );
                correlator.setCommandListener( &listener );
                correlator.setTransportExceptionListener( &listener );
                CPPUNIT_ASSERT( transport.listener == &correlator );
                CPPUNIT_ASSERT( transport.exListener == &correlator );
                
                // Give the thread a little time to get up and running.
                synchronized(&transport.startedMutex)
                {
                    // Start the transport.
                    correlator.start(); 
                    
                    transport.startedMutex.wait();
                }
                
                // Send many oneway request (we'll get them back asynchronously).
                const unsigned int numCommands = 1000;
                MyCommand commands[numCommands];                
                for( unsigned int ix=0; ix<numCommands; ix++ ){
                    correlator.oneway( &commands[ix] );
                }
                
                // Give the thread a little time to get all the messages back.
                concurrent::Thread::sleep( 500 );
                
                // Make sure we got them all back.
                CPPUNIT_ASSERT( listener.commands.size() == numCommands );
                CPPUNIT_ASSERT( listener.exCount == 0 );
                
                correlator.close();
            }
            AMQ_CATCH_RETHROW( exceptions::ActiveMQException )
            AMQ_CATCHALL_THROW( exceptions::ActiveMQException )
        }
        
        void testExceptionResponse(){
            
            try{
                
                MyListener listener;
                MyExceptionResponseTransport transport;
                ResponseCorrelator correlator( &transport, false );
                correlator.setCommandListener( &listener );
                correlator.setTransportExceptionListener( &listener );
                CPPUNIT_ASSERT( transport.listener == &correlator );
                CPPUNIT_ASSERT( transport.exListener == &correlator );
                                                               
                // Give the thread a little time to get up and running.
                synchronized(&transport.startedMutex)
                {
                    // Start the transport.
                    correlator.start();
                    
                    transport.startedMutex.wait();
                }
                
                // Send one request.
                MyCommand cmd;
                Response* resp = correlator.request( &cmd );
                CPPUNIT_ASSERT( resp != NULL );
                CPPUNIT_ASSERT( dynamic_cast<ExceptionResponse*>(resp) != NULL );
                CPPUNIT_ASSERT( resp->getCorrelationId() == cmd.getCommandId() );
                
                // Wait to make sure we get the exception.
                concurrent::Thread::sleep( 100 );
                
                // Since our transport relays our original command back at us as a
                // non-response message, check to make sure we received it and that
                // it is the original command.
                CPPUNIT_ASSERT( listener.commands.size() == 1 );                
                CPPUNIT_ASSERT( listener.exCount == 1 );
                
                correlator.close();
                
                // Destroy the response.
                delete resp;                              
            }
            AMQ_CATCH_RETHROW( exceptions::ActiveMQException )
            AMQ_CATCHALL_THROW( exceptions::ActiveMQException )
        }
        
        void testTransportException(){
            
            try{
                
                MyListener listener;
                MyBrokenTransport transport;
                ResponseCorrelator correlator( &transport, false );
                correlator.setCommandListener( &listener );
                correlator.setTransportExceptionListener( &listener );
                CPPUNIT_ASSERT( transport.listener == &correlator );
                CPPUNIT_ASSERT( transport.exListener == &correlator );
                
                // Give the thread a little time to get up and running.
                synchronized(&transport.startedMutex)
                {
                    // Start the transport.
                    correlator.start();
                    
                    transport.startedMutex.wait();
                }
                
                // Send one request.                
                MyCommand cmd;
                try{
                    correlator.request( &cmd );
                    CPPUNIT_ASSERT(false);
                }catch( CommandIOException& ex ){
                    // Expected.
                }
                
                // Wait to make sure we get the asynchronous message back.
                concurrent::Thread::sleep( 100 );
                
                // Since our transport relays our original command back at us as a
                // non-response message, check to make sure we received it and that
                // it is the original command.
                CPPUNIT_ASSERT( listener.commands.size() == 0 );                
                CPPUNIT_ASSERT( listener.exCount == 1 );
                
                correlator.close();
            }
            AMQ_CATCH_RETHROW( exceptions::ActiveMQException )
            AMQ_CATCHALL_THROW( exceptions::ActiveMQException )
        }
        
        void testMultiRequests(){
            
            try{
                
                MyListener listener;
                MyTransport transport;
                ResponseCorrelator correlator( &transport, false );
                correlator.setCommandListener( &listener );
                correlator.setTransportExceptionListener( &listener );
                CPPUNIT_ASSERT( transport.listener == &correlator );
                CPPUNIT_ASSERT( transport.exListener == &correlator );
                
                // Start the transport.
                correlator.start();                               
                
                // Make sure the start command got down to the thread.
                CPPUNIT_ASSERT( transport.thread != NULL );
                
                // Give the thread a little time to get up and running.
                synchronized(&transport.startedMutex)
                {
                    transport.startedMutex.wait(500);
                }
                
                // Start all the requester threads.
                const unsigned int numRequests = 100;
                RequestThread requesters[numRequests];
                for( unsigned int ix=0; ix<numRequests; ++ix ){
                    requesters[ix].setTransport( &correlator );
                    requesters[ix].start();
                }
                
                // Make sure we got all the responses and that they were all
                // what we expected.
                for( unsigned int ix=0; ix<numRequests; ++ix ){
                    requesters[ix].join();
                    CPPUNIT_ASSERT( requesters[ix].resp != NULL );
                    CPPUNIT_ASSERT( requesters[ix].cmd.getCommandId() == requesters[ix].resp->getCorrelationId() );
                }

                concurrent::Thread::sleep( 25 );
                synchronized( &listener.mutex )
                {
                    unsigned int count = 0;

                    while( listener.commands.size() != numRequests )
                    {
                        listener.mutex.wait( 45 );

                        ++count;

                        if( count == numRequests ) {
                            break;
                        }
                    }
                }

                // Since our transport relays our original command back at us as a
                // non-response message, check to make sure we received it and that
                // it is the original command.
                CPPUNIT_ASSERT( listener.commands.size() == numRequests );                
                CPPUNIT_ASSERT( listener.exCount == 0 );
                
                correlator.close();
            }
            AMQ_CATCH_RETHROW( exceptions::ActiveMQException )
            AMQ_CATCHALL_THROW( exceptions::ActiveMQException )
        }
    };
    
}}

#endif /*ACTIVEMQ_COMMANDS_RESPONSECORRELATORTEST_H_*/
