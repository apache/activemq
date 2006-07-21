#ifndef ACTIVEMQ_COMMANDS_IOTRANSPORTTEST_H_
#define ACTIVEMQ_COMMANDS_IOTRANSPORTTEST_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <activemq/transport/IOTransport.h>
#include <activemq/transport/CommandListener.h>
#include <activemq/transport/CommandReader.h>
#include <activemq/transport/CommandWriter.h>
#include <activemq/transport/Command.h>
#include <activemq/transport/TransportExceptionListener.h>
#include <activemq/concurrent/Concurrent.h>
#include <activemq/io/ByteArrayInputStream.h>
#include <activemq/io/ByteArrayOutputStream.h>
#include <activemq/concurrent/Thread.h>
#include <activemq/concurrent/Mutex.h>

namespace activemq{
namespace transport{

    class IOTransportTest : public CppUnit::TestFixture {

        CPPUNIT_TEST_SUITE( IOTransportTest );
        CPPUNIT_TEST( testStartClose );
        CPPUNIT_TEST( testRead );
        CPPUNIT_TEST( testWrite );
        CPPUNIT_TEST( testException );
        CPPUNIT_TEST_SUITE_END();

    public:
    
        class MyCommand : public Command{
        public:
            MyCommand(){ c = 0; }
            virtual ~MyCommand(){}
            
            char c;
            
            virtual void setCommandId( const unsigned int id ){}
            virtual unsigned int getCommandId() const{ return 0; }
            
            virtual void setResponseRequired( const bool required ){}
            virtual bool isResponseRequired() const{ return false; }
        };
        
        class MyCommandListener : public CommandListener{
        public:
            MyCommandListener(){}
            virtual ~MyCommandListener(){}
            
            std::string str;
            virtual void onCommand( Command* command ){
                const MyCommand* cmd = dynamic_cast<const MyCommand*>(command);
                str += cmd->c;
                delete command;
            }
        };
        
        class MyCommandReader : public CommandReader{
        private:
       
            /**
             * The target input stream.
             */
            io::InputStream* inputStream;
            
        public:
            MyCommandReader(){ throwException = false; }
            virtual ~MyCommandReader(){}
            
            bool throwException;
    
            virtual void setInputStream(io::InputStream* is){
                inputStream = is;
            }

            virtual io::InputStream* getInputStream(void){
                return inputStream;
            }

            virtual Command* readCommand( void ) throw (CommandIOException){
              
                try{
                    if( throwException ){
                        throw CommandIOException();
                    }
                    
                    synchronized( inputStream ){
                        MyCommand* command = new MyCommand();
                        command->c = inputStream->read();
                        return command;
                    }
                    
                    assert(false);
                    return NULL;
                }catch( exceptions::ActiveMQException& ex ){
                    CommandIOException cx( ex );
                    cx.setMark( __FILE__, __LINE__ );
                    throw cx;
                }
            }

            virtual int read(unsigned char* buffer, int count) 
                throw( io::IOException ) {
                return 0;
            }
           
            virtual unsigned char readByte() throw(io::IOException) {
                return 0;
            }
        };
        
        class MyCommandWriter : public CommandWriter{
        private:
        
            /**
             * Target output stream.
             */
            io::OutputStream* outputStream;
            
        public:
            virtual ~MyCommandWriter(){}

            virtual void setOutputStream(io::OutputStream* os){
                outputStream = os;
            }
          
            virtual io::OutputStream* getOutputStream(void){
                return outputStream;
            }

            virtual void writeCommand( const Command* command ) 
                throw (CommandIOException)
            {
                try{
                    synchronized( outputStream ){
                                            
                        const MyCommand* m = 
                            dynamic_cast<const MyCommand*>(command);
                        outputStream->write( m->c );                    
                    }
                }catch( exceptions::ActiveMQException& ex ){
                    CommandIOException cx( ex );
                    cx.setMark( __FILE__, __LINE__ );
                    throw cx;
                }
            }

            virtual void write(const unsigned char* buffer, int count) 
                throw(io::IOException) {}
           
            virtual void writeByte(unsigned char v) throw(io::IOException) {}
        };
        
        class MyExceptionListener : public TransportExceptionListener{
        public:
        
            Transport* transport;
            concurrent::Mutex mutex;
            
            MyExceptionListener(){
                transport = NULL;
            }
            virtual ~MyExceptionListener(){}
            
            virtual void onTransportException( Transport* source, const exceptions::ActiveMQException& ex ){
                transport = source;

                synchronized(&mutex)
                {
                   mutex.notify();
                }
            }
        };        
    
    public:

        virtual void setUp(){}; 
        virtual void tearDown(){};
        
        // This will just test that we can start and stop the 
        // transport without any exceptions.
        void testStartClose(){
            
            io::ByteArrayInputStream is;
            io::ByteArrayOutputStream os;
            MyCommandListener listener;
            MyCommandReader reader;
            MyCommandWriter writer;
            MyExceptionListener exListener;
            IOTransport transport;
            transport.setCommandListener( &listener );
            transport.setCommandReader( &reader );
            transport.setCommandWriter( &writer );
            transport.setInputStream( &is );
            transport.setOutputStream( &os );
            transport.setTransportExceptionListener( &exListener );
            
            transport.start();
            
            concurrent::Thread::sleep( 50 );
            
            transport.close();
        }
        
        void testRead(){
            
            io::ByteArrayInputStream is;
            io::ByteArrayOutputStream os;
            MyCommandListener listener;
            MyCommandReader reader;
            MyCommandWriter writer;
            MyExceptionListener exListener;
            IOTransport transport;
            transport.setCommandListener( &listener );
            transport.setCommandReader( &reader );
            transport.setCommandWriter( &writer );
            transport.setInputStream( &is );
            transport.setOutputStream( &os );
            transport.setTransportExceptionListener( &exListener );
            
            transport.start();
            
            concurrent::Thread::sleep( 10 );
            
            unsigned char buffer[10] = { '1', '2', '3', '4', '5', '6', '7', '8', '9', '0' };
            try{
                synchronized( &is ){
                    is.setByteArray( buffer, 10 );
                }
            }catch( exceptions::ActiveMQException& ex ){
                ex.setMark( __FILE__, __LINE__ );
            }
            
            concurrent::Thread::sleep( 100 );
            
            CPPUNIT_ASSERT( listener.str == "1234567890" );
            
            transport.close();
        }
        
        void testWrite(){
            
            io::ByteArrayInputStream is;
            io::ByteArrayOutputStream os;
            MyCommandListener listener;
            MyCommandReader reader;
            MyCommandWriter writer;
            MyExceptionListener exListener;
            IOTransport transport;
            transport.setCommandListener( &listener );
            transport.setCommandReader( &reader );
            transport.setCommandWriter( &writer );
            transport.setInputStream( &is );
            transport.setOutputStream( &os );
            transport.setTransportExceptionListener( &exListener );
            
            transport.start();
            
            MyCommand cmd;
            cmd.c = '1';
            transport.oneway( &cmd );
            cmd.c = '2';
            transport.oneway( &cmd );
            cmd.c = '3';
            transport.oneway( &cmd );
            cmd.c = '4';
            transport.oneway( &cmd );
            cmd.c = '5';
            transport.oneway( &cmd );
            
            const unsigned char* bytes = os.getByteArray();
            int size = os.getByteArraySize();
            CPPUNIT_ASSERT( size >= 5 );
            CPPUNIT_ASSERT( bytes[0] == '1' );
            CPPUNIT_ASSERT( bytes[1] == '2' );
            CPPUNIT_ASSERT( bytes[2] == '3' );
            CPPUNIT_ASSERT( bytes[3] == '4' );
            CPPUNIT_ASSERT( bytes[4] == '5' );
            
            transport.close();
        }
        
        void testException(){
            
            io::ByteArrayInputStream is;
            io::ByteArrayOutputStream os;
            MyCommandListener listener;
            MyCommandReader reader;
            MyCommandWriter writer;
            MyExceptionListener exListener;
            IOTransport transport;
            transport.setCommandListener( &listener );
            transport.setCommandReader( &reader );
            reader.throwException = true;
            transport.setCommandWriter( &writer );
            transport.setInputStream( &is );
            transport.setOutputStream( &os );
            transport.setTransportExceptionListener( &exListener );
            
            unsigned char buffer[1] = { '1' };
            try{
                synchronized( &is ){                
                    is.setByteArray( buffer, 1);
                }
            }catch( exceptions::ActiveMQException& ex ){
                ex.setMark(__FILE__, __LINE__ );
            }
            
            transport.start();
            
            synchronized(&exListener.mutex)
            {
               if(exListener.transport != &transport)
               {
                  exListener.mutex.wait(1000);
               }
            }
                                    
            CPPUNIT_ASSERT( exListener.transport == &transport );
            
            transport.close();
        }
    };
    
}}

#endif /*ACTIVEMQ_COMMANDS_IOTRANSPORTTEST_H_*/
