#ifndef _ACTIVEMQ_NETWORK_SOCKETFACTORYTEST_H_
#define _ACTIVEMQ_NETWORK_SOCKETFACTORYTEST_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <activemq/network/Socket.h>
#include <activemq/network/TcpSocket.h>
#include <activemq/network/BufferedSocket.h>
#include <activemq/network/ServerSocket.h>
#include <activemq/network/SocketFactory.h>
#include <activemq/util/SimpleProperties.h>
#include <activemq/concurrent/Concurrent.h>
#include <activemq/concurrent/Mutex.h>
#include <activemq/concurrent/Thread.h>

#include <sstream>

namespace activemq{
namespace network{

   class SocketFactoryTest : public CppUnit::TestFixture
   {
      CPPUNIT_TEST_SUITE( SocketFactoryTest );
      CPPUNIT_TEST( test );
      CPPUNIT_TEST_SUITE_END();

      static const int port = 23232;
      
      class MyServerThread : public concurrent::Thread{
      private:
      
         bool done;
         int numClients;
         std::string lastMessage;
         
      public:
      
         concurrent::Mutex mutex;
         
      public:
         MyServerThread(){
            done = false;
            numClients = 0;
         }
         virtual ~MyServerThread(){
            stop();        
         }
         
         std::string getLastMessage(){
            return lastMessage;
         }
         
         int getNumClients(){
            return numClients;
         }
         
         virtual void stop(){
            done = true;
         }
         
         virtual void run(){
            try{
               unsigned char buf[1000];
               
               ServerSocket server;
               server.bind( "127.0.0.1", port );
               
               network::Socket* socket = server.accept();
               server.close();
               
               socket->setSoTimeout( 10 );
               socket->setSoLinger( false );

               synchronized(&mutex)
               {
                   numClients++;

                   mutex.notifyAll();
               }

               while( !done && socket != NULL ){
                                    
                  io::InputStream* stream = socket->getInputStream();
                  if( stream->available() > 0 ){
                     
                     memset( buf, 0, 1000 );
                     try{
                        stream->read( buf, 1000 );
                        
                        lastMessage = (char*)buf;
                        
                        if( strcmp( (char*)buf, "reply" ) == 0 ){
                           io::OutputStream* output = socket->getOutputStream();
                           output->write( (unsigned char*)"hello", strlen("hello" ) );
                        }
                        
                     }catch( io::IOException& ex ){
                        done = true;
                     }                                      
                     
                  }else{
                     Thread::sleep( 10 );
                  }
               }
               
               socket->close();
               delete socket;
               
               numClients--;
               
               synchronized(&mutex)
               {
                   mutex.notifyAll();
               }
               
            }catch( io::IOException& ex ){
               printf("%s\n", ex.getMessage() );
               CPPUNIT_ASSERT( false );
            }catch( ... ){
               CPPUNIT_ASSERT( false );
            }
         }
         
      };
      
   public:
   
   	SocketFactoryTest() {}
   	virtual ~SocketFactoryTest() {}
      
      void test(void)
      {
         try
         {
            MyServerThread serverThread;
            serverThread.start();
         
            concurrent::Thread::sleep( 40 );
            
            util::SimpleProperties properties;
            
            std::ostringstream ostream;
            
            ostream << "127.0.0.1:" << port;
            
            properties.setProperty("uri", ostream.str());
            properties.setProperty("soLinger", "false");
            properties.setProperty("soTimeout", "5");

            Socket* client = SocketFactory::createSocket(properties);

            BufferedSocket* buffSocket = dynamic_cast<BufferedSocket*>(client);

            CPPUNIT_ASSERT( buffSocket != NULL );

            synchronized(&serverThread.mutex)
            {
                if(serverThread.getNumClients() != 1)
                {
                    serverThread.mutex.wait(1000);
                }
            }
            
            CPPUNIT_ASSERT( client->isConnected() );
            
            CPPUNIT_ASSERT( serverThread.getNumClients() == 1 );
            
            client->close();
            
            synchronized(&serverThread.mutex)
            {
                if(serverThread.getNumClients() != 0)
                {
                    serverThread.mutex.wait(1000);
                }
            }

            CPPUNIT_ASSERT( serverThread.getNumClients() == 0 );
            
            serverThread.stop();
            serverThread.join();

            delete client;
         }
         catch(exceptions::ActiveMQException ex)
         {
            CPPUNIT_ASSERT( false );
         }
      }

   };

}}

#endif /*_ACTIVEMQ_NETWORK_SOCKETFACTORYTEST_H_*/
