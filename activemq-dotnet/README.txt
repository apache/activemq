Welcome to the NMS API and the .Net Client for Apache ActiveMQ
==============================================================

For more information see http://incubator.apache.org/activemq/nms.html



To build the code using NAnt type

  nant
  
To run the unit tests you need to run an Apache ActiveMQ Broker first then type

  nant test
  
To generate the documentation type

  nant doc
  
Assuming that you have checked out the ActiveMQ code and the Site in peer directories such as


activemq/
  activemq-dotnet/
  
activemq-site/
   nms/
      ndoc/
      
So that generating the ndoc will copy the generate content into the ndoc directory so it can be deployed on the Apache website.


