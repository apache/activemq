---
title: "Compile ActiveMQ with GCJ"
slug: /compile-activemq-with-gcj
---


You can use [GCJ](http://gcc.gnu.org/java/) to build ActiveMQ as a shared library you can reuse from C++.

### Native compile ActiveMQ HOWTO

#### Abstract

This document describes how to native compile ActiveMQ for use in a C++ environment. The version of ActiveMQ used is 3.2 in this howto. To compile you'll need GCC 4.0.2, or later, with both Java, and C/C++ support.

#### Tools Setup

If you don't already have GCC 4.0.2 installed you need to download and build it. See GCC manuals for complete instructions on how to build GCC but below is a short descriptions of the steps involved. The GCC build steps assumes that you already have an older GCC compiler installed.

*   Unpack GCC into an arbitrary directory, for example /opt/gccbuild, and then create a separate output directory. Your directory structure should look similar to this;
    ```
    /opt/gccbuild/gcc-4.0.2
    /opt/gccbuild/output
    ```
*   Go to the output directory and run configure.
    ```
    cd /opt/gccbuild/output
    ../gcc-4.0.2/configure --prefix=/opt/gcc402
                           --enable-shared
                           --enable-threads=posix
                           --enable-languages=c,c++,java
    ```
*   Run make.
    ```
    make bootstrap
    make install
    ```
*   Download ActiveMQ and copy the JARs to a new empty directory /opt/app, including
    ```
    activeio-1.1.jar
    activemq-core-3.2.jar
    commons-logging-1.0.3.jar
    concurrent-1.3.4.jar
    geronimo-spec-j2ee-jacc-1.0-rc4.jar
    geronimo-spec-j2ee-management-1.0-rc4.jar
    geronimo-spec-jms-1.1-rc4.jar
    geronimo-spec-jta-1.0.1B-rc4.jar
    log4j-1.2.8.jar
    ```

#### Write the Glue Code

Either access the ActiveMQ classes directly from C++ or write a facade object in Java that handles all startup and shutdown logic of ActiveMQ. Save the glue files in the same directory as for the ActiveMQ jars.

An CNI example using a Java object starting the MQ.

##### Bootstrap.cpp
```
#include <gcj/cni.h>
#include <iostream>
#include <java/lang/System.h>
#include <java/lang/Throwable.h>
#include <java/io/PrintStream.h>
#include "MQAdapter.h"

using namespace std;

int main(int argc, char* argv\[\])
{
    cout << "Entering main" << endl;
    using namespace java::lang;

    try
    {
        // Create and startup Java VM
        JvCreateJavaVM(NULL) ;
        JvAttachCurrentThread(NULL, NULL) ;

        System::out->println(JvNewStringLatin1("Java println")) ;

        // Start ActiveMQ
        MQAdapter* pAdapter = new MQAdapter() ;
        pAdapter->start() ;

        // Send a message
        pAdapter->send(JvNewStringLatin1("Hello World!")) ;

        // Shutdown ActiveMQ
        pAdapter->stop() ;
     
        JvDetachCurrentThread() ;
    }
    catch( Throwable *t )
    {
        System::err->println(JvNewStringLatin1("Exception")) ;
        t->printStackTrace() ;
    }
}
```

##### MQAdapter.java
```
import org.activemq.*;
import java.util.Hashtable ;
import javax.jms.*;
import javax.naming.*;

public class MQAdapter
{
    private InitialContext         jndiContext ;
    private QueueConnectionFactory factory ;
    private QueueConnection        connection ;
    private QueueSession           session ;
    private QueueSender            sender ;
    private Queue                  queue ;

    public MQAdapter()
    {
    }

    public void start()
    {
        try
        {
            Hashtable props = new Hashtable() ;
            props.put(Context.INITIAL\_CONTEXT\_FACTORY, "org.activemq.jndi.ActiveMQInitialContextFactory") ;
            props.put(Context.PROVIDER_URL, "tcp://localhost:61616") ;
            props.put("queue.MyQueue", "example.MyQueue") ;

            jndiContext = new InitialContext(props) ;
        
            // Create and configure JMS connection factory
            factory = (QueueConnectionFactory)jndiContext.lookup("ConnectionFactory") ;

            // Lookup Queue
            queue = (Queue)jndiContext.lookup("MyQueue") ;

            // Create a JMS connection
            connection = (QueueConnection)factory.createQueueConnection() ;
            System.out.println("Created connection: " + connection) ;

            // Create a JMS session
            session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE) ;
            System.out.println("Created session: " + session) ;

            // Create JMS sender
            sender  = session.createSender(queue) ;
        }
        catch( Exception e )
        {
            e.printStackTrace() ;

            try
            {
                if( connection != null )
                    connection.close() ;
            } catch( JMSException jmse )
            { /* ignore */ }
        }
    }

    public void stop()
    {
        try
        {
            if( connection != null )
                connection.close() ;
        } catch( JMSException e )
        { /* ignore */ }
    }

    public void send(String msg)
    {
        TextMessage message ;

        try
        {
            message = session.createTextMessage(msg) ;
            sender.send(message) ;
        }
        catch( JMSException e )
        {
            e.printStackTrace() ;
        }
    }
}
```

#### Compile the Java and C++ Code

The Java code must be BC compiled to be able to dynamically link required classes as needed, see reference for more information on BC compilation. Use the suggested script to compile all ActiveMQ JARs and create a class map database.

> **Note**
> 
> Using -Bsymbolic does not seem to work, use -symbolic instead.

```
compile.sh:

#!/bin/sh

# Create new classmap database
gcj-dbtool -n classmap.db

for JAR_FILE in \`find -iname "*.jar"\`
do
    echo "Compiling ${JAR_FILE} to native"
    gcj -shared -findirect-dispatch -fjni -fPIC -Wl,-symbolic -o ${JAR\_FILE}.so ${JAR\_FILE}
    gcj-dbtool -a classmap.db ${JAR\_FILE} ${JAR\_FILE}.so
done
```

*   Run the above script and set environment property GCJ_PROPERTIES.
    ```
    ./compile.sh
    export GCJ_PROPERTIES="gnu.gcj.precompiled.db.path=/opt/app/classmap.db"
    ```

*   Java compile MQAdapter.java
    ```
    gcj --classpath=./geronimo-spec-jms-1.1-rc4.jar:./activemq-core-3.2.jar -C MQAdapter.java
    ```
*   Generate CNI header for MQAdapter.class
    ```
    gcjh MQAdapter
    ```
*   JAR the Java glue code
    ```
    fastjar cf MQAdapter.jar MQAdapter.class
    ```
*   Native compile the Java JAR into a shared library, add output directory to LD\_LIBRARY\_PATH.
    ```
    gcj -shared -findirect-dispatch -fjni -fPIC -Wl,-symbolic -o MQAdapter.so MQAdapter.jar
    export LD\_LIBRARY\_PATH=$LD\_LIBRARY\_PATH:/opt/app
    ```
*   Compile the C++ code
    ```
    g++ -c Bootstrap.cpp
    ```
*   Link Bootstrap with the Java code
    ```
    gcj -o Bootstrap Bootstrap.o -L /opt/app -lgcj -lstdc++ activeio-1.1.jar.so activemq-core-3.2.jar.so
        commons-logging-1.0.3.jar.so concurrent-1.3.4.jar.so geronimo-spec-jms-1.1-rc4.jar.so
        geronimo-spec-j2ee-management-1.0-rc4.jar.so geronimo-spec-j2ee-jacc-1.0-rc4.jar.so
        geronimo-spec-jta-1.0.1B-rc4.jar.so log4j-1.2.8.jar.so MQAdapter.so
    ```

Now, if everything went ok you should be able to run the app. with `./Bootstrap`.

#### References

[How to BC compile with GCJ](http://gcc.gnu.org/wiki/How%20to%20BC%20compile%20with%20GCJ)

[The state of Java on Linux](http://www.redhat.com/magazine/012oct05/features/java/)

