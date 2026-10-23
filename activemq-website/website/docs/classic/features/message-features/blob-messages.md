---
title: "Blob Messages"
slug: /blob-messages
---


Blob Messages
-------------

A common requirement these days is to send around massive files for processing by consumers. Folks want to take advantage of the message broker's features such as reliable, transactional load balancing of queues with smart routing but still manage to deal with huge logical files.

So we are introducing a BlobMessage API which allows massive BLOBs (Binary Large OBjects) to be sent around in some out-of-band transport mechanism. Possible out-of-band mechanisms could be HTTP or FTP or SCP or some other point-to-point protocol.

There are now new createBlobMessage() methods on the ActiveMQSession that you can use for sending BLOBs.

### Sending BlobMessages

You can send a URL around the JMS network, such as a file or URL which exists on some shared file system or web server using the following code
```
BlobMessage message = session.createBlobMessage(new URL("http://some.shared.site.com");
producer.send(message);
```
Or if you are creating files or streams dynamically on the client you may want to upload the file to the broker or some server (Jetty, FTP, WebDav or whatever). In which case you'd use one of the following methods
```
// lets use a local file
BlobMessage message = session.createBlobMessage(new File("/foo/bar");
producer.send(message);

// lets use a stream
InputStream in = ...;
BlobMessage message = session.createBlobMessage(in);
producer.send(message);
```

### Receiving BlobMessages

A [BlobMessage](http://activemq.apache.org/maven/activemq-core/apidocs/org/apache/activemq/BlobMessage.html) is a regular JMS message so it can be received just like any other message...
```
public class MyListener implements MessageListener {
  public void onMessage(Message message) {
    if (message instanceof BlobMessage) {
      BlobMessage blobMessage = (BlobMessage) message;
      InputStream in = blobMessage.getInputStream();

      // process the stream...
    }
  }
}
```

### Configuring the BLOB Transfer Policy

You can explicitly configure the BlobTransferPolicy on an ActiveMQConnectionFactory, ActiveMQConnection or ActiveMQSession. Typically its done on the factory either via Java code or Spring.

You can use the [Connection Configuration URI](connection-configuration-uri) to configure these things via a URI.

For example you can connect to a broker also specifying the uploadUrl to use via
```
tcp://localhost:61616?jms.blobTransferPolicy.uploadUrl=http://foo.com
```
