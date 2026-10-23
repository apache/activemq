---
title: "NMS.ActiveMQ Destination Management"
---

The Apache.NMS.ActiveMQ client provides some additional features beyond the standard NMS API.  One such feature is the ability to delete a given destination on the Broker.  This can be useful during testing when you want to start the test with a destination in a known state, empty or otherwise.  You can delete the destination and then the next send to that destination would recreate it at the Broker side.  Deleting a destination is as simple as the following:

**Delete a destination**

```
protected static void DeleteDestination() 
{ 
    IConnectionFactory factory = new ConnectionFactory(ReplaceEnvVar(connectionURI)); 
    using (Connection connection = factory.CreateConnection() as Connection) 
    { 
        using (ISession session = connection.CreateSession()) 
        { 
            IQueue queue = session.GetQueue(testQueueName); 
            try 
            { 
                connection.DeleteDestination(queue); 
            } 
            catch 
            { 
            } 
        } 
    } 
} 
```
