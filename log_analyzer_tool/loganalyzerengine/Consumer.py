"""
Module Consumer
"""

class Consumer(object):
    """
    This class represents an ActiveMQ Consumer.
    Each consumer is identified by its long id.
    However each consumer also has a short id (an integer) to identify it more easily.
    """
    
    nConsumers = 0
    consumerIdList = []
    consumers = {}
    
    def __init__(self, longId):
        """
        Constructor
        """
        
        self.longId = longId
        self.shortId = Consumer.nConsumers
        
        self.connectionId, sessionId, value = longId.rsplit(':', 2)
        self.sessionId = int(sessionId)
        self.value = int(value)
        
        Consumer.consumers[longId] = self
        Consumer.consumerIdList.append(self.longId)
        Consumer.nConsumers += 1
    
    @classmethod
    def clearData(cls):
        """
        Deletes all information read about Consumers.
        
        Returns nothing.
        """
        
        cls.consumers.clear()
        cls.nConsumers = 0
        del cls.consumerIdList[:]
    
    @classmethod
    def getConsumerByLongId(cls, longId):
        """
        Returns a consumer given its long id.
        If there is no consumer with this long id yet, it will be created.
        """
        
        if longId not in cls.consumers:
            cls.consumers[longId] = Consumer(longId)

        return cls.consumers[longId]
    
    @classmethod
    def shortIdToLongId(cls, shortId):
        """
        Transforms a consumer's short id to a long id.
        
        Returns a long id.
        Throws an IndexError if the short id does not exist.
        """
        
        return cls.consumerIdList[shortId]
    
    @classmethod
    def longIdToShortId(cls, longId):
        """
        Transforms a consumer's long id to a short id.
        
        Returns a long id.
        Throws an KeyError if the long id does not exist.
        """
        return cls.consumers[longId].shortId
    
