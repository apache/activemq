"""
Module Producer
"""

class Producer(object):
    """
    This class represents an ActiveMQ Producer.
    Each producer is identified by its long id.
    However each producer also has a short id (an integer) to identify it more easily.
    """
    
    nProducers = 0
    producerIdList = []
    producers = {}
    
    def __init__(self, longId):
        """
        Constructor
        """
        
        self.longId = longId
        self.shortId = Producer.nProducers
        
        self.connectionId, sessionId, value = longId.rsplit(':', 2)
        self.sessionId = int(sessionId)
        self.value = int(value)
        
        Producer.producers[longId] = self
        Producer.producerIdList.append(self.longId)
        Producer.nProducers += 1
    
    @classmethod
    def clearData(cls):
        """
        Deletes all information read about producers.
        
        Returns nothing.
        """
        
        cls.producers.clear()
        cls.nProducers = 0
        del cls.producerIdList[:]
    
    @classmethod
    def getProducerByLongId(cls, longId):
        """
        Returns a producer given its long id.
        If there is no producer with this long id yet, it will be created.
        """
        
        if longId not in cls.producers:
            cls.producers[longId] = Producer(longId)

        return cls.producers[longId]
    
    @classmethod
    def getProducerByShortId(cls, shortId):
        """
        Returns a producer given its short id.
        If there is no producer with thi short id yet, IndexError will be thrown.
        """
        
        return cls.producers[cls.producerIdList[shortId]]
    
    @classmethod
    def exists(cls, longid):
        """
        Returns if a producer with the given long id exists.
        """
        
        return longid in cls.producers
    
    @classmethod
    def shortIdToLongId(cls, shortId):
        """
        Transforms a producer's short id to a long id.
        
        Returns a long id.
        Throws an IndexError if the short id does not exist.
        """
        
        return cls.producerIdList[shortId]
    
    @classmethod
    def longIdToShortId(cls, longId):
        """
        Transforms a producer's long id to a short id.
        
        Returns a long id.
        Throws an KeyError if the long id does not exist.
        """
        return cls.producers[longId].shortId

        