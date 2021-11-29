import storage as strg
import time

class MVCC :
    def __init__(self) :
        return

    def getTimestampNow(self) :
        return time.time()

    def setTimestampNow(self, key, timestamp, storage : strg) :
        storage.setdata[key]['timestamp'] = timestamp

    def read(self, key, value, storage : strg, timestamp) :
        """ timestamp = self.getTimestampNow() """
        if (storage.setdata[key]['timestamp'] > timestamp) :
            # rollback transaction
            
            # reject operation
            return False
        else :
            value = storage.setdata[key]['value']
            self.setTimestampNow(key, timestamp, storage)
            return True
    
    def write(self, key, value, storage : strg, timestamp) :
        """ timestamp = self.getTimestampNow() """
        if (storage.setdata[key]['timestamp'] > timestamp) :
            # rollback transaction
            
            # reject operation
            return False
        else :
            storage.setdata[key]['value'] = value
            self.setTimestampNow(key, timestamp, storage)
            return True