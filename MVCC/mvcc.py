from storage import *
from element import *
import time

class MVCC :
    def __init__(self):
        '''
            Inisialisasi kelas MVCC
        '''
        return

    def setReadTimestampNow(self, i, timestamp, storage):
        '''
            Ubah read timestamp dari suatu transaksi
        '''
        storage.data[i].readTS = timestamp

    def setWriteTimestampNow(self, i, timestamp, storage):
        '''
            Ubah read timestamp dari suatu transaksi
        '''
        storage.data[i].writeTS = timestamp

    def read(self, key, nilai, storage, timestamp):
        '''
            Method untuk menerapkan aturan
            pembacaan MVCC
        '''

        idx = storage.findBiggestwriteTS(timestamp,key)
        nilai = storage.data[idx].val
        if (storage.data[idx].readTS > timestamp): # R-TS(Qk) > TS(Ti)
            return True
        else:
            self.setReadTimestampNow(idx, timestamp, storage)
            return True
    
    def write(self, key, nilai, storage, timestamp):
        '''
            Method untuk menerapkan aturan
            penulisan MVCC
        '''
        idx = storage.findBiggestwriteTS(timestamp,key)
        if (storage.data[idx].readTS > timestamp): # R-TS(Qk) > TS(Ti)
            # Rollback Ti
            return False
        else :
            if (storage.data[idx].writeTS == timestamp): # TS(Ti) = W-TS(Qk)
                # Overwrite konten Qk
                storage.data[idx].val = nilai
            else:
                # Create a new version Qi of Q
                latestVer = storage.findLatestVersion(key) + 1
                storage.addElement(key,nilai,timestamp,timestamp,latestVer)
            return True