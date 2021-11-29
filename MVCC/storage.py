from element import *

'''
    Storage ialah kelas dengan atribut data

    Data ialah array of Element
    Element mempunyai 5 atribut (Ref. element.py)
'''

class Storage:
    data = []
    
    def __init__(self, length):
        '''
            Inisialisasi storage.

            return: void
        '''
        for i in range(length):
            self.data.append(Element(i,0,0,0,0))

    def printStorage(self):
        '''
            Method untuk memprint storage
        '''
        for i in self.data:
            print("Key:",i.key)
            print("Nilai:",i.val)
            print("Read R-TS:",i.readTS)
            print("Read W-TS:",i.writeTS)
            print("Version:",i.version)
            print()

    def findLatestVersion(self,key):
        '''
            Method untuk mengembalikan versi terbaru dari suatu elemen
        '''
        version = 0
        for i in self.data:
            currKey = i.key
            currVer = i.version
            if (currKey == key and currVer > version):
                version = currVer
        return version

    def findBiggestIDX(self):
        '''
            Method untuk mengembalikan indeks
        '''
        return len(self.data)

    def findBiggestwriteTS(self,timestamp,key):
        '''
            Method to find idx that denote the version of Q
            whose write timestamp is the largest write timestamp
            less than or equal to TS(Ti)
        '''
        idx = 0
        wTS = self.data[0].writeTS
        for i in self.data:
            currKey = i.key
            currWTS = i.writeTS
            if (key == currKey and currWTS > wTS and currWTS <= timestamp): # Qi terdeteksi
                idx = i
                wTS = currWTS
        return idx


    def addElement(self,key,val,rTS,wTS,version):
        '''
            Method untuk menambah elemen baru pada data
        '''
        self.data.append(Element(key,val,rTS,wTS,version))