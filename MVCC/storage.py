class Storage :
    setdata = {}
    
    def __init__(self, length):
        #global setdata
        for i in range (length+1):
            self.put(i,0,0)

    def put(self,key, val, trans_id):
        newdata = {
                "value" : val,
                "timestamp" : 0
            }
        self.setdata[key] = newdata

    def get(self, key, trans_id):
        #return value kalo key ada, kalo gak return false
        hasil = self.setdata.get(key,False)
        print(hasil)
        if (hasil) :
            value = hasil["value"]
            return value
        else :
            return False

    def timestamp(self, key):
        hasil = self.setdata.get(key,False)
        if (hasil) :
            time = hasil["timestamp"]
            return time
        else :
            return 0