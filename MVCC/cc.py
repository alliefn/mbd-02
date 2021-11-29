from storage import Storage
from mvcc import MVCC

class CC:

    ''' MVCC processor '''
    mvcc = None

    ''' Queue untuk menyimpan instruksi transaksi '''
    inQueue = []

    ''' Queue berisi instruksi transaksi yang telah diabort '''
    inQueue_A = []

    ''' Storage object '''
    dS = None

    ''' Tabel transaksi dengan timestamp '''
    trTbl = {}

    ''' List yang berisi id transaksi yang telah di-abort '''
    trID_A = []

    def __init__(self, file : str, countItem : int):
        # file ialah nama file
        # countItem ialah jumlah item pada storage
        self.mvcc = MVCC()
        self.initQueueTr(file)
        self.dS = Storage(countItem)

    def initQueueTr(self, file):
        ''' 
            Isi file tidak boleh mengandung enter (\n)
            Prosedur ini akan membuat array berisi transaksi
            yang akan diproses dan instruksinya.
        '''
        self.inQueue = open(file, "r").read().split(" ")

    def run(self):
        tsCount = 0

        while (len(self.inQueue) != 0 or len(self.inQueue_A) != 0):

            # Inisialisasi transaksi yang pernah di-abort
            if (self.inQueue == []):
                self.inQueue = self.inQueue_A.copy()
                self.inQueue_A.clear()
                self.trTbl.clear()
                self.trID_A.clear()

            # Instruksi yang diproses
            currIn = self.inQueue.pop()

            # Tipe instruksi
            method = ""

            # ID transaksi
            trID = None

            # Timestamp transaksi
            trTS = None

            # Variabel untuk menyimpan key (objek)
            key = None

            # Variabel untuk menyimpan isi dari key (objek)
            value = None

            # Periksa jenis instruksi
            if (currIn.find("r") != -1): # instruksi ialah read
                method = "r"
                currIn_split = currIn.split(method)
                trID = int(currIn_split[0])
                key = int(currIn_split[1])

            elif(currIn.find("w") != -1): # instruksi ialah write
                method = "w"                
                currIn_split = currIn.split(method)
                trID = int(currIn_split[0])
                kvPair = currIn_split[1].split("|")

                key = int(kvPair[0])
                value = int(kvPair[1])

            else:
                print("Error pada "+str(currIn)+". Mohon gunakan r atau w")
                break
            
            if (len(currIn_split) < 2):
                print("Error pada "+str(currIn)+". Mohon gunakan format yang benar pada file test.txt (Contoh: 1r0 atau 2r1,100).")
                break

            # Periksa apakah transaction ID ada pada transaction table
            if (trID in self.trTbl.keys()):

                # Bila ada, assign nilai trTS dengan nilai transaction table dari trID
                trTS = self.trTbl[trID]

            else: # Bila belum ada, maka...

                trTS = tsCount # Assign nilai trTS dengan tsCount
                tsCount += 1
                self.trTbl[trID] = trTS

            # Cek transaksi telah diabort atau tidak
            if (trID in self.trID_A):

                # Pindahkan ke inQueue_A bila telah diabort
                self.inQueue_A.append(currIn)

            else:
                # Bila belum...

                # Jalankan read
                if (method == "r"):
                    result = self.mvcc.read(key, value, self.dS, trTS)

                # Jalankan write
                if (method == "w"):
                    result = self.mvcc.write(key, value, self.dS, trTS)
                
                # Bila status dari result false, transaksi diabort
                if (not result):
                    self.trID_A.append(trID)
                
                # Tambahkan instruksi ke queue ter-abort
                if (trID in self.trID_A):
                    self.inQueue_A.append(currIn)

            print("Curr Instruction: ", currIn)
            print("Instruction Queue: ", self.inQueue)
            print("Transaction Table: ", self.trTbl)
            print("Aborted Transactions ID: ", self.trID_A)
            print("Aborted instructions Queue: ", self.inQueue_A)
            print("Storage: \n")
            self.dS.printStorage()