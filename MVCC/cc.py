from storage import Storage
from mvcc import MVCC
class CC :
    """ The type of algorithm to use """
    algorithm = None
    """ Queue for storing transaction instructions"""
    inst_queue = []
    """ Queue yang berisi instruksi dari transaksi yang telah di-abort """
    inst_queue_A = []
    """ Storage object """
    data_store = None
    """ Transaction tables with their timestamps """    
    trans_table = {}
    """ List yang berisi id transaksi yang telah di-abort"""
    trans_id_A = []

    def __init__(self, algorithm : str, test_case_filename : str, nbItem : int):
        self.initAlgorithm(algorithm)
        self.initInstQueue(test_case_filename)
        self.data_store = Storage(nbItem)

    def initAlgorithm(self, string):
        if (string == "Simple Locking"):
            self.algorithm = SimpleLocking()
        elif (string == "OCC"):
            self.algorithm = OCC()
        elif (string == "MVCC"):
            self.algorithm = MVCC()

    def initInstQueue(self, test_case_filename):
        self.inst_queue = open(test_case_filename, "r").read().split(" ")

        #menghilangkan '\n' diakhir string jika ada
        if (self.inst_queue[-1][-1] == '\n'):
            self.inst_queue[-1] = self.inst_queue[-1][0:-1]

    def run(self):
        timestampCount = 0

        while (self.inst_queue != [] or self.inst_queue_A != []):
            #Menginisialisasi ulang untuk instruksi yang sebelumnya di-abort
            if (self.inst_queue == []):
                self.inst_queue = self.inst_queue_A.copy()
                self.inst_queue_A.clear()
                self.trans_table.clear()
                self.trans_id_A.clear()

            curr_inst = self.inst_queue.pop()
            type_method = ""
            trans_id = None
            trans_timestamp = None
            key = None
            value = None

            #Mengambil id, key, dan jenis operasi instruction paling atas queue
            if (curr_inst.find("r") != -1):
                type_method = "r"
                curr_inst_split = curr_inst.split(type_method)
                trans_id = int(curr_inst_split[0])
                key = int(curr_inst_split[1])
            elif(curr_inst.find("w") != -1):
                type_method = "w"                
                curr_inst_split = curr_inst.split(type_method)
                trans_id = int(curr_inst_split[0])
                kv_pair = curr_inst_split[1].split(",")
                key = int(kv_pair[0])
                value = int(kv_pair[1])
            else :
                print("Error at instruction : ", curr_inst)
                break
            
            if (len(curr_inst_split) < 2):
                print("Error at instruction : ", curr_inst)
                break

            #Menambahkan id transaksi ke transaction table dan mengambil transaction timestamp
            if (trans_id in self.trans_table.keys()):
                trans_timestamp = self.trans_table[trans_id]
            else:
                trans_timestamp = timestampCount
                timestampCount += 1
                self.trans_table[trans_id] = trans_timestamp

            #Jika instruksi sekarang merupakan instruksi yang di-abort, pindahkan curr_inst ke dalam inst_queue_A
            if (trans_id in self.trans_id_A):
                self.inst_queue_A.append(curr_inst)
            else :
                #Jalankan operasi read dari algoritma
                if (type_method == "r"):
                    result = self.algorithm.read(key, value, self.data_store, trans_timestamp)
                #Jalankan operasi write dari algoritma
                if (type_method == "w"):
                    result = self.algorithm.write(key, value, self.data_store, trans_timestamp)
                
                #Add atau remove trans id dari list id transaksi yang di abort
                if (not result):
                    self.trans_id_A.append(trans_id)
                
                #Add instruksi ke queue instruksi yang di abort
                if (trans_id in self.trans_id_A):
                    self.inst_queue_A.append(curr_inst)

            print("Current inst : ", curr_inst)
            print("Instruction Queue : ", self.inst_queue)
            print("Transaction Table : ", self.trans_table)
            print("Aborted Transactions id : ", self.trans_id_A)
            print("Aborted instructions Queue: ", self.inst_queue_A)
            print("Storage : ", self.data_store.setdata, "\n")