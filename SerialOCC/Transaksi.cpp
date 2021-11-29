#include "Transaksi.h"

set<Transaksi *> Transaksi::listTransaksi;

// Constructor 
Transaksi::Transaksi()
{
    // startTimestamp diisi dengan waktu sekarang
    startTimestamp = chrono::system_clock::now();

    //inisialisasi validationTimestamp dan finishTimestamp
    validationTimestamp = chrono::time_point<system_clock>::max();
    finishTimestamp = chrono::time_point<system_clock>::max();
}

// Destructor
Transaksi::~Transaksi()
{
}

// prosedur membaca item data
void Transaksi::read(int *var){
    readSet.insert(var);
    localMemmory.insert(pair<int *,int>(var,*var));
}

// prosedure menuliskan item data pada local variables
void Transaksi::write(int *var, int value){
    writeSet.insert(var);
    localMemmory[var]=value;
}

// prosedure melakukan penulisan data pada database
void Transaksi::writeToDatabase(){
    set<int *>::iterator itr;
    for (itr = writeSet.begin(); itr != writeSet.end(); ++itr)
    {
        *(*itr) = localMemmory[*itr];
    }
    
}

// prekonsidi : startTS(T2) < finishTS(T1) < validationTS(T2)
// mengembalikan false jika terdapat konflik dan true jika tidak
bool Transaksi::checkReadWriteSet(Transaksi* txn){
    set<int *>::iterator i,j;

    // lakukan pengecekan write set
    // lakukan pengecekan apakah terdapat transaksi lain yang konkuren 
    // yang telah melakukan penulisan item data yang sama
    for (i = (*txn).writeSet.begin(); i != (*txn).writeSet.end(); ++i)
    {
        for (j = this->writeSet.begin(); j != this->writeSet.end(); ++j)
        {
            // jika terdapat transaksi konkuren yang sudah commit 
            // yang sudah melakukan penulisan terhadap item data yang sama
            // maka validasi gagal
            if (*i == *j)
            {
                return false;
            }
        }
    }

    // lakukan pengecekan read set
    // lakukan pengecekan apakah terdapat pembacaan item data
    // yang sudah dimodifikasi oleh transaksi konkuren lain yang sudah commit 
    for (i = (*txn).writeSet.begin(); i != (*txn).writeSet.end(); ++i)
    {
        for (j = this->readSet.begin(); j != this->readSet.end(); ++j)
        {
            // jika ada, maka validasi gagal
            if (*i == *j)
            {
                return false;
            }
        }
    }

    // dikembalikan nilai true karena tidak terdaoat irisan pada write set dan read set
    return true;
}

// prosedure dijalankan ketika Read and execution phase selesai
void Transaksi::validate_CommitOrAbort(){
    set<Transaksi *>::iterator i;
    bool valid = true;
    // validation phase
    // Lakukan pengecekan untuk setiap data yang berada pada read dan write set dari transaksi
    // pada validation test, tidak dibolehkan ada perubahan pada database 
    // jika perubahan data terakhir dilakukan setelah transaksi dimulai

    // catat validation timestamp (validationTS)
    this->validationTimestamp = system_clock::now();

    // jika listTransaksi kosong, berarti tidak ada transaksi konkuren yang timestamp-nya lebih kecil (awal) dari Transaksi saat ini. 
    // Validasi Transaksi berhasil dan bisa commit.
    if (listTransaksi.size() == 0)
    {
        //validasi sukses
        cout << "Validasi transaksi berhasil! Mulai penulisan data pada basis data." << endl;

        // write phase
        writeToDatabase();

        // catat finish timestamp (finishTS)
        this->finishTimestamp = system_clock::now();
        cout << "Commit transaksi." << endl;

        // masukkan transaksi yang sudah commit pada listTransaksi
        Transaksi::listTransaksi.insert(this);

    }
    else
    {
        for (i = listTransaksi.begin(); i != listTransaksi.end(); ++i)
        {
            if ((*i)!=this){
                if ((*i)->finishTimestamp < this->startTimestamp)
                {
                    // memenuhi kondisi pertama
                    // validasi untuk T dan *i sukses (tidak ada write set T dan *i yang konflik)
                }
                else if (this->startTimestamp < (*i)->finishTimestamp && (*i)->finishTimestamp < this->validationTimestamp)
                {
                    if(checkReadWriteSet(*i)){
                        // memenuhi kondisi kedua
                        // validasi untuk T dan *i sukses
                    }
                    else{
                        // validasi gagal
                        valid = false;
                        break;
                    }
                }
                // untuk kasus lain validasi gagal
                else
                {
                    valid = false;
                    break;
                }
                
            }
        }

        // write phase
        if (i == listTransaksi.end() && valid == true)
        {
            //Validasi berhasil
            cout << "Validasi transaksi berhasil! Mulai penulisan data pada basis data." << endl;
            writeToDatabase();

            // catat finish timestamp (finishTS)
            this->finishTimestamp = system_clock::now();
            cout << "Commit transaksi." << endl;

            // masukkan transaksi yang sudah commit pada listTransaksi
            Transaksi::listTransaksi.insert(this);
           
        }
        else
        {
            //Validasi gagal
            cout << "Validasi transaksi gagal! Rollback transaksi." << endl;
        }
        
    }
    
}