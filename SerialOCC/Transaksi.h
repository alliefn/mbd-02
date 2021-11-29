#include <set>
#include <iostream>
#include <chrono> 
#include <ctime>
#include <map>

using namespace std::chrono; 
using namespace std;

#define COMPLETE 1
#define INCOMPLETE 0
#define ABORT -1

class Transaksi
{
    private:
        static set<Transaksi *> listTransaksi;
        set<int *> writeSet;
        set<int *> readSet;
        map<int *,int> localMemmory;

    public:
        time_point<system_clock> startTimestamp;
        time_point<system_clock> validationTimestamp;
        time_point<system_clock> finishTimestamp;

        Transaksi();
        ~Transaksi();
        void read(int *var);
        void write(int *var, int value);
        void writeToDatabase();
        bool checkReadWriteSet(Transaksi* txn);
        void validate_CommitOrAbort();
};