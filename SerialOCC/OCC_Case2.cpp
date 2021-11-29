#include "Transaksi.h"

int main(int argc, char const *argv[])
{
    int a = 1, b = 2;
    cout << "T1 start" << endl;
    Transaksi t1;

    cout << "T1 read item data b : " << b << endl;
    t1.read(&b);

    cout << "T2 start" << endl;
    Transaksi t2;

    cout << "T2 read item data b : " << b << endl;
    t2.read(&b);

    cout << "T1 write item data a menjadi 5" << endl;
    t1.write(&a,5);

    cout << "T2 read item data a : " << a << endl;
    t2.read(&a);

    cout << "Validasi T1" << endl;
    t1.validate_CommitOrAbort();

    cout << "Validasi T2" << endl;
    t2.validate_CommitOrAbort();

    cout << "Value item data saat ini : " << endl;
    cout << "a : " << a << " dan b : " << b << endl;
    
    return 0;
}
