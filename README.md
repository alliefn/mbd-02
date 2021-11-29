# Tugas Besar 2 IF3140 Manajemen Basis Data
Kelompok 1 Kelas K01

# Simple Locking (exclusive locks only)
Implementation Simple Locking (exclusive locks only) DBMS Menggunakan Bahasa C++
<br><br>
Untuk menjalankan hasil implementasi, ketik perintah berikut pada terminal ubuntu
```
make test
```

# Multiversion Timestamp Ordering Concurrency Control (MVCC)
Implementation Simple Locking (exclusive locks only) DBMS Menggunakan Bahasa C++
<br><br>
Untuk menjalankan hasil implementasi, ketik perintah berikut pada terminal ubuntu
```
make test
```

# Serial Optimistic Concurrency Control (OCC)
Implementation Multiversion Timestamp Ordering Concurrency Control DBMS Menggunakan Bahasa C++
<br><br>
## Test Case 1 OCC (Validasi Kedua Transaksi Berhasil)
Untuk menjalankan test case 1<br>
Masuk ke dalam folder SerialOCC, lalu ketik perintah berikut pada terminal ubuntu
```
g++ OCC_Case1.cpp Transaksi.cpp
./a.out
```
<br><br>

## Test Case 2 OCC (Terdapat Irisan WriteSet T1 dengan ReadSet T2)
Untuk menjalankan test case 2 <br>
Masuk ke dalam folder SerialOCC, lalu ketik perintah berikut pada terminal ubuntu
```
g++ OCC_Case2.cpp Transaksi.cpp
./a.out
```
<br><br>

## Test Case 3 OCC (Terdapat Irisan WriteSet T1 dengan WriteSet T2)
Untuk menjalankan test case 3 <br>
Masuk ke dalam folder SerialOCC, lalu ketik perintah berikut pada terminal ubuntu
```
g++ OCC_Case3.cpp Transaksi.cpp
./a.out
```