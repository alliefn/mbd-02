// Author: Alexander Thomson (thomson@cs.yale.edu)
// Modified by: Christina Wallin (christina.wallin@yale.edu)
// Modified by: Kun Ren (kun.ren@yale.edu)


#include "txn/txn_processor.h"
#include <stdio.h>
#include <set>

#include "txn/lock_manager.h"

// Thread & queue counts for StaticThreadPool initialization.
#define THREAD_COUNT 8

TxnProcessor::TxnProcessor(CCMode mode)
    : mode_(mode), tp_(THREAD_COUNT), next_unique_id_(1) {
  if (mode_ == LOCKING_EXCLUSIVE_ONLY)
    lm_ = new LockManagerA(&ready_txns_);
  else if (mode_ == LOCKING)
    lm_ = new LockManagerB(&ready_txns_);

  // Create the storage
  if (mode_ == MVCC) {
    storage_ = new MVCCStorage();
  } else {
    storage_ = new Storage();
  }

  storage_->InitStorage();

  // Start 'RunScheduler()' running.
  cpu_set_t cpuset;
  pthread_attr_t attr;
  pthread_attr_init(&attr);
  CPU_ZERO(&cpuset);
  CPU_SET(0, &cpuset);
  CPU_SET(1, &cpuset);
  CPU_SET(2, &cpuset);
  CPU_SET(3, &cpuset);
  CPU_SET(4, &cpuset);
  CPU_SET(5, &cpuset);
  CPU_SET(6, &cpuset);
  pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);
  pthread_t scheduler_;
  pthread_create(&scheduler_, &attr, StartScheduler, reinterpret_cast<void*>(this));

}

void* TxnProcessor::StartScheduler(void * arg) {
  reinterpret_cast<TxnProcessor *>(arg)->RunScheduler();
  return NULL;
}

TxnProcessor::~TxnProcessor() {
  if (mode_ == LOCKING_EXCLUSIVE_ONLY || mode_ == LOCKING)
    delete lm_;

  delete storage_;
}

void TxnProcessor::NewTxnRequest(Txn* txn) {
  // Atomically assign the txn a new number and add it to the incoming txn
  // requests queue.
  mutex_.Lock();
  txn->unique_id_ = next_unique_id_;
  next_unique_id_++;
  txn_requests_.Push(txn);
  mutex_.Unlock();
}

Txn* TxnProcessor::GetTxnResult() {
  Txn* txn;
  while (!txn_results_.Pop(&txn)) {
    // No result yet. Wait a bit before trying again (to reduce contention on
    // atomic queues).
    sleep(0.000001);
  }
  return txn;
}

void TxnProcessor::RunScheduler() {
  switch (mode_) {
    case SERIAL:                 RunSerialScheduler(); break;
    case LOCKING:                RunLockingScheduler(); break;
    case LOCKING_EXCLUSIVE_ONLY: RunLockingScheduler(); break;
    case OCC:                    RunOCCScheduler(); break;
    case P_OCC:                  RunOCCParallelScheduler(); break;
    case MVCC:                   RunMVCCScheduler();
  }
}

void TxnProcessor::RunSerialScheduler() {
  Txn* txn;
  while (tp_.Active()) {
    // Get next txn request.
    if (txn_requests_.Pop(&txn)) {
      // Execute txn.
      ExecuteTxn(txn);

      // Commit/abort txn according to program logic's commit/abort decision.
      if (txn->Status() == COMPLETED_C) {
        ApplyWrites(txn);
        txn->status_ = COMMITTED;
      } else if (txn->Status() == COMPLETED_A) {
        txn->status_ = ABORTED;
      } else {
        // Invalid TxnStatus!
        DIE("Completed Txn has invalid TxnStatus: " << txn->Status());
      }

      // Return result to client.
      txn_results_.Push(txn);
    }
  }
}

void TxnProcessor::RunLockingScheduler() {
  Txn* txn;
  while (tp_.Active()) {
    // Start processing the next incoming transaction request.
    if (txn_requests_.Pop(&txn)) {
      bool blocked = false;
      // Request read locks.
      for (set<Key>::iterator it = txn->readset_.begin();
           it != txn->readset_.end(); ++it) {
        if (!lm_->ReadLock(txn, *it)) {
          blocked = true;
          // If readset_.size() + writeset_.size() > 1, and blocked, just abort
          if (txn->readset_.size() + txn->writeset_.size() > 1) {
            // Release all locks that already acquired
            for (set<Key>::iterator it_reads = txn->readset_.begin(); true; ++it_reads) {
              lm_->Release(txn, *it_reads);
              if (it_reads == it) {
                break;
              }
            }
            break;
          }
        }
      }

      if (blocked == false) {
        // Request write locks.
        for (set<Key>::iterator it = txn->writeset_.begin();
             it != txn->writeset_.end(); ++it) {
          if (!lm_->WriteLock(txn, *it)) {
            blocked = true;
            // If readset_.size() + writeset_.size() > 1, and blocked, just abort
            if (txn->readset_.size() + txn->writeset_.size() > 1) {
              // Release all read locks that already acquired
              for (set<Key>::iterator it_reads = txn->readset_.begin(); it_reads != txn->readset_.end(); ++it_reads) {
                lm_->Release(txn, *it_reads);
              }
              // Release all write locks that already acquired
              for (set<Key>::iterator it_writes = txn->writeset_.begin(); true; ++it_writes) {
                lm_->Release(txn, *it_writes);
                if (it_writes == it) {
                  break;
                }
              }
              break;
            }
          }
        }
      }

      // If all read and write locks were immediately acquired, this txn is
      // ready to be executed. Else, just restart the txn
      if (blocked == false) {
        ready_txns_.push_back(txn);
      } else if (blocked == true && (txn->writeset_.size() + txn->readset_.size() > 1)){
        mutex_.Lock();
        txn->unique_id_ = next_unique_id_;
        next_unique_id_++;
        txn_requests_.Push(txn);
        mutex_.Unlock();
      }
    }

    // Process and commit all transactions that have finished running.
    while (completed_txns_.Pop(&txn)) {
      // Commit/abort txn according to program logic's commit/abort decision.
      if (txn->Status() == COMPLETED_C) {
        ApplyWrites(txn);
        txn->status_ = COMMITTED;
      } else if (txn->Status() == COMPLETED_A) {
        txn->status_ = ABORTED;
      } else {
        // Invalid TxnStatus!
        DIE("Completed Txn has invalid TxnStatus: " << txn->Status());
      }

      // Release read locks.
      for (set<Key>::iterator it = txn->readset_.begin();
           it != txn->readset_.end(); ++it) {
        lm_->Release(txn, *it);
      }
      // Release write locks.
      for (set<Key>::iterator it = txn->writeset_.begin();
           it != txn->writeset_.end(); ++it) {
        lm_->Release(txn, *it);
      }

      // Return result to client.
      txn_results_.Push(txn);
    }

    // Start executing all transactions that have newly acquired all their
    // locks.
    while (ready_txns_.size()) {
      // Get next ready txn from the queue.
      txn = ready_txns_.front();
      ready_txns_.pop_front();

      // Start txn running in its own thread.
      tp_.RunTask(new Method<TxnProcessor, void, Txn*>(
            this,
            &TxnProcessor::ExecuteTxn,
            txn));

    }
  }
}

void TxnProcessor::ExecuteTxn(Txn* txn) {

  // Get the start time
  txn->occ_start_time_ = GetTime();

  // Read everything in from readset.
  for (set<Key>::iterator it = txn->readset_.begin();
       it != txn->readset_.end(); ++it) {
    // Save each read result iff record exists in storage.
    Value result;
    if (storage_->Read(*it, &result))
      txn->reads_[*it] = result;
  }

  // Also read everything in from writeset.
  for (set<Key>::iterator it = txn->writeset_.begin();
       it != txn->writeset_.end(); ++it) {
    // Save each read result iff record exists in storage.
    Value result;
    if (storage_->Read(*it, &result))
      txn->reads_[*it] = result;
  }

  // Execute txn's program logic.
  txn->Run();

  // Hand the txn back to the RunScheduler thread.
  completed_txns_.Push(txn);
}

void TxnProcessor::ApplyWrites(Txn* txn) {
  // Write buffered writes out to storage.
  for (map<Key, Value>::iterator it = txn->writes_.begin();
       it != txn->writes_.end(); ++it) {
    storage_->Write(it->first, it->second, txn->unique_id_);
  }
}


void TxnProcessor::RunOCCScheduler() {
  // Dapatkan request transaksi baru selanjutnya (bila ada yang pending) dan masukkan ke dalam execution thread
  while (tp_.Active()) {
    Txn *txn, *txn2;
    
    if (txn_requests_.Pop(&txn)) {
      // Begin phase
		  // Catat start time transaksi

      // Read and execution (modify) phase
      // Pada execution thread, jalankan operasi (read/write) yang ada pada transaksi txn
      tp_.RunTask(new Method<TxnProcessor, void, Txn*>(this, &TxnProcessor::ExecuteTxn, txn));
    }

    // Untuk transaksi yang sudah selesai (akan commit), lakukan tahap validation (validation  test)
    while (completed_txns_.Pop(&txn2)) {
      if (txn2->Status() == COMPLETED_A) {
          txn2->status_ = ABORTED;
      } else {
          // Validation phase
          // Lakukan pengecekan untuk setiap data yang berada pada read dan write set dari transaksi
          // pada validation test, tidak dibolehkan ada perubahan pada database 
          // jika perubahan data terakhir dilakukan setelah transaksi dimulai
			    // maka validasi gagal

          // inisialisasi validasi berhasil
          bool validate = true;

          // pengecekan read set
          for (auto&& item : txn2->readset_) {
            if (txn2->occ_start_time_ < storage_->Timestamp(item)) validate = false;
          }

          // pengecekan write set
          for (auto&& item : txn2->writeset_) {
            if (txn2->occ_start_time_ < storage_->Timestamp(item)) validate = false;
          }

          // Write phase (commit/restart)
          // jika validasi berhasil, lakukan penulisan data pada database dan commit transaksi
          if (validate) {
              // aplikasikan perubahan (lakukan penulisan) data pada database dengan memanggil fungsi ApplyWrites
              ApplyWrites(txn2);

              // Tandai transaksi menjadi sudah commit dengan mengubah status transaksi menjadi COMMITTED
              txn->status_ = COMMITTED;
            
          // jika terdapat transaksi lain (yang konkuren) yang telah melakukan modifikasi terhadap data yang akan ditulis, 
          // maka validasi gagal dan transaksi akan di rollback (hapus operasi yang sudah dijalankan dan mulai ulang tranksasi)
          } else {
              // hapus operasi (read/write) yang sudah dijalankan dan ubah status transaksi menjadi INCOMPLETE
              txn2->reads_.empty();
              txn2->writes_.empty();
              txn2->status_ = INCOMPLETE;

              // Mulai ulang transaksi
              this->mutex_.Lock();
              txn->unique_id_ = next_unique_id_;
              next_unique_id_++;
              this->txn_requests_.Push(txn2);
              this->mutex_.Unlock();
          }
      }
      this->txn_results_.Push(txn2);
    }
  }
}

void TxnProcessor::RunOCCParallelScheduler() {
  // CPSC 438/538:
  //
  // Implement this method! Note that implementing OCC with parallel
  // validation may need to create another method, like
  // TxnProcessor::ExecuteTxnParallel.
  // Note that you can use active_set_ and active_set_mutex_ we provided
  // for you in the txn_processor.h
  //
  // [For now, run serial scheduler in order to make it through the test
  // suite]
  RunSerialScheduler();
}

void TxnProcessor::RunMVCCScheduler() {
  // CPSC 438/538:
  //
  // Implement this method!

  // Hint:Pop a txn from txn_requests_, and pass it to a thread to execute.
  // Note that you may need to create another execute method, like TxnProcessor::MVCCExecuteTxn.
  //
  // [For now, run serial scheduler in order to make it through the test
  // suite]

  Txn *txn;

  while (tp_.Active()) {

    // Ambil elemen pertama dari transaksi (Lalu hapus dari transaksi),
    // segera lanjutkan ke prosedur MVCCExecuteTxn untuk mengeksekusinya
    if (txn_requests_.Pop(&txn)) {

      // Jalankan prosedur TxnProcessor::MVCCExecuteTxn untuk memproses transaksi
      tp_.RunTask(new Method<TxnProcessor, void, Txn*>(this, &TxnProcessor::MVCCExecuteTxn, txn));
    }
  }
}

void TxnProcessor::MVCCExecuteTxn(Txn* txn) {

  // Create a procedure to read elements from a particular set.
  /* auto readElm = [&](set<Key>& curset) -> void {
    for (auto& element : curset) {
      // Iff record exist in storage, save it to each read result.
      Value result;
      storage_->Lock(element);
      if (storage_->Read(element, &result, txn->unique_id_))
        txn->reads_[element] = result;
      storage_->Unlock(element);
    }
  };

  // Baca semua data untuk transaksi dari storage
  readElm(txn->readset_), readElm(txn->writeset_); */

  // Baca seluruh data yang diperlukan untuk transaksi ini
  // dari storage (Untuk writeset_)
  for (auto& elm : txn->readset_ ) {
    Value result;
    storage_->Lock(elm);

    // Save to each read result if record exist (and vice versa) in storage
    if (storage_->Read(elm, &result, txn->unique_id_))
      // reads_ ialah results of reads performed by the transaction.
      txn->reads_[elm] = result;
    storage_->Unlock(elm);
  }

  // Baca seluruh data yang diperlukan untuk transaksi ini
  // dari storage (Untuk writeset_)
  for (auto& elm : txn->writeset_ ) {
    Value result;
    storage_->Lock(elm);

    // Save to each read result if record exist (and vice versa) pada storage
    if (storage_->Read(elm, &result, txn->unique_id_))
      // reads_ ialah results of reads performed by the transaction.
      txn->reads_[elm] = result;
    storage_->Unlock(elm);
  }

  // Execute logic dari transaksi dengan melakukan Run()
  txn->Run();

  // Dalam hal ada transaksi yang 'dianggap'
  // telah selesai, pindahkan lagi ke thread RunScheduler
  completed_txns_.Push(txn);

  // Dapatkan semua kunci untuk write_set_
  MVCCLockWriteKeys(txn);

  // Call MVCCStorage::CheckWrite method to check all keys in the write_set_
  if (MVCCCheckWrites(txn)) {
    // Jika key melewati pemeriksaan apakah bisa write...

    // Lakukan write...
    ApplyWrites(txn);

    // ...lalu lepaskan semua kunci pada write_set_
    MVCCUnlockWriteKeys(txn);

    // Tandai bahwa transaksi
    // selesai dan push ke hasil txn
    txn->status_ = COMMITTED;
    txn_results_.Push(txn);

  } else {
    // Bila ada key yang tidak lolos pemeriksaan...

    // ...lepaskan semua lock pada write_set_
    MVCCUnlockWriteKeys(txn);

    // Bersihkan txn
    txn->reads_.clear();
    txn->writes_.clear();
    txn->status_ = INCOMPLETE;

    // Mulai ulang transaksi
    mutex_.Lock();
    txn->unique_id_ = next_unique_id_++;
    txn_requests_.Push(txn);
    mutex_.Unlock();
  }
}

bool TxnProcessor::MVCCCheckWrites(Txn* txn) {
  for (auto& elm : txn->writeset_) {
    if (storage_->CheckWrite(elm, txn->unique_id_))
      return true;
  }
  return false;
}

void TxnProcessor::MVCCLockWriteKeys(Txn* txn) {
  for (auto& elm : txn->writeset_) {
    storage_->Lock(elm);
  }
}

void TxnProcessor::MVCCUnlockWriteKeys(Txn* txn) {
  for (auto& elm : txn->writeset_) {
    storage_->Unlock(elm);
  }
}