// Author: Kun Ren (kun.ren@yale.edu)
// Modified by Daniel Abadi

#include "txn/mvcc_storage.h"

// Init the storage
void MVCCStorage::InitStorage() {
  for (int i = 0; i < 1000000;i++) {
    Write(i, 0, 0);
    Mutex* key_mutex = new Mutex();
    mutexs_[i] = key_mutex;
  }
}

// Free memory.
MVCCStorage::~MVCCStorage() {
  for (unordered_map<Key, deque<Version*>*>::iterator it = mvcc_data_.begin();
       it != mvcc_data_.end(); ++it) {
    delete it->second;          
  }
  
  mvcc_data_.clear();
  
  for (unordered_map<Key, Mutex*>::iterator it = mutexs_.begin();
       it != mutexs_.end(); ++it) {
    delete it->second;          
  }
  
  mutexs_.clear();
}

// Lock the key to protect its version_list. Remember to lock the key when you read/update the version_list 
void MVCCStorage::Lock(Key key) {
  mutexs_[key]->Lock();
}

// Unlock the key.
void MVCCStorage::Unlock(Key key) {
  mutexs_[key]->Unlock();
}

// MVCC Read
bool MVCCStorage::Read(Key key, Value* result, int txn_unique_id) {
  // CPSC 438/538:
  //
  // Implement this method!
  
  // Hint: Iterate the version_lists and return the verion whose write timestamp
  // (version_id) is the largest write timestamp less than or equal to txn_unique_id.

  // If there exists a record for the specified key, sets '*result' equal to
  // the value associated with the key and returns true, else returns false;
  // The third parameter is the txn_unique_id(txn timestamp), which is used for MVCC.
  if (!mvcc_data_.count(key)) {
    return false;
  }

  // get maximum version ID
  int maxID = 0;
  for (auto& elm : *mvcc_data_[key]) {
    int currID = elm->version_id_;
    if (currID <= txn_unique_id && currID > maxID) {
      maxID = currID;
    };
  }

  for (auto& elm : *mvcc_data_[key]) {
    if (elm->version_id_ == maxID) {
      if (elm->max_read_id_ < txn_unique_id) {
        elm->max_read_id_ = txn_unique_id;
      }
      *result = elm->value_;
      return true;
    }
  }

  return false;
}


// Check whether apply or abort the write
bool MVCCStorage::CheckWrite(Key key, int txn_unique_id) {
  // CPSC 438/538:
  //
  // Implement this method!
  
  // Hint: Before all writes are applied, we need to make sure that each write
  // can be safely applied based on MVCC timestamp ordering protocol. This method
  // only checks one key, so you should call this method for each key in the
  // write_set. Return true if this key passes the check, return false if not. 
  // Note that you don't have to call Lock(key) in this method, just
  // call Lock(key) before you call this method and call Unlock(key) afterward.
  
  // periksa jika ada
  if (!mvcc_data_.count(key)) {
    return false;
  }

  // get maximum version ID
  int maxID = 0;
  for (auto& elm : *mvcc_data_[key]) {
    int currID = elm->version_id_;
    if (currID <= txn_unique_id && currID > maxID) {
      maxID = currID;
    };
  }

  // check one key
  for (auto elm : *mvcc_data_[key]) {
    if (elm->version_id_ == maxID) {
      return (elm->max_read_id_ <= txn_unique_id);
    }
  }

  return true;
}

// MVCC Write, call this method only if CheckWrite return true.
void MVCCStorage::Write(Key key, Value value, int txn_unique_id) {
  // CPSC 438/538:
  //
  // Implement this method!
  
  // Hint: Insert a new version (malloc a Version and specify its value/version_id/max_read_id)
  // into the version_lists. Note that InitStorage() also calls this method to init storage. 
  // Note that you don't have to call Lock(key) in this method, just
  // call Lock(key) before you call this method and call Unlock(key) afterward.
  if (mvcc_data_.count(key)) {

    // get maximum version ID
    int maxID = 0;
    for (auto& elm : *mvcc_data_[key]) {
      int currID = elm->version_id_;
      if (currID <= txn_unique_id && currID > maxID) {
        maxID = currID;
      };
    }

    for (auto& elm : *mvcc_data_[key]) {
      // Misalkan terdapat TS(Ti) = W-TS(Qk) sehingga
      // cukup perlu overwrite konten saja
      if (elm->version_id_ == maxID && maxID == txn_unique_id) {
        // overwrite konten, lalu kembali
        elm->value_ = value;
        return;
      }
    }
  } else {
    // insert data baru
    mvcc_data_[key] = new std::deque<Version*>;
  }

  // struct untuk data baru
  Version *new_version = new Version;

  // Version {value_, max_read_id_, version_id_}
  *new_version = Version {value, txn_unique_id, txn_unique_id};

  // push ke mvcc_data_[key]
  mvcc_data_[key]->push_back(new_version);
}


