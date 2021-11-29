// Author: Alexander Thomson (thomson@cs.yale.edu)
// Modified by: Kun Ren (kun.ren@yale.edu)
//
// Lock manager implementing deterministic two-phase locking as described in
// 'The Case for Determinism in Database Systems'.

#include <deque>
#include <iostream>
#include "txn/lock_manager.h"


using namespace std;
using std::deque;

LockManager::~LockManager() {
  // Cleanup lock_table_
  for (auto it = lock_table_.begin(); it != lock_table_.end(); it++) {
    delete it->second;
  }
}


/**
 Mendapatkan deque dgn key = key atau membuatnya jika blm ada
*/
deque<LockManager::LockRequest>* LockManager::_getLockQueue(const Key& key) {
  deque<LockRequest> *dq = this->lock_table_[key];
  if (!dq) {
    // cout << "\tBuat Deque baru"<<endl;
    dq = new deque<LockRequest>();
    this->lock_table_[key] = dq;
  }
  return dq;
}

LockManagerA::LockManagerA(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}
bool LockManagerA::WriteLock(Txn* txn, const Key& key) {

  // Txn buat request lock exclusive
  LockRequest lreq(EXCLUSIVE, txn);

  // Mendapatkan deque dgn key = key atau membuatnya jika blm ada
  deque<LockRequest> *deque = _getLockQueue(key);

  deque->push_back(lreq);

  if (deque->size() == 1) { // Berarti bisa langsung diberikan locknya (immediately granted)
    // cout << "\tWrite Lock dapat langsung diberikan" << endl;
    return true;
  }else{
    // increment txn_waits untuk txn
    this->txn_waits_[txn]++;
  }
  // cout << "\tWrite Lock tidak dapat langsung diberikan" << endl;
  return false;
}

bool LockManagerA::ReadLock(Txn* txn, const Key& key) {
  // Txn buat request lock exclusive
  LockRequest lreq(EXCLUSIVE, txn);

  // Mendapatkan deque dgn key = key atau membuatnya jika blm ada
  deque<LockRequest> *deque = _getLockQueue(key);

  deque->push_back(lreq);

  if (deque->size() == 1) { // Berarti bisa langsung diberikan locknya (immediately granted)
    // cout << "\tRead Lock dapat langsung diberikan" << endl;
    return true;
  }else{
    // increment txn_waits untuk txn
    this->txn_waits_[txn]++;
  }
  // cout << "\tRead Lock tidak dapat langsung diberikan" << endl;
  return false;
}

void LockManagerA::Release(Txn* txn, const Key& key) {
  
  // Mendapatkan deque dgn key = key atau membuatnya jika blm ada
  deque<LockRequest> *deque = _getLockQueue(key);

  // cek jika txn di front queue  
  if (deque->front().txn_ == txn){
    // cout << "\tHapus front txn dari deque/lock table" << endl;
    deque->pop_front();
    // jika masih transaksi di lock table maka masukkan ke ready transaksi
    if (deque->size() > 0){
      // cout << "\tMasukkan ke ready_txn next front" << endl;
      this->ready_txns_->push_back(deque->front().txn_);
    } 
  }else{ // jika tidak di front queue maka tinggal hapus saja 
    for (auto iter = deque->begin(); iter < deque->end(); iter++) {
      if (iter->txn_ == txn) { 
          // cout << "\tHapus txn dari lock table" << endl;
          deque->erase(iter);
          break;
      }
    }
  }
}

LockMode LockManagerA::Status(const Key& key, vector<Txn*>* owners) {
  
  deque<LockRequest> *deque = _getLockQueue(key);
  if (deque->size() == 0) {
    // cout << "\tStatus UNLOCKED" << endl;
    return UNLOCKED;
  }else {
    owners->clear();
    owners->push_back(deque->front().txn_);
    // cout << "\tStatus EXCLUSIVE" << endl;
    return EXCLUSIVE;
  }
}

LockManagerB::LockManagerB(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}

bool LockManagerB::_addLock(LockMode mode, Txn* txn, const Key& key) {
  LockRequest rq(mode, txn);
  LockMode status = Status(key, nullptr);

  deque<LockRequest> *dq = _getLockQueue(key);
  dq->push_back(rq);

  bool granted = status == UNLOCKED;
  if (mode == SHARED) {
    granted |= _noExclusiveWaiting(key);
  } else {
    _numExclusiveWaiting[key]++;
  }

  if (!granted)
    txn_waits_[txn]++;

  return granted;
}


bool LockManagerB::WriteLock(Txn* txn, const Key& key) {
  return _addLock(EXCLUSIVE, txn, key);
}

bool LockManagerB::ReadLock(Txn* txn, const Key& key) {
  return _addLock(SHARED, txn, key);
}

void LockManagerB::Release(Txn* txn, const Key& key) {
  deque<LockRequest> *queue = _getLockQueue(key);

  for (auto it = queue->begin(); it < queue->end(); it++) {
    if (it->txn_ == txn) {
      queue->erase(it);
      if (it->mode_ == EXCLUSIVE) {
        _numExclusiveWaiting[key]--;
      }

      break;
    }
  }

  // Advance the lock, by making new owners ready.
  // Some in newOwners already own the lock.  These are not in
  // txn_waits_.
  vector<Txn*> newOwners;
  Status(key, &newOwners);

  for (auto&& owner : newOwners) {
    auto waitCount = txn_waits_.find(owner);
    if (waitCount != txn_waits_.end() && --(waitCount->second) == 0) {
      ready_txns_->push_back(owner);
      txn_waits_.erase(waitCount);
    }
  }
}

LockMode LockManagerB::Status(const Key& key, vector<Txn*>* owners) {
  deque<LockRequest> *dq = _getLockQueue(key);
  if (dq->empty()) {
    return UNLOCKED;
  }

  LockMode mode = EXCLUSIVE;
  vector<Txn*> txn_owners;
  for (auto&& lockRequest : *dq) {
    if (lockRequest.mode_ == EXCLUSIVE && mode == SHARED)
        break;

    txn_owners.push_back(lockRequest.txn_);
    mode = lockRequest.mode_;

    if (mode == EXCLUSIVE)
      break;
  }

  if (owners)
    *owners = txn_owners;

  return mode;
}

inline bool LockManagerB::_noExclusiveWaiting(const Key& key) {
  return _numExclusiveWaiting[key] == 0;
}




