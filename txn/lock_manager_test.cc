// Author: Alexander Thomson (thomson@cs.yale.edu)
// Modified by: Kun Ren (kun.ren@yale.edu)

#include "txn/lock_manager.h"

#include <set>
#include <string>

#include "utils/testing.h"

using std::set;
using namespace std;

TEST(LockManagerA_1) {
  deque<Txn*> ready_txns;
  LockManagerA lm(&ready_txns);
  vector<Txn*> owners;

  Txn* t1 = reinterpret_cast<Txn*>(1);
  Txn* t2 = reinterpret_cast<Txn*>(2);
  Txn* t3 = reinterpret_cast<Txn*>(3);


  cout << "TEST 1" << endl;
  // Txn 1 acquires read lock.
  cout << "[ Txn 1 minta read lock ]" << endl;
  lm.ReadLock(t1, 101);
  ready_txns.push_back(t1);   // Txn 1 is ready.
  EXPECT_EQ(EXCLUSIVE, lm.Status(101, &owners));
  EXPECT_EQ(1, owners.size());
  EXPECT_EQ(t1, owners[0]);
  EXPECT_EQ(1, ready_txns.size());
  EXPECT_EQ(t1, ready_txns.at(0));

  // Txn 2 requests write lock. Not granted.
  cout << "[ Txn 2 minta write lock. Tapi tidak bisa langsung diberikan ]" << endl;
  lm.WriteLock(t2, 101);
  EXPECT_EQ(EXCLUSIVE, lm.Status(101, &owners));
  EXPECT_EQ(1, owners.size());
  EXPECT_EQ(t1, owners[0]);
  EXPECT_EQ(1, ready_txns.size());

  // Txn 3 requests read lock. Not granted.
  cout << "[ Txn 3 minta read lock. Tapi tidak bisa langsung diberikan ]" << endl;
  lm.ReadLock(t3, 101);
  EXPECT_EQ(EXCLUSIVE, lm.Status(101, &owners));
  EXPECT_EQ(1, owners.size());
  EXPECT_EQ(t1, owners[0]);
  EXPECT_EQ(1, ready_txns.size());

  // Txn 1 releases lock.  Txn 2 is granted write lock.
  cout << "[ Txn 1 melepaskan lock.  Txn 2 dapat write lock ]" << endl;
  lm.Release(t1, 101);
  EXPECT_EQ(EXCLUSIVE, lm.Status(101, &owners));
  EXPECT_EQ(1, owners.size());
  EXPECT_EQ(t2, owners[0]);
  EXPECT_EQ(2, ready_txns.size());
  EXPECT_EQ(t2, ready_txns.at(1));

  // Txn 2 releases lock.  Txn 3 is granted read lock.
  cout << "[ Txn 2 melepaskan lock.  Txn 3 dapat read lock ]" << endl;
  lm.Release(t2, 101);
  EXPECT_EQ(EXCLUSIVE, lm.Status(101, &owners));
  EXPECT_EQ(1, owners.size());
  EXPECT_EQ(t3, owners[0]);
  EXPECT_EQ(3, ready_txns.size());
  EXPECT_EQ(t3, ready_txns.at(2));
  END;
  cout << "END TEST 1" << endl;
}

TEST(LockManagerA_2) {
  deque<Txn*> ready_txns;
  LockManagerA lm(&ready_txns);
  vector<Txn*> owners;

  Txn* t1 = reinterpret_cast<Txn*>(1);
  Txn* t2 = reinterpret_cast<Txn*>(2);
  Txn* t3 = reinterpret_cast<Txn*>(3);
  Txn* t4 = reinterpret_cast<Txn*>(4);

  cout << "TEST 2" << endl;
  cout << "[ Txn 1 minta read lock ]" << endl;
  lm.ReadLock(t1, 101);   // Txn 1 acquires read lock.
  cout << "[ Txn 1 dimasukkan ke ready_txns ]" << endl;
  ready_txns.push_back(t1);  // Txn 1 is ready.
  cout << "[ Txn 2 minta write lock. Tapi tidak bisa langsung diberikan ]" << endl;
  lm.WriteLock(t2, 101);  // Txn 2 requests write lock. Not granted.
  cout << "[ Txn 3 minta read lock. Tapi tidak bisa langsung diberikan ]" << endl;
  lm.ReadLock(t3, 101);   // Txn 3 requests read lock. Not granted.
  cout << "[ Txn 4 minta read lock. Tapi tidak bisa langsung diberikan ]" << endl;
  lm.ReadLock(t4, 101);   // Txn 4 requests read lock. Not granted.
  cout << "[ Txn 2 cancel write lock request ]" << endl;
  lm.Release(t2, 101);    // Txn 2 cancels write lock request.

  // Txn 1 should now have a read lock (EXCLUSIVE LOCK) and Txns 3 and 4 should be next in line.
  cout << "[ Cek status T1 ]" << endl;
  EXPECT_EQ(EXCLUSIVE, lm.Status(101, &owners));
  EXPECT_EQ(1, owners.size());
  EXPECT_EQ(t1, owners[0]);

  // Txn 1 releases lock.  Txn 3 is granted read lock.
  cout << "[ Txn 1 melepaskan lock.  Txn 3 dapat read lock ]" << endl;
  lm.Release(t1, 101);
  EXPECT_EQ(EXCLUSIVE, lm.Status(101, &owners));
  EXPECT_EQ(1, owners.size());
  EXPECT_EQ(t3, owners[0]);
  EXPECT_EQ(2, ready_txns.size());
  EXPECT_EQ(t3, ready_txns.at(1));

  // Txn 3 releases lock.  Txn 4 is granted read lock.
  cout << "[ Txn 3 melepaskan lock.  Txn 4 dapat read lock ]"<<endl;
  lm.Release(t3, 101);
  EXPECT_EQ(EXCLUSIVE, lm.Status(101, &owners));
  EXPECT_EQ(1, owners.size());
  EXPECT_EQ(t4, owners[0]);
  EXPECT_EQ(3, ready_txns.size());
  EXPECT_EQ(t4, ready_txns.at(2));

  END;
  cout << "END TEST 2" << endl;
}

TEST(LockManagerA_3) {
  deque<Txn*> ready_txns;
  LockManagerA lm(&ready_txns);
  vector<Txn*> owners;

  Txn* t1 = reinterpret_cast<Txn*>(1);
  Txn* t2 = reinterpret_cast<Txn*>(2);
  Txn* t3 = reinterpret_cast<Txn*>(3);

  cout << "TEST 3" << endl;
  cout << "[ Txn 1 minta read lock ]" << endl;
  lm.ReadLock(t1, 101);   // Txn 1 acquires read lock.
 
  ready_txns.push_back(t1);  // Txn 1 is ready.

  cout << "[ Txn 2 minta write lock ]" << endl;
  lm.WriteLock(t2, 101);  // Txn 2 requests write lock. Not granted.
  
  cout << "[ Txn 1 melepaskan lock.  Txn 2 dapat write lock ]" << endl;
  lm.Release(t1, 101);
  EXPECT_EQ(EXCLUSIVE, lm.Status(101, &owners));
  EXPECT_EQ(1, owners.size());
  EXPECT_EQ(t2, owners[0]); // t2 dapat write lock

  cout << "[ Txn 3 minta read lock ]" << endl;
  lm.ReadLock(t3,101);

  cout << "[ Txn 1 minta write lock ]" << endl;
  lm.WriteLock(t1, 101);
  
  cout << "[ Txn 2 melepaskan lock.  Txn 3 dapat read lock ]" << endl;
  lm.Release(t2, 101);
  EXPECT_EQ(EXCLUSIVE, lm.Status(101, &owners));
  EXPECT_EQ(1, owners.size());
  EXPECT_EQ(t3, owners[0]); // t3 dapat read lock
  EXPECT_EQ(3, ready_txns.size());
  EXPECT_EQ(t3, ready_txns.back());

  cout << "[ Txn 3 melepaskan lock.  Txn 1 dapat write lock ]" << endl;
  lm.Release(t3, 101);
  EXPECT_EQ(EXCLUSIVE, lm.Status(101, &owners));
  EXPECT_EQ(1, owners.size());
  EXPECT_EQ(t1, owners[0]); // t1 dapat write lock
  EXPECT_EQ(4, ready_txns.size());
  EXPECT_EQ(t1, ready_txns.back());

  END;
  cout << "END TEST 3" << endl;
}


int main(int argc, char** argv) {
  LockManagerA_1();
  LockManagerA_2();
  LockManagerA_3();
}

