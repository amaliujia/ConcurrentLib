//
// Created by Rui Wang on 16/8/16.
//

#ifndef CONCURRENTLIB_CUCOOHASHINGTABLE_H
#define CONCURRENTLIB_CUCOOHASHINGTABLE_H

#include <array>
#include <vector>
#include <functional>
#include <utility>
#include <bitset>
#include <mutex>
#include <atomic>
#include <memory>
#include <queue>

#define BUCKET_SIZE 4
#define BUCKET_NUM  512
#define CACHE_LINE_SIZE 64
#define MAX_STEP 128

namespace concurrent_lib {

template <typename KeyType,
          typename ValueType,
          class KeyHahser = std::hash<KeyType>,
          class KeyEqualChekcer = std::equal_to<KeyType>>
class CuckoohashingTable {
 public:
  CuckoohashingTable():table_(BUCKET_NUM) {
    //TODO: resize the lock
    // locks_.resize(BUCKET_NUM);
  }
  ~CuckoohashingTable() {}

  bool Lookup(const KeyType& key) {
      return CuckooLookupLoop(key);
  }

  // return true is inserting succeed.
  // return false is finding a duplicate value.
  // passing in rvalue.
  bool Insert(KeyType&& key, ValueType&& value) {
    return CuckooInsertLoop(key, value);
  }

  size_t Size() {
    return table_.GetTableSize();
  }

 private:
  enum BucketInsertRetCode {
    OK,
    INSERT,
    DUPLICATE
  };

  enum CuckooPathCode {
    GOOD,
    FULL,
    MAXSTEP,
    RESIZE
  };



  class Spinlock {
  private:
    std::atomic_flag lock_;
  public:
    Spinlock() {
      lock_.clear();
    }

    inline void lock() {
      while (lock_.test_and_set(std::memory_order_acquire));
    }

    inline void unlock() {
      lock_.clear(std::memory_order_release);
    }

    inline bool try_lock() {
      return !lock_.test_and_set(std::memory_order_acquire);
    }

  } __attribute__((aligned(64)));


  class Mutexlock {
  private:
    std::mutex lock_;

  public:
    inline void lock() {
      lock_.lock();
    }

    inline void unlock() {
      lock_.unlock();
    }

    inline bool try_lock() {
      return lock_.try_lock();
    }
  }__attribute__((aligned(64)));

  // exceptions
  class TableSizeException {};

  typedef std::pair<KeyType, ValueType> Cell;
  class Bucket {
  private:
    // std::array is as efficient as array initialized as [], where
    // std::array provides more functions to set and get data.
    std::array<char, BUCKET_SIZE> hashesArray_;
    std::array<typename std::aligned_storage<
             sizeof(Cell), alignof(Cell)>::type,
             BUCKET_SIZE> Cells_;
    std::bitset<BUCKET_SIZE> occupied_;
  public:
    inline bool IfOccupied(size_t i)  {
      return occupied_[i];
    }

    inline Cell& GetCell(size_t i) {
      return *static_cast<Cell*>(
      static_cast<void*>(&Cells_[i]));
    }

    inline void SetPartialKey(size_t i, char paritialKey) {
      hashesArray_[i] = paritialKey;
    }

    inline void SetOccupiedBit(size_t i) {
      occupied_.set(i);
    }

    inline void SetKeyValue(size_t i, KeyType&& key, ValueType&& value) {
      new (&Cells_[i]) Cell(std::move(key), std::move(value));
    }

    inline bool IfAvailable() {
      for (int i = 0; i < BUCKET_SIZE; i++) {
        if (!occupied_[i]) {
          return true;
        }
      }
      return false;
    }
  };

  /*
   * BucketMetadata class saves the index of buckets in table, and the locks acquired if any.
   * BucketMetadata is responsible to release locks.
   */
  template <size_t N>
  class BucketMetadata {
  private:
    std::array<size_t, N> indexes;
    size_t tableSize;
    CuckoohashingTable *map;
  public:
    BucketMetadata() {}
    BucketMetadata(const BucketMetadata& bucketMetadata) {
      tableSize = bucketMetadata.GetTableSize();
      for (int i = 0; i < N; i++) {
        indexes[i] = bucketMetadata.GetN(i);
      }
      map = bucketMetadata.GetMapPtr();
    }
    ~BucketMetadata() {
      // Has been called Release() somewhere
      if (map == nullptr) {
        return;
      }

      for (int i = 0; i < N; i++) {
        map->Unlock(indexes[i]);
      }
      map = nullptr;
    }

    inline void Release() {
      this->~BucketMetadata();
      map = nullptr;
    }

    inline void AddBucket(size_t i, size_t bucketPos) {
      if (i >= N) {
        // log error
      } else {
        indexes[i] = bucketPos;
      }
    }

    inline void AddTableSize(size_t i) {
      tableSize = i;
    }

    inline CuckoohashingTable* GetMapPtr() const {
      return map;
    }

    inline size_t GetTableSize() const {
      return tableSize;
    }

    bool Lock() {
      if (N == 2) {
        return map->LockTwo(tableSize, indexes[0], indexes[1]);
      }

      return false;
    }

    void Unlock() {
      if (N == 2) {
        map->UnlockTwo(indexes[0], indexes[1]);
      }
    }

    size_t inline GetN(size_t i) const {
      if (i >= N) {
        // log error
      } else {
        return indexes[i];
      }
    }
  };

  typedef BucketMetadata<2> TwoBucketMetadata;

  class Table {
   public:
    Table(size_t sizeBase): sizeBase_(sizeBase) {
      size_t size = size_t(1) << sizeBase;
      buckets_ = new Bucket[size];
    }

    ~Table() {
      DeallocMem();
    }

    inline size_t GetTableSize() const {
      return size_t(1) << sizeBase_.load(std::memory_order_acquire);
    }

    inline size_t GetTableSizeBase() {
      return sizeBase_.load(std::memory_order_acquire);
    }

    Bucket& GetBucket(size_t index) {
      return buckets_[index];
    }

    class CuckooPathNode {
    private:
      std::unique_ptr<TwoBucketMetadata> metadataPtr;

    public:
      CuckooPathNode(size_t size, size_t i, size_t j) {
        metadataPtr = new TwoBucketMetadata();
        metadataPtr->AddBucket(0, i);
        metadataPtr->AddBucket(1, j);
        metadataPtr->AddTableSize(size);
      }

      CuckooPathCode Swap() {
        if (metadataPtr->Lock() == false) {
          // If metadata lock fail, means table is been resize in this table
          // In this case, all the key are remapped already.
          // So cannot continue, must redo the insert.
          return RESIZE;
        }


        metadataPtr->Unlock();
        return GOOD;
      }
    };

   private:
    void DeallocMem() {
      delete[] buckets_;
    }

    std::atomic<size_t> sizeBase_;
    Bucket *buckets_;
  };

  inline size_t GetHashValue(const KeyType& key) const {
    return Hasher_(key);
  }

  inline std::pair<size_t, size_t> GetTwoIndexes(const size_t hashValue) const {
    const size_t curTableSize = table_.GetTableSize();
    size_t pos1 = hashValue % curTableSize;
    // 0xc6a4a7935bd1e995 is the hash constant from 64-bit MurmurHash2
    size_t pos2 = (hashValue & 0xf) * 0xc6a4a7935bd1e995 % curTableSize;

    return std::pair<size_t, size_t>(pos1, pos2);
  }

  bool LookupOneBucket(const KeyType& key, size_t index) {
    Bucket& bucket = table_.GetBucket(index);

    for (size_t i = 0; i < BUCKET_SIZE; i++) {
      if (!bucket.IfOccupied(i)) {
        continue;
      }

      // compare keys
      if (keyEqualChekcer(bucket.GetCell(i).first, key) == true) {
        return true;
      }
    }

    return false;
  }

  bool CuckooLookupLoop(const KeyType& key) {
      //auto indexes = TwoBucketsPos(GetHashValue(key));
      auto indexes = SnapshotAndLockTwo(GetHashValue(key));
      // lock should be released after this return.
      // because local variable is saved in stack.
      // indexes should be destructed after return.
      return CuckooLookup(key, indexes);
  }

  bool CuckooLookup(const KeyType& key,
                    const TwoBucketMetadata& indexes) {
    if (LookupOneBucket(key, indexes.GetN(0))) {
      return true;
    }

    if (LookupOneBucket(key, indexes.GetN(1))) {
      return true;
    }

    return false;
  }

  BucketInsertRetCode InsertOneBucket(size_t i, size_t index, char paritialKey, KeyType&& key, ValueType&& value) {
      Bucket& bucket = table_.GetBucket(index);

    bucket.SetOccupiedBit(i);
    bucket.SetPartialKey(i, paritialKey);
    bucket.SetKeyValue(i, std::forward<KeyType>(key), std::forward<ValueType>(value));

    return BucketInsertRetCode::INSERT;
  }

  BucketInsertRetCode CheckDuplicateBucket(size_t index_first, const KeyType& key, int& index) {
    Bucket &bucket_first = table_.GetBucket(index_first);

    index = -1;
    for (size_t i = 0; i < BUCKET_SIZE; i++) {
      if (bucket_first.IfOccupied(i)) {
        Cell &cell = bucket_first.GetCell(i);
        if (keyEqualChekcer(cell.first, key)) {
          return BucketInsertRetCode::DUPLICATE;
        }
      } else if (index == -1) {
        index = i;
      }
    }

    return BucketInsertRetCode::OK;
  }


  inline CuckooPathCode SearchCuckooPath(size_t tableSize, const TwoBucketMetadata& startBucket) {
    int depth = 2;
    std::queue<TwoBucketMetadata> bucketQueue;
    bucketQueue.push(TwoBucketMetadata(startBucket));

    while (depth <= MAX_STEP) {
      if (table_.GetTableSize() != tableSize) {
        // throw exception?
        return RESIZE;
      }

      // Poll last node from queue and get the second bucket index.
      // Based on second bucket index, find another bucket wihch is not equal to the first bucket index,
      // and that can move node, then create a CuckooPathNode, push the node to the end of queue.

      // then what should be the next bucket?
      // the first choice is a bucket with empty slots, if find it, stop search and return, ready to swap.
      // If not, find a full bucket (in order or randomly), push to the end of queue, continue util depth exceeds threshold.
    }
    return FULL;
  }

  inline CuckooPathCode SwapCuckooPath() {

  }

  bool CuckooInsertLoop(KeyType&& key, ValueType&& value) {
    const size_t hashValue = GetHashValue(key);

    while (true) {
      try {
        auto indexes = SnapshotAndLockTwo(hashValue);

        // Acquired two locks, start insert now.
        BucketInsertRetCode code;
        int index1, index2;
        code = CheckDuplicateBucket(indexes.GetN(0), key, index1);

        if (code == BucketInsertRetCode::DUPLICATE) {
          return false;
        }

        code = CheckDuplicateBucket(indexes.GetN(1), key, index1);

        if (code == BucketInsertRetCode::DUPLICATE) {
          return false;
        }

        if (index1 != -1) {
          InsertOneBucket(index1,
                          indexes.GetN(0),
                          PartialHashValue(hashValue),
                          std::forward<KeyType>(key),
                          std::forward<ValueType>(value));

        } else if (index2 != -1) {
          InsertOneBucket(index2,
                          indexes.GetN(1),
                          PartialHashValue(hashValue),
                          std::forward<KeyType>(key),
                          std::forward<ValueType>(value));
        } else {

          // Cuckoo search path and cuckoo move
          // If done this part, then insert is done.
          // else need to resize table
          // Has to RELEASE locks above.

          // release locks
          indexes.Release();

          CuckooPathCode code = SearchCuckooPath(indexes.GetTableSize(), indexes);

          if (code == MAXSTEP || code == FULL) {
            // do resize
            // and the throw a exception to restart
            // but how to make sure multi threads reszie the table at the same time.
            // hint: do check on tableSize when acquire all locks.

          } else if (code == GOOD) {
            SwapCuckooPath();
          } else {
            // what chould this one be?
          }
        }

        // Release the acquired locks at the beginning of loop.
        indexes.Release();
        return true;
      } catch(TableSizeException) {

      }
    }
  }

  inline size_t TableSize(size_t tableSizeBase) {
      return size_t(1) << tableSizeBase;
  }

  inline size_t HashMask(size_t tableSizeBase) {
      return TableSize(tableSizeBase) - 1;
  }

  inline char PartialHashValue(size_t hashValue) {
      return (char)(hashValue >> (sizeof(size_t) - sizeof(char)) * 8);
  }

  inline size_t IndexOff(size_t tableSizeBase, size_t hashValue) {
      return hashValue & HashMask(tableSizeBase);
  }

  inline size_t AlternativeIndexOff(size_t tableSizeBase, char partialHashValue, size_t pos) {
      char nonZeroTag = (partialHashValue >> 1 << 1) + 1;
      size_t hashOfTag = static_cast<size_t >(nonZeroTag * 0xc6a4a7935bd1e995);
      return (pos ^ hashOfTag) & HashMask(tableSizeBase);
  }

  std::pair<size_t, size_t> TwoBucketsPos(size_t hashValue) {
    size_t posFirst = IndexOff(table_.GetTableSizeBase(), hashValue);
    char paritial = PartialHashValue(hashValue);
    size_t posSecond = AlternativeIndexOff(table_.GetTableSizeBase(), paritial, posFirst);
    return std::pair<size_t, size_t>(posFirst, posSecond);
  };

  TwoBucketMetadata SnapshotAndLockTwo(size_t hashValue) {
    size_t tableSize = table_.GetTableSizeBase();
    size_t posFirst = IndexOff(tableSize, hashValue);
    char paritial = PartialHashValue(hashValue);
    size_t posSecond = AlternativeIndexOff(tableSize, paritial, posFirst);


    TwoBucketMetadata metadata;
    metadata.AddBucket(0, posFirst);
    metadata.AddBucket(1, posSecond);
    metadata.AddTableSize(tableSize);

    if (!LockTwo(tableSize, posFirst, posSecond)) {
      //return false? or throw a exception?
      return metadata;
    }

    return metadata;
  };

public:
  bool LockTwo(size_t tableSizeBase, size_t posFirst, size_t posSecond) {
    locks_[posFirst].lock();
    if (table_.GetTableSizeBase() != tableSizeBase) {
      locks_[posFirst].unlock();
      return false;
    }

    locks_[posSecond].lock();

    return true;
  }

  bool UnlockTwo(size_t posFirst, size_t posSecond) {
    locks_[posFirst].unlock();
    locks_[posSecond].unlock();
  }

  bool Unlock(TwoBucketMetadata& twoBucketMetadata) {
    locks_[twoBucketMetadata.GetN(0)].unlock();
    locks_[twoBucketMetadata.GetN(1)].unlock();
  }

  bool LockTwo(size_t tableSizeBase, TwoBucketMetadata& twoBucketMetadata) {
    locks_[twoBucketMetadata.GetN(0)].lock();
    if (table_.GetTableSizeBase() != tableSizeBase) {
      locks_[twoBucketMetadata.GetN(0)].unlock();
      return false;
    }

    locks_[twoBucketMetadata.GetN(1)].lock();

    return true;
  }

  void LockAll() {
  }

  void inline Unlock(size_t i) {
    locks_[i].unlock();
  }

private:
    Table table_;

    KeyEqualChekcer keyEqualChekcer;

    typedef KeyHahser Hasher_;

    std::array<Spinlock, BUCKET_NUM> locks_;
};
}  // namespace concurrent_lib

#endif //CONCURRENTLIB_CUCOOHASHINGTABLE_H
