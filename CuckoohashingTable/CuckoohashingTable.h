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

#define BUCKET_SIZE 4
#define BUCKET_NUM  512
#define CACHE_LINE_SIZE 64

namespace concurrent_lib {

template <typename KeyType,
          typename ValueType,
          class KeyHahser = std::hash<KeyType>,
          class KeyEqualChekcer = std::equal_to<KeyType>>
class CuckoohashingTable {
 public:
  CuckoohashingTable():table_(BUCKET_NUM) {
    locks_.resize(BUCKET_NUM);
  }
  ~CuckoohashingTable() {}

  bool Lookup(const KeyType& key) {
    return CuckooLookup(key, indexes);
  }

  // return true is inserting succeed.
  // return false is finding a duplicate value.
  // passing in rvalue.
  bool Insert(KeyType&& key, ValueType&& value) {
    return CuckooInsertLoop(key, value);
  }

 private:
  enum BucketInsertRetCode {
    INSERT,
    DUPLICATE,
    EMPTY
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

  typedef std::pair<KeyType, ValueType> Cell;
  class Bucket {
  private:
    // std::array is as efficient as array initialized as [], where
    // std::array provides more functions to set and get data.
//    std::array<uint64_t, BUCKET_SIZE> hashesArray_;
    std::array<typename std::aligned_storage<
             sizeof(Cell), alignof(Cell)>::type,
             BUCKET_SIZE> Cells_;
    std::bitset<BUCKET_SIZE> occupied_;
  public:
    inline bool IfOccupied(size_t i) {
      return occupied_[i];
    }

    inline const Cell& GetCell(size_t i) const {
      return *static_cast<const Cell*>(
      static_cast<const void*>(&Cells_[i]));
    }
  };

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

  bool CuckooLookup(const KeyType& key,
                    const std::pair<size_t, size_t>& indexes) {
    if (LookupOneBucket(key, indexes.first)) {
      return true;
    }

    if (LookupOneBucket(key, indexes.second)) {
      return true;
    }

    return false;
  }

  BucketInsertRetCode InsertOneBucket(size_t index, KeyType&& key, ValueType&& value) {

  }

  bool CuckooInsertLoop(KeyType&& key, ValueType&& value) {
    const size_t hashValue = GetHashValue(key);

    while (true) {
      auto indexes = TwoBucketsPos(hashValue);
      if (!LockTwo(table_.GetTableSizeBase(), indexes.first, indexes.second)) {
        continue;
      }

    }
      // Acquired two locks, start insert now.
    }

    BucketInsertRetCode code;

    code = InsertOneBucket(indexes.first,
                         std::forward<KeyType>(key),
                         std::forward<ValueType>(value));
    if (code == BucketInsertRetCode::INSERT) {
      return true;
    }

    if (code == BucketInsertRetCode::DUPLICATE) {
      return false;
    }

    code = InsertOneBucket(indexes.second,
                           std::forward<KeyType>(key),
                           std::forward<ValueType>(value));

    if (code == BucketInsertRetCode::INSERT) {
      return true;
    }

    if (code == BucketInsertRetCode::DUPLICATE) {
      return false;
    }


    // Cuckoo search path and cuckoo move


    // resize
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

  bool LockTwo(size_t tableSizeBase, size_t posFirst, size_t posSecond) {
    locks_[posFirst].lock();
    if (table_.GetTableSizeBase() != tableSizeBase) {
      locks_[posFirst].unlock();
      return false;
    }

    locks_[posSecond].lock();

    return true;
  }

private:
    Table table_;

    KeyEqualChekcer keyEqualChekcer;

    typedef KeyHahser Hasher_;

    std::vector<Spinlock> locks_;
};

}  // namespace concurrent_lib

#endif //CONCURRENTLIB_CUCOOHASHINGTABLE_H
