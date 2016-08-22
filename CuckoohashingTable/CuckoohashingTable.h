//
// Created by Rui Wang on 16/8/16.
//

#ifndef CONCURRENTLIB_CUCOOHASHINGTABLE_H
#define CONCURRENTLIB_CUCOOHASHINGTABLE_H

#include <array>
#include <functional>
#include <utility>
#include <bitset>

#define BUCKET_SIZE 4
#define BUCKET_NUM  512
#define CACHE_LINE_SIZE 64

namespace concurrent_lib {

template <typename KeyType,
          typename ValueType,
          class KeyEqualChekcer = std::equal_to<KeyType>>
class CuckoohashingTable {
 private:
    Table table_;

    KeyEqualChekcer keyEqualChekcer;

    typedef std::hash<KeyType> Hasher_;


 public:
  CuckoohashingTable() {}
  ~CuckoohashingTable() {}

  bool Lookup(const KeyType& key) {
    const size_t hashValue = GetHashValue(key);
    auto indexes = GetTwoIndexes(hashValue);
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
    Table() {
      buckets_ = new Bucket[BUCKET_NUM];
    }

    ~Table() {
      DeallocMem();
    }

    inline size_t GetTableSize() const {
      return BUCKET_NUM;
    }

    Bucket& GetBucket(size_t index) {
      return buckets_[index];
    }

   private:
    void DeallocMem() {
      delete[] buckets_;
    }


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
    auto indexes = GetTwoIndexes(hashValue);

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

  inline size_t AlternativeIndexOff(size_t tableSizeBase, size_t partialHashValue, size_t pos) {
      char nonZeroTag = (partialHashValue >> 1 << 1) + 1;
      size_t hashOfTag = static_cast<size_t >(nonZeroTag * 0xc6a4a7935bd1e995);
      return (pos ^ hashOfTag) & HashMask(tableSizeBase);
  }

  std::pair<size_t, size_t> TwoBucketsPos(size_t hashValue) {

  };
};

}  // namespace concurrent_lib

#endif //CONCURRENTLIB_CUCOOHASHINGTABLE_H
