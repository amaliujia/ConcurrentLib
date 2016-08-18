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

template <typename KeyType, typename ValueType>
class CuckoohashingTable {
 private:
  Table table_;

  typedef std::hash<KeyType> Hasher_;

 public:
  CuckoohashingTable() {}
  ~CuckoohashingTable() {}

  bool Lookup(const KeyType& key) {
    const size_t hashValue = GetHashValue(key);
    auto indexes = GetTwoIndexes(hashValue);
    return CuckooLookup(key, indexes);
  }

 private:
  typedef std::pair<KeyType, ValueType> Cell;
  class Bucket {
  private:
    // std::array is as efficient as array initialized as [], where
    // std::array provides more functions to set and get data.
    std::array<uint64_t, BUCKET_SIZE> hashesArray_;
    std::array<typename std::aligned_storage<
             sizeof(Cell), alignof(Cell)>::type,
             BUCKET_SIZE> Cells_;
    std::bitset<BUCKET_SIZE> occupied_;
  public:
    bool IfOccupied(size_t i) {
      return occupied_[i];
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

  inline size_t GetHashValue(KeyType key) const {
    return Hasher_(key);
  }

  inline std::pair<size_t, size_t> GetTwoIndexes(const size_t hashValue) const {
    const size_t curTableSize = table_.GetTableSize();
    size_t pos1 = hashValue % curTableSize;
    // 0xc6a4a7935bd1e995 is the hash constant from 64-bit MurmurHash2
    size_t pos2 = (hashValue & 0xffff) * 0xc6a4a7935bd1e995 % curTableSize;

    return std::pair<size_t, size_t>(pos1, pos2);
  };

  bool LookupOneBucket(const KeyType& key, size_t index) {
    Bucket& bucket = table_.GetBucket(index);

    for (size_t i = 0; i < BUCKET_SIZE; i++) {
      if (!bucket.IfOccupied(i)) {
        continue;
      }

      // compare keys
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
};

}  // namespace concurrent_lib

#endif //CONCURRENTLIB_CUCOOHASHINGTABLE_H
