//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_concurrent_test.cpp
//
// Identification: test/storage/b_plus_tree_concurrent_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * grading_b_plus_tree_checkpoint_2_concurrent_test.cpp
 */

#include <algorithm>
#include <chrono>  // NOLINT
#include <cstdio>
#include <functional>
#include <future>  // NOLINT
#include <random>
#include <thread>       // NOLINT
#include "test_util.h"  // NOLINT

#include "buffer/buffer_pool_manager_instance.h"
#include "gtest/gtest.h"
#include "storage/index/b_plus_tree.h"

// Macro for time out mechanism
#define TEST_TIMEOUT_BEGIN                           \
  std::promise<bool> promisedFinished;               \
  auto futureResult = promisedFinished.get_future(); \
                              std::thread([](std::promise<bool>& finished) {
#define TEST_TIMEOUT_FAIL_END(X)                                                                  \
  finished.set_value(true);                                                                       \
  }, std::ref(promisedFinished)).detach();                                                        \
  EXPECT_TRUE(futureResult.wait_for(std::chrono::milliseconds(X)) != std::future_status::timeout) \
      << "Test Failed Due to Time Out";

namespace bustub {
// helper function to launch multiple threads
template <typename... Args>
void LaunchParallelTest(uint64_t num_threads, uint64_t txn_id_start, Args &&...args) {
  std::vector<std::thread> thread_group;

  // Launch a group of threads
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group.push_back(std::thread(args..., txn_id_start + thread_itr, thread_itr));
  }

  // Join the threads with the main thread
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group[thread_itr].join();
  }
}

// helper function to insert
void InsertHelper(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &keys,
                  uint64_t tid, __attribute__((unused)) uint64_t thread_itr = 0) {
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(tid);
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree->Insert(index_key, rid, transaction);
  }
  delete transaction;
}

// helper function to seperate insert
void InsertHelperSplit(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &keys,
                       int total_threads, uint64_t tid, __attribute__((unused)) uint64_t thread_itr) {
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(tid);
  for (auto key : keys) {
    if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
      int64_t value = key & 0xFFFFFFFF;
      rid.Set(static_cast<int32_t>(key >> 32), value);
      index_key.SetFromInteger(key);
      tree->Insert(index_key, rid, transaction);
    }
  }
  delete transaction;
}

// helper function to delete
void DeleteHelper(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &remove_keys,
                  uint64_t tid, __attribute__((unused)) uint64_t thread_itr = 0) {
  GenericKey<8> index_key;
  // create transaction
  auto *transaction = new Transaction(tid);
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree->Remove(index_key, transaction);
  }
  delete transaction;
}

// helper function to seperate delete
void DeleteHelperSplit(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree,
                       const std::vector<int64_t> &remove_keys, int total_threads, uint64_t tid,
                       __attribute__((unused)) uint64_t thread_itr) {
  GenericKey<8> index_key;
  // create transaction
  auto *transaction = new Transaction(tid);
  for (auto key : remove_keys) {
    if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
      index_key.SetFromInteger(key);
      tree->Remove(index_key, transaction);
    }
  }
  delete transaction;
}

void LookupHelper(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &keys,
                  uint64_t tid, __attribute__((unused)) uint64_t thread_itr = 0) {
  auto *transaction = new Transaction(tid);
  GenericKey<8> index_key;
  RID rid;
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    std::vector<RID> result;
    bool res = tree->GetValue(index_key, &result, transaction);
    EXPECT_EQ(res, true);
    EXPECT_EQ(result.size(), 1);
    EXPECT_EQ(result[0], rid);
  }
  delete transaction;
}

const size_t NUM_ITERS = 100;
const size_t NUM_ITERS_DEBUG = 100;

void InsertTest1Call() {
  for (size_t iter = 0; iter < NUM_ITERS_DEBUG; iter++) {
    // create KeyComparator and index schema
    auto key_schema = ParseCreateStatement("a bigint");
    GenericComparator<8> comparator(key_schema.get());

    auto *disk_manager = new DiskManager("test.db");
    BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
    // create b+ tree
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
    // create and fetch header_page
    page_id_t page_id;
    auto header_page = bpm->NewPage(&page_id);
    (void)header_page;
    // keys to Insert
    std::vector<int64_t> keys;
    int64_t scale_factor = 100;
    for (int64_t key = 1; key < scale_factor; key++) {
      keys.push_back(key);
    }
    LaunchParallelTest(4, 0, InsertHelper, &tree, keys);

    std::vector<RID> rids;
    GenericKey<8> index_key;
    for (auto key : keys) {
      rids.clear();
      index_key.SetFromInteger(key);
      tree.GetValue(index_key, &rids);
      EXPECT_EQ(rids.size(), 1);

      int64_t value = key & 0xFFFFFFFF;
      EXPECT_EQ(rids[0].GetSlotNum(), value);
    }

    int64_t start_key = 1;
    int64_t current_key = start_key;
    index_key.SetFromInteger(start_key);
    for (auto iterator = tree.Begin(); iterator != tree.End(); ++iterator) {
      auto location = (*iterator).second;
      EXPECT_EQ(location.GetPageId(), 0);
      EXPECT_EQ(location.GetSlotNum(), current_key);
      current_key = current_key + 1;
    }

    EXPECT_EQ(current_key, keys.size() + 1);

    bpm->UnpinPage(HEADER_PAGE_ID, true);

    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
  }
}

void InsertTest2Call() {
  for (size_t iter = 0; iter < NUM_ITERS; iter++) {
    // create KeyComparator and index schema
    auto key_schema = ParseCreateStatement("a bigint");
    GenericComparator<8> comparator(key_schema.get());

    auto *disk_manager = new DiskManager("test.db");
    BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
    // create b+ tree
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
    // create and fetch header_page
    page_id_t page_id;
    auto header_page = bpm->NewPage(&page_id);
    (void)header_page;
    // keys to Insert
    std::vector<int64_t> keys;
    int64_t scale_factor = 400;
    for (int64_t key = 1; key < scale_factor; key++) {
      keys.push_back(key);
    }
    LaunchParallelTest(8, 0, InsertHelperSplit, &tree, keys, 2);

    std::vector<RID> rids;
    GenericKey<8> index_key;
    for (auto key : keys) {
      rids.clear();
      index_key.SetFromInteger(key);
      tree.GetValue(index_key, &rids);
      EXPECT_EQ(rids.size(), 1);
      int64_t value = key & 0xFFFFFFFF;
      EXPECT_EQ(rids[0].GetSlotNum(), value);
    }

    int64_t start_key = 1;
    int64_t current_key = start_key;
    index_key.SetFromInteger(start_key);
    for (auto iterator = tree.Begin(); iterator != tree.End(); ++iterator) {
      auto location = (*iterator).second;
      EXPECT_EQ(location.GetPageId(), 0);
      current_key = current_key + 1;
    }

    EXPECT_EQ(current_key, keys.size() + 1);

    bpm->UnpinPage(HEADER_PAGE_ID, true);

    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
  }
}

void DeleteTest1Call() {
  for (size_t iter = 0; iter < NUM_ITERS; iter++) {
    // create KeyComparator and index schema
    auto key_schema = ParseCreateStatement("a bigint");
    GenericComparator<8> comparator(key_schema.get());

    auto *disk_manager = new DiskManager("test.db");
    BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
    // create b+ tree
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
    // create and fetch header_page
    page_id_t page_id;
    auto header_page = bpm->NewPage(&page_id);
    (void)header_page;
    // sequential insert
    std::vector<int64_t> keys = {1, 2, 3, 4, 5};
    InsertHelper(&tree, keys, 1);

    std::vector<int64_t> remove_keys = {1, 5, 3, 4};
    LaunchParallelTest(2, 1, DeleteHelper, &tree, remove_keys);

    int64_t start_key = 2;
    int64_t current_key = start_key;
    int64_t size = 0;

    for (auto iterator = tree.Begin(); iterator != tree.End(); ++iterator) {
      auto location = (*iterator).second;
      EXPECT_EQ(location.GetPageId(), 0);
      EXPECT_EQ(location.GetSlotNum(), current_key);
      current_key = current_key + 1;
      size = size + 1;
    }

    EXPECT_EQ(size, 1);

    bpm->UnpinPage(HEADER_PAGE_ID, true);

    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
  }
}

void DeleteTest2Call() {
  for (size_t iter = 0; iter < NUM_ITERS; iter++) {
    // create KeyComparator and index schema
    auto key_schema = ParseCreateStatement("a bigint");
    GenericComparator<8> comparator(key_schema.get());

    auto *disk_manager = new DiskManager("test.db");
    BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
    // create b+ tree
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
    // create and fetch header_page
    page_id_t page_id;
    auto header_page = bpm->NewPage(&page_id);
    (void)header_page;

    // sequential insert
    std::vector<int64_t> keys = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    InsertHelper(&tree, keys, 1);

    std::vector<int64_t> remove_keys = {1, 4, 3, 2, 5, 6};
    LaunchParallelTest(2, 1, DeleteHelperSplit, &tree, remove_keys, 2);

    int64_t start_key = 7;
    int64_t current_key = start_key;
    int64_t size = 0;

    for (auto iterator = tree.Begin(); iterator != tree.End(); ++iterator) {
      auto location = (*iterator).second;
      EXPECT_EQ(location.GetPageId(), 0);
      EXPECT_EQ(location.GetSlotNum(), current_key);
      current_key = current_key + 1;
      size = size + 1;
    }

    EXPECT_EQ(size, 4);

    bpm->UnpinPage(HEADER_PAGE_ID, true);

    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
  }
}

void MixTest1Call() {
  for (size_t iter = 0; iter < 1; iter++) {
    // create KeyComparator and index schema
    auto key_schema = ParseCreateStatement("a bigint");
    GenericComparator<8> comparator(key_schema.get());

    auto *disk_manager = new DiskManager("test.db");
    BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
    // create b+ tree
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 3, 5);

    // create and fetch header_page
    page_id_t page_id;
    auto header_page = bpm->NewPage(&page_id);
    (void)header_page;
    // first, populate index
    std::vector<int64_t> for_insert;
    std::vector<int64_t> for_delete;
    size_t sieve = 2;  // divide evenly
    size_t total_keys = 10;
    for (size_t i = 1; i <= total_keys; i++) {
      if (i % sieve == 0) {
        for_insert.push_back(i);
      } else {
        for_delete.push_back(i);
      }
    }
    auto rng = std::default_random_engine{};
    std::shuffle(for_insert.begin(), for_insert.end(), rng);
    std::shuffle(for_delete.begin(), for_delete.end(), rng);
    for (auto key : for_delete) {
      std::cout << "For delete: " << key << std::endl;
    }
    // Insert all the keys to delete
    InsertHelper(&tree, for_delete, 1);
    LOG_DEBUG("INSERTED KEYS FOR DELETION");

    auto insert_task = [&](int tid) { InsertHelper(&tree, for_insert, tid); };
    auto delete_task = [&](int tid) { DeleteHelper(&tree, for_delete, tid); };
    std::vector<std::function<void(int)>> tasks;
    tasks.emplace_back(insert_task);
    tasks.emplace_back(delete_task);
    std::vector<std::thread> threads;
    size_t num_threads = 10;
    // Assigning functions to threads
    for (size_t i = 0; i < num_threads; i++) {
      threads.emplace_back(std::thread{tasks[i % tasks.size()], i});
      std::cout << "thread " << i << " assigned task " << i % tasks.size() << std::endl;
    }
    LOG_DEBUG("ASSIGNED TASKS");
    for (size_t i = 0; i < num_threads; i++) {
      std::cout << "thread " << i << " starting " << std::endl;
      threads[i].join();
    }
    sort(for_insert.begin(), for_insert.end());
    int64_t size = 0;

    for (auto iterator = tree.Begin(); iterator != tree.End(); ++iterator) {
      EXPECT_EQ(((*iterator).first).ToString(), for_insert[size]);
      size++;
    }

    EXPECT_EQ(size, for_insert.size());

    bpm->UnpinPage(HEADER_PAGE_ID, true);

    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
  }
}

const int T2 = 200;
void MixTest2Call() {
  for (size_t iter = 0; iter < T2; iter++) {
    // create KeyComparator and index schema
    //    LOG_DEBUG("iteration %lu", iter);
    auto key_schema = ParseCreateStatement("a bigint");
    GenericComparator<8> comparator(key_schema.get());

    auto *disk_manager = new DiskManager("test.db");
    BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
    // create b+ tree
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
    // create and fetch header_page
    page_id_t page_id;
    auto header_page = bpm->NewPage(&page_id);
    (void)header_page;

    // Add perserved_keys
    std::vector<int64_t> perserved_keys;
    std::vector<int64_t> dynamic_keys;
    size_t total_keys = 1000;
    size_t sieve = 5;
    for (size_t i = 1; i <= total_keys; i++) {
      if (i % sieve == 0) {
        perserved_keys.push_back(i);
      } else {
        dynamic_keys.push_back(i);
      }
    }
    InsertHelper(&tree, perserved_keys, 1);
    // Check there are 1000 keys in there
    size_t size;

    auto insert_task = [&](int tid) { InsertHelper(&tree, dynamic_keys, tid); };
    auto delete_task = [&](int tid) { DeleteHelper(&tree, dynamic_keys, tid); };
    auto lookup_task = [&](int tid) { LookupHelper(&tree, perserved_keys, tid); };

    std::vector<std::thread> threads;
    std::vector<std::function<void(int)>> tasks;
    tasks.emplace_back(insert_task);
    tasks.emplace_back(delete_task);
    tasks.emplace_back(lookup_task);

    size_t num_threads = 6;
    for (size_t i = 0; i < num_threads; i++) {
      threads.emplace_back(std::thread{tasks[i % tasks.size()], i});
    }
    for (size_t i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    size = 0;

    for (auto iterator = tree.Begin(); iterator != tree.End(); ++iterator) {
      if (((*iterator).first).ToString() % sieve == 0) {
        size++;
      }
    }

    EXPECT_EQ(size, perserved_keys.size());

    bpm->UnpinPage(HEADER_PAGE_ID, true);

    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
  }
}

void MixTest3Call() {
  for (size_t iter = 0; iter < NUM_ITERS; iter++) {
    // create KeyComparator and index schema
    auto key_schema = ParseCreateStatement("a bigint");
    GenericComparator<8> comparator(key_schema.get());

    auto *disk_manager = new DiskManager("test.db");
    BufferPoolManager *bpm = new BufferPoolManagerInstance(10, disk_manager);
    // create b+ tree
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);

    // create and fetch header_page
    page_id_t page_id;
    auto header_page = bpm->NewPage(&page_id);
    (void)header_page;
    // first, populate index
    std::vector<int64_t> for_insert;
    std::vector<int64_t> for_delete;
    size_t total_keys = 1000;
    for (size_t i = 1; i <= total_keys; i++) {
      if (i > 500) {
        for_insert.push_back(i);
      } else {
        for_delete.push_back(i);
      }
    }
    // Insert all the keys to delete
    InsertHelper(&tree, for_delete, 1);

    auto insert_task = [&](int tid) { InsertHelper(&tree, for_insert, tid); };
    auto delete_task = [&](int tid) { DeleteHelper(&tree, for_delete, tid); };

    std::vector<std::function<void(int)>> tasks;
    tasks.emplace_back(insert_task);
    tasks.emplace_back(delete_task);
    std::vector<std::thread> threads;
    size_t num_threads = 10;
    for (size_t i = 0; i < num_threads; i++) {
      threads.emplace_back(std::thread{tasks[i % tasks.size()], i});
    }
    for (size_t i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    int64_t size = 0;

    for (auto iterator = tree.Begin(); iterator != tree.End(); ++iterator) {
      EXPECT_EQ(((*iterator).first).ToString(), for_insert[size]);
      size++;
    }

    EXPECT_EQ(size, for_insert.size());

    bpm->UnpinPage(HEADER_PAGE_ID, true);

    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
  }
}

void MixTest4Call() {
  for (size_t iter = 0; iter < NUM_ITERS; iter++) {
    // create KeyComparator and index schema
    auto key_schema = ParseCreateStatement("a bigint");
    GenericComparator<8> comparator(key_schema.get());

    auto *disk_manager = new DiskManager("test.db");
    BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
    // create b+ tree
    BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);

    // create and fetch header_page
    page_id_t page_id;
    auto header_page = bpm->NewPage(&page_id);
    (void)header_page;
    // first, populate index
    std::vector<int64_t> for_insert;
    std::vector<int64_t> for_delete;
    size_t total_keys = 1000;
    for (size_t i = 1; i <= total_keys; i++) {
      if (i > total_keys / 2) {
        for_insert.push_back(i);
      } else {
        for_delete.push_back(i);
      }
    }
    // Insert all the keys to delete
    InsertHelper(&tree, for_delete, 1);
    int64_t size = 0;

    auto insert_task = [&](int tid) { InsertHelper(&tree, for_insert, tid); };
    auto delete_task = [&](int tid) { DeleteHelper(&tree, for_delete, tid); };

    std::vector<std::function<void(int)>> tasks;
    tasks.emplace_back(insert_task);
    tasks.emplace_back(delete_task);
    std::vector<std::thread> threads;
    size_t num_threads = 10;
    for (size_t i = 0; i < num_threads; i++) {
      threads.emplace_back(std::thread{tasks[i % tasks.size()], i});
    }
    for (size_t i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    for (auto iterator = tree.Begin(); iterator != tree.End(); ++iterator) {
      EXPECT_EQ(((*iterator).first).ToString(), for_insert[size]);
      size++;
    }

    EXPECT_EQ(size, for_insert.size());

    DeleteHelper(&tree, for_insert, 1);
    size = 0;

    for (auto iterator = tree.Begin(); iterator != tree.End(); ++iterator) {
      EXPECT_EQ(((*iterator).first).ToString(), for_insert[size]);
      size++;
    }
    EXPECT_EQ(size, 0);

    bpm->UnpinPage(HEADER_PAGE_ID, true);

    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
  }
}

/*
 * Score: 5
 * Description: Concurrently insert a set of keys.
 */
TEST(BPlusTreeTestC2Con, InsertTest1) {
  TEST_TIMEOUT_BEGIN
  InsertTest1Call();
  remove("test.db");
  remove("test.log");
  TEST_TIMEOUT_FAIL_END(1000 * 600)
}

/*
 * Score: 5
 * Description: Split the concurrent insert test to multiple threads
 * without overlap.
 */
TEST(BPlusTreeTestC2Con, InsertTest2) {
  TEST_TIMEOUT_BEGIN
  InsertTest2Call();
  remove("test.db");
  remove("test.log");
  TEST_TIMEOUT_FAIL_END(1000 * 600)
}

/*
 * Score: 5
 * Description: Concurrently delete a set of keys.
 */
TEST(BPlusTreeTestC2Con, DeleteTest1) {
  TEST_TIMEOUT_BEGIN
  DeleteTest1Call();
  remove("test.db");
  remove("test.log");
  TEST_TIMEOUT_FAIL_END(1000 * 600)
}

/*
 * Score: 5
 * Description: Split the concurrent delete task to multiple threads
 * without overlap.
 */
TEST(BPlusTreeTestC2Con, DeleteTest2) {
  TEST_TIMEOUT_BEGIN
  DeleteTest2Call();
  remove("test.db");
  remove("test.log");
  TEST_TIMEOUT_FAIL_END(1000 * 600)
}

/*
 * Score: 5
 * Description: First insert a set of keys.
 * Then concurrently delete those already inserted keys and
 * insert different set of keys. Check if all old keys are
 * deleted and new keys are added correctly.
 */
TEST(BPlusTreeTestC2Con, DISABLED_MixTest1) {
  TEST_TIMEOUT_BEGIN
  MixTest1Call();
  remove("test.db");
  remove("test.log");
  TEST_TIMEOUT_FAIL_END(1000 * 600)
}

/*
 * Score: 5
 * Description: Insert a set of keys. Concurrently insert and delete
 * a differnt set of keys.
 * At the same time, concurrently get the previously inserted keys.
 * Check all the keys get are the same set of keys as previously
 * inserted.
 */
TEST(BPlusTreeTestC2Con, DISABLED_MixTest2) {
  TEST_TIMEOUT_BEGIN
  MixTest2Call();
  remove("test.db");
  remove("test.log");
  TEST_TIMEOUT_FAIL_END(1000 * 600)
}

/*
 * Score: 5
 * Description: First insert a set of keys.
 * Then concurrently delete those already inserted keys and
 * insert different set of keys. Check if all old keys are
 * deleted and new keys are added correctly.
 */
TEST(BPlusTreeTestC2Con, DISABLED_MixTest3) {
  TEST_TIMEOUT_BEGIN
  MixTest3Call();
  remove("test.db");
  remove("test.log");
  TEST_TIMEOUT_FAIL_END(1000 * 600)
}

TEST(BPlusTreeTestC2Con, DISABLED_MixTest4) {
  TEST_TIMEOUT_BEGIN
  MixTest4Call();
  remove("test.db");
  remove("test.log");
  TEST_TIMEOUT_FAIL_END(1000 * 600)
}

/*
 * Score: 5
 * Description: The same test that has been run for checkpoint 1,
 * but added iterator for value checking
 */
TEST(BPlusTreeConcurrentTestC2Seq, InsertTest1) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  for (auto iterator = tree.Begin(); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);

  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

/*
 * Score: 5
 * Description: The same test that has been run for checkpoint 1
 * but added iterator for value checking
 */
TEST(BPlusTreeConcurrentTestC2Seq, InsertTest2) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  std::vector<int64_t> keys = {5, 4, 3, 2, 1};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  start_key = 3;
  current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); !iterator.IsEnd(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  bpm->UnpinPage(HEADER_PAGE_ID, true);

  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

/*
 * Score: 10
 * Description: Insert a set of keys, use GetValue and iterator to
 * check the the inserted keys. Then delete a subset of the keys.
 * Finally use the iterator to check the remained keys.
 */
TEST(BPlusTreeConcurrentTestC2Seq, DeleteTest1) {
  // create KeyComparator and index schema
  std::string create_stmt = "a bigint";
  auto key_schema = ParseCreateStatement(create_stmt);
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  for (auto iterator = tree.Begin(); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  std::vector<int64_t> remove_keys = {1, 5};
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }

  start_key = 2;
  current_key = start_key;
  int64_t size = 0;
  for (auto iterator = tree.Begin(); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, 3);

  bpm->UnpinPage(HEADER_PAGE_ID, true);

  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

/*
 * Score: 10
 * Description: Similar to DeleteTest2, except that, during the Remove step,
 * a different subset of keys are removed.
 */
TEST(BPlusTreeConcurrentTestC2Seq, DeleteTest2) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  //  std::vector<int64_t> keys;
  //  for (int64_t key = 1; key < 10000; key++) {
  //    keys.push_back(key);
  //  }
  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  std::vector<int64_t> remove_keys = {1, 5, 3, 4};
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }

  start_key = 2;
  current_key = start_key;
  int64_t size = 0;
  for (auto iterator = tree.Begin(); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);

  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

/*
 * Score: 10
 * Description: Insert 10000 keys. Use GetValue and the iterator to iterate
 * through the inserted keys. Then remove 9900 inserted keys. Finally, use
 * the iterator to check the correctness of the remaining keys.
 */
TEST(BPlusTreeConcurrentTestC2Seq, DISABLED_ScaleTest) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(12, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  int64_t scale = 10000;
  std::vector<int64_t> keys;
  for (int64_t key = 1; key < scale; key++) {
    keys.push_back(key);
  }

  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  for (auto iterator = tree.Begin(); iterator != tree.End(); ++iterator) {
    (void)*iterator;
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }
  EXPECT_EQ(current_key, keys.size() + 1);

  int64_t remove_scale = 9900;
  std::vector<int64_t> remove_keys;
  for (int64_t key = 1; key < remove_scale; key++) {
    remove_keys.push_back(key);
  }
  // std::random_shuffle(remove_keys.begin(), remove_keys.end());
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
    if (key == 9899) {
      // tree.Print(bpm);
    }
  }

  start_key = 9900;
  current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    (void)*iterator;
    current_key = current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, 100);

  bpm->UnpinPage(HEADER_PAGE_ID, true);

  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

/*
 * Score: 10
 * Description: Insert a set of keys. Concurrently insert and delete
 * a different set of keys.
 * At the same time, concurrently get the previously inserted keys.
 * Check all the keys get are the same set of keys as previously
 * inserted.
 */
TEST(BPlusTreeConcurrentTestC2Seq, DISABLED_SequentialMixTest) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(5, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // first, populate index
  std::vector<int64_t> for_insert;
  std::vector<int64_t> for_delete;
  size_t sieve = 2;  // divide evenly
  size_t total_keys = 1000;
  for (size_t i = 1; i <= total_keys; i++) {
    if (i % sieve == 0) {
      for_insert.push_back(i);
    } else {
      for_delete.push_back(i);
    }
  }

  // Insert all the keys, including the ones that will remain at the end and
  // the ones that are going to be removed next.
  for (size_t i = 0; i < total_keys / 2; i++) {
    int64_t insert_key = for_insert[i];
    int64_t insert_value = insert_key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(insert_key >> 32), insert_value);
    index_key.SetFromInteger(insert_key);
    tree.Insert(index_key, rid, transaction);

    int64_t delete_key = for_delete[i];
    int64_t delete_value = delete_key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(delete_key >> 32), delete_value);
    index_key.SetFromInteger(delete_key);
    tree.Insert(index_key, rid, transaction);
  }

  // Remove the keys in for_delete
  for (auto key : for_delete) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }

  // Only half of the keys should remain
  int64_t start_key = 2;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    EXPECT_EQ(((*iterator).first).ToString(), for_insert[size]);
    size++;
  }

  EXPECT_EQ(size, for_insert.size());

  bpm->UnpinPage(HEADER_PAGE_ID, true);

  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

}  // namespace bustub