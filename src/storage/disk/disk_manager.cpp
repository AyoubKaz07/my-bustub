//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_manager.cpp
//
// Identification: src/storage/disk/disk_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sys/stat.h>
#include <cassert>
#include <cstring>
#include <iostream>
#include <mutex>  // NOLINT
#include <string>
#include <thread>  // NOLINT

#include "common/exception.h"
#include "common/logger.h"
#include "storage/disk/disk_manager.h"
#include <unistd.h>
#include <fcntl.h>

namespace bustub {

static char *buffer_used;

/**
 * Constructor: open/create a single database file & log file
 * @input db_file: database file name
 */
DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::string::size_type n = file_name_.rfind('.');
  if (n == std::string::npos) {
    LOG_DEBUG("wrong file format");
    return;
  }
  log_name_ = file_name_.substr(0, n) + ".log";

  // Open log file using file descriptor
  log_fd_ = open(log_name_.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  if (log_fd_ < 0) {
    throw Exception("can't open log file");
  }

  // Open db file using file descriptor
  db_fd_ = open(db_file.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  if (db_fd_ < 0) {
    close(log_fd_);
    throw Exception("can't open db file");
  }

  buffer_used = nullptr;
}


/**
 * Close all file streams
 */
void DiskManager::ShutDown() {
  {
    std::scoped_lock scoped_db_io_latch(db_io_latch_);
    close(db_fd_);  // Close the db file descriptor
  }
  close(log_fd_);  // Close the log file descriptor
}

/**
 * Write the contents of the specified page into disk file
 */
void DiskManager::WritePage(page_id_t page_id, const char *page_data) {
  std::scoped_lock scoped_db_io_latch(db_io_latch_);
  size_t offset = static_cast<size_t>(page_id) * BUSTUB_PAGE_SIZE;

  ssize_t bytes_written = pwrite(db_fd_, page_data, BUSTUB_PAGE_SIZE, offset);
  if (bytes_written == -1) {
    LOG_DEBUG("I/O error while writing with pwrite");
    return;
  }
  num_writes_ += 1;
}


/**
 * Read the contents of the specified page into the given memory area
 */
void DiskManager::ReadPage(page_id_t page_id, char *page_data) {
  std::scoped_lock scoped_db_io_latch(db_io_latch_);
  int offset = page_id * BUSTUB_PAGE_SIZE;
  // check if read beyond file length
  if (offset > GetFileSize(file_name_)) {
    LOG_DEBUG("I/O error reading past end of file");
  } else {

    ssize_t bytes_read = pread(db_fd_, page_data, BUSTUB_PAGE_SIZE, offset);
    if (bytes_read == -1) {
      LOG_DEBUG("I/O error while reading with pread");
      return;
    }

    // If file ends before reading BUSTUB_PAGE_SIZE
    if (bytes_read < BUSTUB_PAGE_SIZE) {
      LOG_DEBUG("Read less than a page");
      memset(page_data + bytes_read, 0, BUSTUB_PAGE_SIZE - bytes_read);
    }
  }
}

/**
 * Write the contents of the log into disk file
 * Only return when sync is done, and only perform sequence write
 */
void DiskManager::WriteLog(char *log_data, int size) {
  assert(log_data != buffer_used);
  buffer_used = log_data;

  if (size == 0) {  // no effect on num_flushes_ if log buffer is empty
    return;
  }

  flush_log_ = true;

  num_flushes_ += 1;
  ssize_t bytes_written = pwrite(log_fd_, log_data, size, 0);  // Always appending
  if (bytes_written == -1) {
    LOG_DEBUG("I/O error while writing log with pwrite");
    return;
  }

  flush_log_ = false;
}


/**
 * Read the contents of the log into the given memory area
 * Always read from the beginning and perform sequence read
 * @return: false means already reach the end
 */
bool DiskManager::ReadLog(char *log_data, int size, int offset) {
  if (offset >= GetFileSize(log_name_)) {
    return false;
  }

  ssize_t bytes_read = pread(log_fd_, log_data, size, offset);
  if (bytes_read == -1) {
    LOG_DEBUG("I/O error while reading log with pread");
    return false;
  }

  if (bytes_read < size) {
    memset(log_data + bytes_read, 0, size - bytes_read);
  }

  return true;
}

/**
 * Returns number of flushes made so far
 */
auto DiskManager::GetNumFlushes() const -> int { return num_flushes_; }

/**
 * Returns number of Writes made so far
 */
auto DiskManager::GetNumWrites() const -> int { return num_writes_; }

/**
 * Returns true if the log is currently being flushed
 */
auto DiskManager::GetFlushState() const -> bool { return flush_log_; }

/**
 * Private helper function to get disk file size
 */
auto DiskManager::GetFileSize(const std::string &file_name) -> int {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? static_cast<int>(stat_buf.st_size) : -1;
}

}  // namespace bustub
