//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  page_id_t new_page_id = AllocatePage();
  // TODO
  latch_.lock();
  frame_id_t free_frame;

  if(!free_list_.empty()) {
    // Get free frame_id_t
    free_frame = free_list_.front();
    free_list_.pop_front();

  } else {
    // Not exists free page, replacer victim a LRU page
    if (!replacer_->Evict(&free_frame)) {
      // No frames can be evicted.
      latch_.unlock();
      return nullptr;
    }

    // If page is dirty, then write it to the disk first
    if (pages_[free_frame].is_dirty_) {
      if (!FlushPage(pages_[free_frame].page_id_)) {
        // Failed to write the page to disk
        latch_.unlock();
        return nullptr;
      }
    }

    // Reset the memory and the metadata
    //    auto rpc_page = pages_[free_frame];
    pages_[free_frame].ResetMemory();
    pages_[free_frame].page_id_ = INVALID_PAGE_ID;
    pages_[free_frame].is_dirty_ = false;
    pages_[free_frame].pin_count_ = 1;   // At least one thread call this func
  }
  // Add new page_id to pages
  pages_[free_frame].page_id_ = new_page_id;

  // Establish the relation between page and frame in page table
  page_table_[new_page_id] = free_frame;
  // Pin the frame
  replacer_->SetEvictable(free_frame, false);
  *page_id = new_page_id;
  latch_.unlock();

  return &pages_[free_frame];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  // TODO:

  return nullptr;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  // TODO
  return false;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  // TODO
  return false;
}

void BufferPoolManager::FlushAllPages() {
  // TODO
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  // TODO
  return false;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
