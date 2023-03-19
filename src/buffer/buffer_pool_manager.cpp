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
    pages_[free_frame].pin_count_ = 0;
  }

  page_id_t new_page_id = AllocatePage();
  // Add new page_id to pages
  pages_[free_frame].page_id_ = new_page_id;
  pages_[free_frame].pin_count_ = 1;   // At least one thread call this func

  // Establish the relation between page and frame in page table
  page_table_[new_page_id] = free_frame;
  // Pin the frame
  replacer_->SetEvictable(free_frame, false);
  *page_id = new_page_id;
  latch_.unlock();

  return &pages_[free_frame];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  latch_.lock();
  Page * target_page;
  // Search for page_id in the buffer pool
  if(page_table_.count(page_id) != 0) {
    auto target_frame = page_table_[page_id];
    target_page = &pages_[target_frame];
    target_page->pin_count_++;
    latch_.unlock();
    return target_page;
  }

  // If not found, pick a replacement frame
  target_page = NewPage(&page_id);
  if (target_page == nullptr) {
    // All frames in use and not evictable
    return nullptr;
  }
  // Read the page from the disk
  disk_manager_->ReadPage(page_id, target_page->data_);
  latch_.unlock();
  return target_page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  latch_.lock();
  // Page not found
  if(page_table_.count(page_id) == 0) {
    latch_.unlock();
    return false;
  }
  // pin_count less or equal than zero
  auto unpin_frame = page_table_[page_id];
  if(pages_[unpin_frame].pin_count_ <= 0) {
    latch_.unlock();
    return false;
  }

  // Decrease the pin_count
  pages_[unpin_frame].pin_count_--;
  if(pages_[unpin_frame].pin_count_ == 0) {
    // Firstly decrease to zero, should be set for evictable
    replacer_->SetEvictable(unpin_frame, true);
  }
  // Set dirty flag
  if(is_dirty) {
    pages_[unpin_frame].is_dirty_ = true;
  }

  latch_.unlock();
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  latch_.lock();
  if(page_table_.count(page_id) == 0) {
    // Page not found
    latch_.unlock();
    return false;
  }
  // Flush to disk
  auto target_frame = page_table_[page_id];
  if(pages_[target_frame].page_id_ == INVALID_PAGE_ID) {
    // Invalid page
    latch_.unlock();
    return false;
  }
  // Write to disk
  disk_manager_->WritePage(page_id, pages_[target_frame].data_);
  pages_[target_frame].is_dirty_ = false;
  latch_.unlock();
  return true;

}

void BufferPoolManager::FlushAllPages() {
  latch_.lock();
  // Flush all the pages in buffer pool
  for(int i=0; i<static_cast<int>(pool_size_); i++) {
    FlushPage(pages_[i].page_id_);
  }
  latch_.unlock();
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  latch_.lock();
  if(page_table_.count(page_id) == 0){
    latch_.unlock();
    return true;
  }
  auto delete_frame = page_table_[page_id];
  if(pages_[delete_frame].pin_count_ > 0) {
    // Pinned
    latch_.unlock();
    return false;
  }
  // Stop tracking and add delete frame to free list
  replacer_->Remove(delete_frame);
  page_table_.erase(page_id);
  free_list_.push_back(delete_frame);
  // Reset the data
  pages_[delete_frame].page_id_ = INVALID_PAGE_ID;
  pages_[delete_frame].ResetMemory();
  pages_[delete_frame].pin_count_ = 0;
  pages_[delete_frame].is_dirty_ = false;

  latch_.unlock();
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
