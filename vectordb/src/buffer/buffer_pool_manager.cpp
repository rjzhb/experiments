#include "buffer/buffer_pool_manager.h"
#include <exception>
#include <memory>
#include <mutex>

#include "common/exception.h"
#include "common/macros.h"
#include "fmt/core.h"
#include "storage/page/page.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager) {}

BufferPoolManager::~BufferPoolManager() = default;

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> l(latch_);
  *page_id = AllocatePage();
  pages_.emplace(*page_id, std::make_unique<Page>());
  return pages_.find(*page_id)->second.get();
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  auto iter = pages_.find(page_id);
  if (iter == pages_.end()) {
    fmt::println(stderr, "page not exist / invalid RID: {}", page_id);
    std::terminate();
  }
  return iter->second.get();
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool { return true; }

void BufferPoolManager::FlushAllPages() {}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  UNIMPLEMENTED("page guard not supported");
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { UNIMPLEMENTED("page guard not supported"); }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  UNIMPLEMENTED("page guard not supported");
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  UNIMPLEMENTED("page guard not supported");
}

}  // namespace bustub
