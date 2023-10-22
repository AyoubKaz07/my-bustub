#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  // std::cout << "进入getvalue  " << key << std::endl;
  if (IsEmpty()) {
    return false;
  }

  Page *page = FindLeaf(key);
  if (page == nullptr) {
    return false;
  }
  auto leaf_page = reinterpret_cast<LeafPage *>(page->GetData());

  int index = leaf_page->KeyIndex(key, comparator_);
  /*std::cout << "searching " << key << "index found: " << index << "\n";
  std::cout << "Value at index: " << leaf_page->ValueAt(index) << "\n";
  std::cout << "Size of leaf page: " << leaf_page->GetSize() << "\n";
  std::cout << "key at index: " << leaf_page->KeyAt(index) << "\n";*/
  if (index < leaf_page->GetSize() && comparator_(leaf_page->KeyAt(index), key) == 0) {
    result->push_back(leaf_page->ValueAt(index));
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return true;
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeaf(const KeyType &key) -> Page * {
  if (IsEmpty()) {
    return nullptr;
  }
  Page *curr_page = buffer_pool_manager_->FetchPage(root_page_id_);
  while (true) {
    if (curr_page == nullptr) {
      return nullptr;
    }
    if (root_page_id_ == curr_page->GetPageId()) {
      break;
    }
    buffer_pool_manager_->UnpinPage(curr_page->GetPageId(), false);
    curr_page = buffer_pool_manager_->FetchPage(root_page_id_);
  }
  auto curr_inter_page = reinterpret_cast<InternalPage *>(curr_page->GetData());
  while (!curr_inter_page->IsLeafPage()) {
    Page *next_page = buffer_pool_manager_->FetchPage(curr_inter_page->Find(key, comparator_));
    buffer_pool_manager_->UnpinPage(curr_page->GetPageId(), false);
    auto next_inter_page = reinterpret_cast<InternalPage *>(next_page->GetData());
    curr_page = next_page;
    curr_inter_page = next_inter_page;
  }
  return curr_page;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  Page *page = FindLeaf(key);

  while (page == nullptr) {
    if (IsEmpty()) {
      page_id_t page_id;
      page = buffer_pool_manager_->NewPage(&page_id);
      auto leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
      leaf_page->Init(page_id, INVALID_PAGE_ID, leaf_max_size_);
      root_page_id_ = page_id;
      UpdateRootPageId();
      buffer_pool_manager_->UnpinPage(page_id, true);
    }

    page = FindLeaf(key);
  }
  auto leaf_page = reinterpret_cast<LeafPage *>(page->GetData());

  int index = leaf_page->KeyIndex(key, comparator_);
  if (!leaf_page->Insert(key, value, index, comparator_)) {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return false;
  }
  if (leaf_page->GetSize() == leaf_max_size_) {
    // Create new page
    page_id_t new_page_id;
    Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);
    auto new_leaf_page = reinterpret_cast<LeafPage *>(new_page->GetData());
    new_leaf_page->Init(new_page_id, INVALID_PAGE_ID, leaf_max_size_);

    // Move half of the data to new page
    leaf_page->Split(new_page);

    // Update parent page
    InsertIntoParent(page, new_leaf_page->KeyAt(0), new_page);
    buffer_pool_manager_->UnpinPage(new_page_id, true);
  }

  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  return true;
}

/*
 * Custom method to insert the middle key to parent
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(Page *leaf, const KeyType &key, Page *child) {
  auto leaf_page = reinterpret_cast<BPlusTreePage *>(leaf->GetData());

  if (leaf_page->GetParentPageId() == INVALID_PAGE_ID) {
    // create new page
    page_id_t page_id;
    Page *new_page = buffer_pool_manager_->NewPage(&page_id);
    auto new_root = reinterpret_cast<InternalPage *>(new_page->GetData());

    // Update the parent node pointers of all child pages of parent page.
    new_root->Init(page_id, INVALID_PAGE_ID, internal_max_size_);
    new_root->SetValueAt(0, leaf->GetPageId());
    new_root->SetKeyAt(1, key);
    new_root->SetValueAt(1, child->GetPageId());
    new_root->IncreaseSize(2);

    auto sibling_page = reinterpret_cast<BPlusTreePage *>(child->GetData());
    // Update the parent page id of child pages
    leaf_page->SetParentPageId(page_id);
    sibling_page->SetParentPageId(page_id);

    root_page_id_ = page_id;
    buffer_pool_manager_->UnpinPage(page_id, true);
    return;
  }
  // parent page exists
  page_id_t parent_page_id = leaf_page->GetParentPageId();
  Page *parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
  auto parent_inter = reinterpret_cast<InternalPage *>(parent_page->GetData());

  if (parent_inter->GetSize() < parent_inter->GetMaxSize()) {
    // parent page has space
    auto child_page = reinterpret_cast<InternalPage *>(child->GetData());
    int index = parent_inter->KeyIndex(key, comparator_);
    parent_inter->Insert(key, child_page->GetPageId(), index, comparator_);
    child_page->SetParentPageId(parent_page_id);

    buffer_pool_manager_->UnpinPage(parent_page_id, true);
    return;
  }
  // parent has no space
  page_id_t parent_child_id;
  Page *parent_child_page = buffer_pool_manager_->NewPage(&parent_child_id);
  auto parent_child_inter = reinterpret_cast<InternalPage *>(parent_child_page->GetData());
  parent_child_inter->Init(parent_child_id, INVALID_PAGE_ID, parent_inter->GetMaxSize());

  // Diff splitting than leafs, includes pointes adjustment
  parent_inter->Split(key, child, parent_child_page, buffer_pool_manager_, comparator_);

  // Update next page id for both pages
  /*parent_child_inter->SetNextPageId(parent_inter->GetNextPageId());
  parent_inter->SetNextPageId(parent_child_id);*/

  InsertIntoParent(parent_page, parent_child_inter->KeyAt(0), parent_child_page);

  buffer_pool_manager_->UnpinPage(parent_child_id, true);
  buffer_pool_manager_->UnpinPage(parent_page_id, true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  Page *page = FindLeaf(key);
  if (page == nullptr) {
    return;
  }
  DeleteEntry(page, key);
}

/*
 * Custom method to delete the entry from the leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeleteEntry(Page *N_page, const KeyType &key) {
  auto N_node = reinterpret_cast<BPlusTreePage *>(N_page->GetData());
  if (N_node->IsLeafPage()) {
    auto leaf_page = reinterpret_cast<LeafPage *>(N_page->GetData());
    // if couldn't delete return immediately
    if (!leaf_page->Delete(key, comparator_)) {
      return;
    }
  } else {
    auto internal_page = reinterpret_cast<InternalPage *>(N_page->GetData());
    // if couldn't delete return immediately
    if (!internal_page->Delete(key, comparator_)) {
      return;
    }
  }
  // N is root and now empty tree
  if (N_node->IsRootPage()) {
    if (N_node->IsLeafPage() && N_node->GetSize() == 0) {
      root_page_id_ = INVALID_PAGE_ID;
      UpdateRootPageId();
      buffer_pool_manager_->UnpinPage(N_node->GetPageId(), true);
      buffer_pool_manager_->DeletePage(N_node->GetPageId());
      return;
    }
    // N is root and has one more child
    if (N_node->GetPageType() == IndexPageType::INTERNAL_PAGE && N_node->IsRootPage() && N_node->GetSize() == 1) {
      this->Print(buffer_pool_manager_);
      // print the only key/value pair left
      auto inter_node = reinterpret_cast<InternalPage *>(N_page->GetData());
      root_page_id_ = inter_node->ValueAt(0);
      UpdateRootPageId();
      buffer_pool_manager_->UnpinPage(N_node->GetPageId(), true);
      buffer_pool_manager_->DeletePage(N_node->GetPageId());
      return;
    }
    buffer_pool_manager_->UnpinPage(N_page->GetPageId(), true);
    return;
  }
  // N is not root and has too few entries
  if (N_node->GetSize() < N_node->GetMinSize()) {
    Page *NP_page;
    // key in between N and NP in parent(N);
    KeyType KeyPrime;
    bool is_pred;

    auto N_node_inter = reinterpret_cast<InternalPage *>(N_page->GetData());
    auto parent_page_id = N_node_inter->GetParentPageId();
    auto parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
    auto parent_node_inter = reinterpret_cast<InternalPage *>(parent_page->GetData());

    N_node_inter->FindNeighbor(N_node->GetPageId(), NP_page, KeyPrime, is_pred, buffer_pool_manager_);
    auto NP_node = reinterpret_cast<BPlusTreePage *>(NP_page->GetData());
    // entries in N & NP can fit in a single node
    if (NP_node->GetSize() + N_node->GetSize() <= N_node->GetMaxSize()) {
      /*Start Coalesce*/
      if (!is_pred) {
        auto temp_page = N_page;
        N_page = NP_page;
        NP_page = temp_page;

        auto temp_node = N_node;
        N_node = NP_node;
        NP_node = temp_node;
      }
      if (!N_node->IsLeafPage()) {
        auto NP_inter_node = reinterpret_cast<InternalPage *>(NP_page->GetData());
        NP_inter_node->Merge(N_page, KeyPrime, buffer_pool_manager_);
      } else {
        auto N_node_leaf = reinterpret_cast<LeafPage *>(N_page->GetData());
        auto NP_node_leaf = reinterpret_cast<LeafPage *>(NP_page->GetData());
        NP_node_leaf->Merge(N_page, buffer_pool_manager_);
        NP_node_leaf->SetNextPageId(N_node_leaf->GetNextPageId());
      }
      buffer_pool_manager_->UnpinPage(NP_page->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(N_page->GetPageId(), true);
      buffer_pool_manager_->DeletePage(N_page->GetPageId());
      DeleteEntry(parent_page, KeyPrime);
    } else {
      if (is_pred) {
        if (!N_node->IsLeafPage()) {
          auto NP_node_inter = reinterpret_cast<InternalPage *>(NP_page->GetData());
          auto N_node_inter = reinterpret_cast<InternalPage *>(N_page->GetData());

          page_id_t last_value = NP_node_inter->ValueAt(NP_node_inter->GetSize() - 1);
          KeyType last_key = NP_node_inter->KeyAt(NP_node_inter->GetSize() - 1);

          NP_node_inter->DeleteLast(last_key, comparator_);
          N_node_inter->InsertFirst(last_key, last_value);

          auto child_page = buffer_pool_manager_->FetchPage(last_value);
          auto child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
          child_node->SetParentPageId(N_node->GetPageId());
          buffer_pool_manager_->UnpinPage(last_value, true);

          int index_to_insert = parent_node_inter->KeyIndex(KeyPrime, comparator_);
          parent_node_inter->SetKeyAt(index_to_insert, last_key);

          buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
          buffer_pool_manager_->UnpinPage(NP_page->GetPageId(), true);
          buffer_pool_manager_->UnpinPage(N_page->GetPageId(), true);
        } else {
          auto NP_node_leaf = reinterpret_cast<LeafPage *>(NP_page->GetData());
          auto N_node_leaf = reinterpret_cast<LeafPage *>(N_page->GetData());

          ValueType first_value = NP_node_leaf->ValueAt(NP_node_leaf->GetSize() - 1);
          KeyType first_key = NP_node_leaf->KeyAt(NP_node_leaf->GetSize() - 1);

          NP_node_leaf->DeleteLast(first_key, comparator_);
          N_node_leaf->InsertFirst(first_key, first_value);

          int index_to_insert = parent_node_inter->KeyIndex(KeyPrime, comparator_);
          parent_node_inter->SetKeyAt(index_to_insert, first_key);

          buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
          buffer_pool_manager_->UnpinPage(NP_page->GetPageId(), true);
          buffer_pool_manager_->UnpinPage(N_page->GetPageId(), true);
        }
      } else {
        if (!N_node->IsLeafPage()) {
          auto NP_node_inter = reinterpret_cast<InternalPage *>(NP_page->GetData());
          auto N_node_inter = reinterpret_cast<InternalPage *>(N_page->GetData());

          page_id_t last_value = NP_node_inter->ValueAt(0);
          KeyType last_key = NP_node_inter->KeyAt(1);

          NP_node_inter->DeleteFirst(last_key, comparator_);
          N_node_inter->InsertLast(last_key, last_value);

          auto child_page = buffer_pool_manager_->FetchPage(last_value);
          auto child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
          child_node->SetParentPageId(N_node->GetPageId());
          buffer_pool_manager_->UnpinPage(last_value, true);

          int index_to_insert = parent_node_inter->KeyIndex(KeyPrime, comparator_);
          parent_node_inter->SetKeyAt(index_to_insert, last_key);

          buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
          buffer_pool_manager_->UnpinPage(NP_page->GetPageId(), true);
          buffer_pool_manager_->UnpinPage(N_page->GetPageId(), true);
        } else {
          auto NP_node_leaf = reinterpret_cast<LeafPage *>(NP_page->GetData());
          auto N_node_leaf = reinterpret_cast<LeafPage *>(N_page->GetData());

          ValueType last_value = NP_node_leaf->ValueAt(0);
          KeyType last_key = NP_node_leaf->KeyAt(0);

          NP_node_leaf->DeleteFirst(last_key, comparator_);
          N_node_leaf->InsertLast(last_key, last_value);

          int index_to_insert = parent_node_inter->KeyIndex(KeyPrime, comparator_);
          parent_node_inter->SetKeyAt(index_to_insert, last_key);

          buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
          buffer_pool_manager_->UnpinPage(NP_page->GetPageId(), true);
          buffer_pool_manager_->UnpinPage(N_page->GetPageId(), true);
        }
      }
    }
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { 
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE();
  }
  Page *curr_page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto curr_inter_node = reinterpret_cast<InternalPage *>(curr_page->GetData());
  while (!curr_inter_node->IsLeafPage()) {
    Page* next_page = buffer_pool_manager_->FetchPage(curr_inter_node->ValueAt(0));
    auto next_inter_node = reinterpret_cast<InternalPage *>(next_page->GetData());

    buffer_pool_manager_->UnpinPage(curr_page->GetPageId(), false);

    curr_page = next_page;
    curr_inter_node = next_inter_node;
  }

  return INDEXITERATOR_TYPE(curr_page, curr_page->GetPageId(), 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { 
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE();
  }
  auto leaf_page = FindLeaf(key);
  auto leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());

  int index = 0;
  for (; index < leaf_node->GetSize(); index++) {
    if (comparator_(leaf_node->KeyAt(index), key) == 0) {
      break;
    }
  }
  // return end, just like <iterator>
  if (index == leaf_node->GetSize()) {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return End();
  }
  return INDEXITERATOR_TYPE(leaf_page, leaf_page->GetPageId(), index, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { 
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE();
  }
  Page *curr_page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto curr_inter_node = reinterpret_cast<InternalPage *>(curr_page->GetData());
  while (!curr_inter_node->IsLeafPage()) {
    Page* next_page = buffer_pool_manager_->FetchPage(curr_inter_node->ValueAt(curr_inter_node->GetSize()-1));
    auto next_inter_node = reinterpret_cast<InternalPage *>(next_page->GetData());

    buffer_pool_manager_->UnpinPage(curr_page->GetPageId(), false);

    curr_page = next_page;
    curr_inter_node = next_inter_node;
  }
  auto curr_node = reinterpret_cast<LeafPage *>(curr_page->GetData());

  page_id_t page_id = curr_page->GetPageId();
  buffer_pool_manager_->UnpinPage(page_id, false);

  return INDEXITERATOR_TYPE(curr_page, page_id, curr_node->GetSize(), buffer_pool_manager_);
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!child_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << child_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(child_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
