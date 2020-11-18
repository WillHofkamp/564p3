/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include <algorithm>
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"


using namespace std;

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset_,
		const Datatype attrType)
{
  bufMgr = bufMgrIn;
  attrByteOffset = attrByteOffset_;
  attributeType = attrType;

  ostringstream idx_str{};
  idx_str << relationName << ',' << attrByteOffset;
  outIndexName = idx_str.str();

  relationName.copy(indexMetaInfo.relationName, 20, 0);
  indexMetaInfo.attrByteOffset = attrByteOffset;
  indexMetaInfo.attrType = attrType;

  file = new BlobFile(outIndexName, true);

  NonLeafNodeInt *newLeafNode;
  bufMgr->allocPage(file, indexMetaInfo.rootPageNo, (Page *&)newLeafNode);
  memset(newLeafNode, 0, Page::SIZE);
  newLeafNode->level = -1;
  
  bufMgr->unPinPage(file, indexMetaInfo.rootPageNo, true);

  FileScan fscan(relationName, bufMgr);
  try {
    RecordId scanRid;
    while (1) {
      fscan.scanNext(scanRid);
      std::string recordStr = fscan.getRecord();
      const char *record = recordStr.c_str();
      int key = *((int *)(record + attrByteOffset));
      insertEntry(&key, scanRid);
    }
  } catch (const EndOfFileException e) {
  }
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
  if (scanExecuting) {
    endScan();
  }
  bufMgr->flushFile(file);
  delete file;
}

/**
 * Recursively insert the given key-record pair into the subtree with the given
 * root node. If the root node requires a split, the page number of the newly
 * created node will be return
 *
 * @param origPageId page id of the page that stores the root node of the
 *        subtree.
 * @param key the key of the key-record pair to be inserted
 * @param rid the record ID of the key-record pair to be inserted
 * @param midVal a pointer to an integer value to be stored in the parent node.
 *        If the insertion requires a split in the current level, midVal is set
 *        to the smallest key stored in the subtree pointed by the newly created
 *        node.
 *
 * @return the page number of the newly created node if a split occurs, or 0
 *         otherwise.
 */
PageId BTreeIndex::recursiveInsert(PageId origPageId, int key, RecordId rid,
                          int &midVal) {
  Page *origPage;
  bufMgr->readPage(file, origPageId, origPage);

  if (*((int *)origPage) == -1)  {
    LeafNodeInt *origNode = (LeafNodeInt *)origPage;

    // finde the insertion index
    static auto leafComp = [](const RecordId &r1, const RecordId &r2) {
    return r1.page_number > r2.page_number;
    };
    static RecordId emptyRecord{};

    RecordId *start = origNode->ridArray;
    RecordId *end = &origNode->ridArray[INTARRAYLEAFSIZE];

    int len = lower_bound(start, end, emptyRecord, leafComp) - start;
    int lbResult = lower_bound(origNode->keyArray, &origNode->keyArray[len], key) - origNode->keyArray;
    int result = lbResult >= len ? -1 : lbResult;
    int index =  result == -1 ? len : result;

    // if not full, directly insert the key and record id to the node
    if (!(!(origNode->ridArray[INTARRAYLEAFSIZE - 1].page_number == 0 &&
            origNode->ridArray[INTARRAYLEAFSIZE - 1].slot_number == 0))) {
    const size_t insertLeafLen = INTARRAYLEAFSIZE - index - 1;

    // shift items to add space for the new element
    memmove(&origNode->keyArray[index + 1], &origNode->keyArray[index], insertLeafLen * sizeof(int));
    memmove(&origNode->ridArray[index + 1], &origNode->ridArray[index], insertLeafLen * sizeof(RecordId));

    // save the key and record id to the leaf node
    origNode->keyArray[index] = key;
    origNode->ridArray[index] = rid;

    bufMgr->unPinPage(file, origPageId, true);
    return 0;
    }

    // the node is full at this point

    // the middle index for spliting the page
    const int middleIndex = INTARRAYLEAFSIZE / 2;

    // whether the new element is insert to the left half of the original node
    bool insertToLeft = index < middleIndex;

    // alloc a page for the new node
    PageId newPageId;
    NonLeafNodeInt *newLeafNode;
    bufMgr->allocPage(file, newPageId, (Page *&)newLeafNode);
    memset(newLeafNode, 0, Page::SIZE);
    newLeafNode->level = -1;
    LeafNodeInt *newNode = (LeafNodeInt *)newLeafNode;

    // split the node to origNode and newNode
    int newLeafIndex = middleIndex + insertToLeft;
    const size_t splitLen = INTARRAYLEAFSIZE - newLeafIndex;

    // copy elements from old node to new node
    memcpy(&newNode->keyArray, &origNode->keyArray[newLeafIndex], splitLen * sizeof(int));
    memcpy(&newNode->ridArray, &origNode->ridArray[newLeafIndex], splitLen * sizeof(RecordId));

    // remove elements from old node
    memset(&origNode->keyArray[newLeafIndex], 0, splitLen * sizeof(int));
    memset(&origNode->ridArray[newLeafIndex], 0, splitLen * sizeof(RecordId));

    // insert the key and record id to the node
    if (insertToLeft) {
    const size_t newLeftNodeLen = INTARRAYLEAFSIZE - index - 1;

    // shift items to add space for the new element
    memmove(&origNode->keyArray[index + 1], &origNode->keyArray[index], newLeftNodeLen * sizeof(int));
    memmove(&origNode->ridArray[index + 1], &origNode->ridArray[index], newLeftNodeLen * sizeof(RecordId));

    // save the key and record id to the leaf node
    origNode->keyArray[index] = key;
    origNode->ridArray[index] = rid;
    } else {
    int newRightIndex = index - middleIndex;
    const size_t newRightNodeLen = INTARRAYLEAFSIZE - newRightIndex - 1;

    // shift items to add space for the new element
    memmove(&newNode->keyArray[newRightIndex + 1], &newNode->keyArray[newRightIndex], newRightNodeLen * sizeof(int));
    memmove(&newNode->ridArray[newRightIndex + 1], &newNode->ridArray[newRightIndex], newRightNodeLen * sizeof(RecordId));

    // save the key and record id to the leaf node
    newNode->keyArray[newRightIndex] = key;
    newNode->ridArray[newRightIndex] = rid;
    }

    // set the next page id
    newNode->rightSibPageNo = origNode->rightSibPageNo;
    origNode->rightSibPageNo = newPageId;

    // unpin the new node and the original node
    bufMgr->unPinPage(file, origPageId, true);
    bufMgr->unPinPage(file, newPageId, true);

    // set the middle value
    midVal = newNode->keyArray[0];
    return newPageId; 
  }

  NonLeafNodeInt *origNode = (NonLeafNodeInt *)origPage;

  // find the child page id
  static auto nonLeafComparison = [](const PageId &p1, const PageId &p2) { return p1 > p2; };
  PageId *start = origNode->pageNoArray;
  PageId *end = &origNode->pageNoArray[INTARRAYNONLEAFSIZE + 1];
  int childLen = lower_bound(start, end, 0, nonLeafComparison) - start;
  int lbChildLen = childLen - 1;
  int lbResult = lower_bound(origNode->keyArray, &origNode->keyArray[lbChildLen], key) - origNode->keyArray;
  int childResult = lbResult >= lbChildLen ? -1 : lbResult;
  int origChildPageIndex = childResult == -1 ? childLen - 1 : childResult;
  PageId origChildPageId = origNode->pageNoArray[origChildPageIndex];

  // insert key, rid to child and check whether child is splitted
  int newChildMidVal;
  PageId newChildPageId = recursiveInsert(origChildPageId, key, rid, newChildMidVal);

  // not split in child
  if (newChildPageId == 0) {
    bufMgr->unPinPage(file, origPageId, false);
    return 0;
  }

  // split in child, need to add splitted child to currNode
  PageId *secondStart = origNode->pageNoArray;
  PageId *secondEnd = &origNode->pageNoArray[INTARRAYNONLEAFSIZE + 1];
  int splitChildLen = lower_bound(secondStart, secondEnd, 0, nonLeafComparison) - start;
  int lbSplitChildLen = splitChildLen - 1;
  lbResult = lower_bound(origNode->keyArray, &origNode->keyArray[lbSplitChildLen], newChildMidVal) - origNode->keyArray;
  int splitChildResult = lbResult >= lbSplitChildLen ? -1 : lbResult;
  int index = splitChildResult == -1 ? splitChildLen - 1 : splitChildResult;

  if (origNode->pageNoArray[INTARRAYNONLEAFSIZE] == 0) {  // current node is not full
    const size_t insertNonLeafLen = INTARRAYNONLEAFSIZE - index - 1;

    // shift items to add space for the new element
    memmove(&origNode->keyArray[index + 1], &origNode->keyArray[index], insertNonLeafLen * sizeof(int));
    memmove(&origNode->pageNoArray[index + 2], &origNode->pageNoArray[index + 1], insertNonLeafLen * sizeof(PageId));

    // store the key and page number to the node
    origNode->keyArray[index] = newChildMidVal;
    origNode->pageNoArray[index + 1] = newChildPageId;

    bufMgr->unPinPage(file, origPageId, true);
    return 0;
  }

  // the middle index for spliting the page
  int middleIndex = (INTARRAYNONLEAFSIZE - 1) / 2;

  // whether the new element is insert to the left half of the original node
  bool insertToLeft = index < middleIndex;

  // split
  int splitIndex = middleIndex + insertToLeft;
  int insertIndex = insertToLeft ? index : index - middleIndex;

  // insert to right[0]
  bool moveKeyUp = !insertToLeft && insertIndex == 0;

  // if we need to move key up, set midVal = key, else key at splited index
  midVal = moveKeyUp ? newChildMidVal : origNode->keyArray[splitIndex];

  // alloc a page for the new node
  PageId newPageId;
  NonLeafNodeInt *newNode;
  bufMgr->allocPage(file, newPageId, (Page *&)newNode);
  memset(newNode, 0, Page::SIZE);

  // split the node to origNode and newNode
  size_t newLeafLen = INTARRAYNONLEAFSIZE - splitIndex;

  // copy keys from old node to new node
  if (moveKeyUp)
    memcpy(&newNode->keyArray, &origNode->keyArray[splitIndex], newLeafLen * sizeof(int));
  else
    memcpy(&newNode->keyArray, &origNode->keyArray[splitIndex + 1], (newLeafLen - 1) * sizeof(int));

  // copy values from old node to new node
  memcpy(&newNode->pageNoArray, &origNode->pageNoArray[splitIndex + 1], newLeafLen * sizeof(PageId));

  // remove elements from old node
  memset(&origNode->keyArray[splitIndex], 0, newLeafLen * sizeof(int));
  memset(&origNode->pageNoArray[splitIndex + 1], 0, newLeafLen * sizeof(PageId));

  // need to insert
  if (!moveKeyUp) {
    NonLeafNodeInt *node = insertToLeft ? origNode : newNode;

    const size_t insertNonLeafLen = INTARRAYNONLEAFSIZE - insertIndex - 1;

    // shift items to add space for the new element
    memmove(&node->keyArray[insertIndex + 1], &node->keyArray[insertIndex], insertNonLeafLen * sizeof(int));
    memmove(&node->pageNoArray[insertIndex + 2], &node->pageNoArray[insertIndex + 1], insertNonLeafLen * sizeof(PageId));

    // store the key and page number to the node
    node->keyArray[insertIndex] = newChildMidVal;
    node->pageNoArray[insertIndex + 1] = newChildPageId;
  }

  // write the page back
  bufMgr->unPinPage(file, origPageId, true);
  bufMgr->unPinPage(file, newPageId, true);

  // return new page
  return newPageId;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
  int midval;
  PageId pid = recursiveInsert(indexMetaInfo.rootPageNo, *(int *)key, rid, midval);

  if (pid != 0) {
    // alloc a new page for root
    PageId newRootPageId;
    NonLeafNodeInt *newRoot;
    bufMgr->allocPage(file, newRootPageId, (Page *&)newRoot);
    memset(newRoot, 0, Page::SIZE);

    // set key and page numbers
    newRoot->keyArray[0] = midval;
    newRoot->pageNoArray[0] = indexMetaInfo.rootPageNo;
    newRoot->pageNoArray[1] = pid;

    // unpin the root page
    bufMgr->unPinPage(file, newRootPageId, true);

    indexMetaInfo.rootPageNo = newRootPageId;
  }
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{
  if (lowOpParm != GT && lowOpParm != GTE) {
    throw BadOpcodesException();
  } 
  if (highOpParm != LT && highOpParm != LTE) {
    throw BadOpcodesException();
  }

  lowValInt = *((int *)lowValParm);
  highValInt = *((int *)highValParm);
  if (lowValInt > highValInt) throw BadScanrangeException();

  lowOp = lowOpParm;
  highOp = highOpParm;

  scanExecuting = true;

  currentPageNum = indexMetaInfo.rootPageNo;

  while (true) {
    bufMgr->readPage(file, currentPageNum, currentPageData);
    if (*((int *)currentPageData) == -1) break;

    NonLeafNodeInt *node = (NonLeafNodeInt *)currentPageData;

    bufMgr->unPinPage(file, currentPageNum, false);

    static auto pageComp = [](const PageId &p1, const PageId &p2) { return p1 > p2; };
    PageId *start = node->pageNoArray;
    PageId *end = &node->pageNoArray[INTARRAYNONLEAFSIZE + 1];
    int len = lower_bound(start, end, 0, pageComp) - start;
    int checkLen = len - 1;
    int lbCheck = lower_bound(node->keyArray, &node->keyArray[checkLen], lowValInt) - node->keyArray;
    int result = lbCheck >= checkLen ? -1 : lbCheck;
    int indexResult = result == -1 ? len - 1 : result;
    currentPageNum = node->pageNoArray[indexResult];
  }

  LeafNodeInt *node = (LeafNodeInt *)currentPageData;
  static auto comp = [](const RecordId &r1, const RecordId &r2) {
    return r1.page_number > r2.page_number;
  };
  static RecordId emptyRecord{};

  RecordId *start = node->ridArray;
  RecordId *end = &node->ridArray[INTARRAYLEAFSIZE];

  int len = lower_bound(start, end, emptyRecord, comp) - start;
  if (!(lowOp == GTE)) {
    lowValInt++;
  }
  int lbResult = lower_bound(node->keyArray, &node->keyArray[len], lowValInt) - node->keyArray;
  int entryIndex = lbResult >= len ? -1 : lbResult;
  if (entryIndex == -1) {
    bufMgr->unPinPage(file, currentPageNum, false);
    currentPageNum = node->rightSibPageNo;
    bufMgr->readPage(file, currentPageNum, currentPageData);
    nextEntry = 0;
  } else {
    nextEntry = entryIndex;
  }
  RecordId outRid = node->ridArray[nextEntry];
  if ((outRid.page_number == 0 && outRid.slot_number == 0) ||
      node->keyArray[nextEntry] > highValInt ||
      (node->keyArray[nextEntry] == highValInt && highOp == LT)) {
    endScan();
    throw NoSuchKeyFoundException();
  }
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

void BTreeIndex::scanNext(RecordId& outRid) 
{
  if (!scanExecuting) {
    throw ScanNotInitializedException();
  }
  LeafNodeInt *node = (LeafNodeInt *)currentPageData;
  outRid = node->ridArray[nextEntry];
  int val = node->keyArray[nextEntry];

  if ((outRid.page_number == 0 &&
       outRid.slot_number == 0) ||            // current record ID is empty
      val > highValInt ||                     // value is out of range
      (val == highValInt && highOp == LT)) {  // value reaches the higher end
    throw IndexScanCompletedException();
  }
  nextEntry++;
  if (nextEntry >= INTARRAYLEAFSIZE ||
    node->ridArray[nextEntry].page_number == 0) {
    bufMgr->unPinPage(file, currentPageNum, false);
    currentPageNum = node->rightSibPageNo;
    bufMgr->readPage(file, currentPageNum, currentPageData);
    nextEntry = 0;
  }
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
void BTreeIndex::endScan() 
{
  if (!scanExecuting) {
    throw ScanNotInitializedException();
  }
  scanExecuting = false;
  bufMgr->unPinPage(file, currentPageNum, false);
}

}