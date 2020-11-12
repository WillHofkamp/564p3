/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"


//#define DEBUG

namespace badgerdb
{

/**
 * Alloca a page in the buffer for an internal node
 *
 * @param newPageId the page number for the new node
 * @return a pointer to the new internal node
 */
NonLeafNodeInt *BTreeIndex::allocNonLeafNode(PageId &newPageId) {
  NonLeafNodeInt *newNode;
  bufMgr->allocPage(file, newPageId, (Page *&)newNode);
  memset(newNode, 0, Page::SIZE);
  return newNode;
}

/**
 * Alloc a page in the buffer for a leaf node
 *
 * @param newPageId the page number for the new node
 * @return a pointer to the new leaf node
 */
LeafNodeInt *BTreeIndex::allocLeafNode(PageId &newPageId) {
  LeafNodeInt *newNode = (LeafNodeInt *)allocNonLeafNode(newPageId);
  newNode->level = -1;
  return newNode;
}

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
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

  allocLeafNode(indexMetaInfo.rootPageNo);
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
  } catch (EndOfFileException e) {
  }
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

void BTreeIndex::scanNext(RecordId& outRid) 
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
void BTreeIndex::endScan() 
{

}

}
