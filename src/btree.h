/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University
 * of Wisconsin-Madison.
 */

#pragma once

#include <iostream>
#include <sstream>
#include <string>
#include "string.h"

#include "buffer.h"
#include "file.h"
#include "page.h"
#include "types.h"

namespace badgerdb {

/**
 * @brief Datatype enumeration type.
 */
enum Datatype { INTEGER = 0, DOUBLE = 1, STRING = 2 };

/**
 * @brief Scan operations enumeration. Passed to BTreeIndex::startScan() method.
 */
enum Operator {
  LT,  /* Less Than */
  LTE, /* Less Than or Equal to */
  GTE, /* Greater Than or Equal to */
  GT   /* Greater Than */
};

/**
 * @brief Number of key slots in B+Tree leaf for INTEGER key.
 */
//                                                  sibling ptr             key
//                                                  rid
const int INTARRAYLEAFSIZE =
    (Page::SIZE - sizeof(PageId)) / (sizeof(int) + sizeof(RecordId));

/**
 * @brief Number of key slots in B+Tree non-leaf for INTEGER key.
 */
//                                                     level     extra pageNo
//                                                     key       pageNo
const int INTARRAYNONLEAFSIZE = (Page::SIZE - sizeof(int) - sizeof(PageId)) /
                                (sizeof(int) + sizeof(PageId));

/**
 * @brief The meta page, which holds metadata for Index file, is always first
 * page of the btree index file and is cast to the following structure to store
 * or retrieve information from it. Contains the relation name for which the
 * index is created, the byte offset of the key value on which the index is
 * made, the type of the key and the page no of the root page. Root page starts
 * as page 2 but since a split can occur at the root the root page may get moved
 * up and get a new page no.
 */
struct IndexMetaInfo {
  /**
   * Name of base relation.
   */
  char relationName[20];

  /**
   * Offset of attribute, over which index is built, inside the record stored in
   * pages.
   */
  int attrByteOffset;

  /**
   * Type of the attribute over which index is built.
   */
  Datatype attrType;

  /**
   * Page number of root page of the B+ Tree inside the file index file.
   */
  PageId rootPageNo;
};

/*
Each node is a page, so once we read the page in we just cast the pointer to the
page to this struct and use it to access the parts These structures basically
are the format in which the information is stored in the pages for the index
file depending on what kind of node they are. The level memeber of each non leaf
structure seen below is set to 1 if the nodes at this level are just above the
leaf nodes. Otherwise set to 0.
*/

/**
 * @brief Structure for all non-leaf nodes when the key is of INTEGER type.
 */
struct NonLeafNodeInt {
  /**
   * Level of the node in the tree.
   */
  int level = 0;

  /**
   * Stores keys.
   */
  int keyArray[INTARRAYNONLEAFSIZE]{};

  /**
   * Stores page numbers of child pages which themselves are other non-leaf/leaf
   * nodes in the tree.
   */
  PageId pageNoArray[INTARRAYNONLEAFSIZE + 1]{};
};

/**
 * @brief Structure for all leaf nodes when the key is of INTEGER type.
 */

struct LeafNodeInt {
  int level = -1;

  /**
   * Stores keys.
   */
  int keyArray[INTARRAYLEAFSIZE]{};

  /**
   * Stores RecordIds.
   */
  RecordId ridArray[INTARRAYLEAFSIZE]{};

  /**
   * Page number of the leaf on the right side.
   * This linking of leaves allows to easily move from one leaf to the next leaf
   * during index scan.
   */
  PageId rightSibPageNo = 0;
};

/**
 * @brief BTreeIndex class. It implements a B+ Tree index on a single attribute
 * of a relation. This index supports only one scan at a time.
 */
class BTreeIndex {
 private:
  /**
   * File object for the index file.
   */
  File *file{};

  /**
   * Buffer Manager Instance.
   */
  BufMgr *bufMgr{};

  /**
   * Datatype of attribute over which index is built.
   */
  Datatype attributeType;

  /**
   * Offset of attribute, over which index is built, inside records.
   */
  int attrByteOffset{};

  // MEMBERS SPECIFIC TO SCANNING

  /**
   * True if an index scan has been started.
   */
  bool scanExecuting{};

  /**
   * Index of next entry to be scanned in current leaf being scanned.
   */
  int nextEntry{};

  /**
   * Page number of current page being scanned.
   */
  PageId currentPageNum{};

  /**
   * Current Page being scanned.
   */
  Page *currentPageData{};

  /**
   * Low INTEGER value for scan.
   */
  int lowValInt{};

  /**
   * High INTEGER value for scan.
   */
  int highValInt{};

  /**
   * Low Operator. Can only be GT(>) or GTE(>=).
   */
  Operator lowOp{GT};

  /**
   * High Operator. Can only be LT(<) or LTE(<=).
   */
  Operator highOp{LT};

  struct IndexMetaInfo indexMetaInfo {};

  /**
   * Recursively insert the given key-record pair into the subtree with the
   * given root node. If the root node requires a split, the page number of the
   * newly created node will be return
   *
   * @param origPageId page id of the page that stores the root node of the
   *        subtree.
   * @param key the key of the key-record pair to be inserted
   * @param rid the record ID of the key-record pair to be inserted
   * @param midVal a pointer to an integer value to be stored in the parent
   * node. If the insertion requires a split in the current level, midVal is set
   *        to the smallest key stored in the subtree pointed by the newly
   * created node.
   *
   * @return the page number of the newly created node if a split occurs, or 0
   *         otherwise.
   */
  PageId recursiveInsert(PageId origPageId, int key, RecordId rid, int &midVal);

 public:
  /**
   * BTreeIndex Constructor.
   * Check to see if the corresponding index file exists. If so, open the
   * file. If not, create it and insert entries for every tuple in the base
   * relation using FileScan class.
   *
   * @param relationName        Name of file.
   * @param outIndexName        Return the name of index file.
   * @param bufMgrIn						Buffer
   * Manager Instance
   * @param attrByteOffset			Offset of attribute, over which
   * index is to be built, in the record
   * @param attrType						Datatype
   * of attribute over which index is built
   * @throws  BadIndexInfoException     If the index file already exists for
   * the corresponding attribute, but values in metapage(relationName,
   * attribute byte offset, attribute type etc.) do not match with values
   * received through constructor parameters.
   */
  BTreeIndex(const std::string &relationName, std::string &outIndexName,
             BufMgr *bufMgrIn, const int attrByteOffset,
             const Datatype attrType);

  /**
   * BTreeIndex Destructor.
   * End any initialized scan, flush index file, after unpinning any pinned
   * pages, from the buffer manager and delete file instance thereby closing the
   * index file. Destructor should not throw any exceptions. All exceptions
   * should be caught in here itself.
   * */
  ~BTreeIndex();

  /**
   * Insert a new entry using the pair <value,rid>.
   * Start from root to recursively find out the leaf to insert the entry in.
   *The insertion may cause splitting of leaf node. This splitting will require
   *addition of new leaf page number entry into the parent non-leaf, which may
   *in-turn get split. This may continue all the way upto the root causing the
   *root to get split. If root gets split, metapage needs to be changed
   *accordingly. Make sure to unpin pages as soon as you can.
   * @param key			Key to insert, pointer to integer/double/char
   *string
   * @param rid			Record ID of a record whose entry is getting
   *inserted into the index.
   **/
  void insertEntry(const void *key, const RecordId rid);

  /**
   * Begin a filtered scan of the index.  For instance, if the method is called
   * using ("a",GT,"d",LTE) then we should seek all entries with a value
   * greater than "a" and less than or equal to "d".
   * If another scan is already executing, that needs to be ended here.
   * Set up all the variables for scan. Start from root to find out the leaf
   *page that contains the first RecordID that satisfies the scan parameters.
   *Keep that page pinned in the buffer pool.
   * @param lowVal	Low value of range, pointer to integer / double / char
   *string
   * @param lowOp		Low operator (GT/GTE)
   * @param highVal	High value of range, pointer to integer / double / char
   *string
   * @param highOp	High operator (LT/LTE)
   * @throws  BadOpcodesException If lowOp and highOp do not contain one of
   *their their expected values
   * @throws  BadScanrangeException If lowVal > highval
   * @throws  NoSuchKeyFoundException If there is no key in the B+ tree that
   *satisfies the scan criteria.
   **/
  void startScan(const void *lowVal, const Operator lowOp,
                       const void *highVal, const Operator highOp);

  /**
   * Fetch the record id of the next index entry that matches the scan.
   * Return the next record from current page being scanned. If current page has
   *been scanned to its entirety, move on to the right sibling of current page,
   *if any exists, to start scanning that page. Make sure to unpin any pages
   *that are no longer required.
   * @param outRid	RecordId of next record found that satisfies the scan
   *criteria returned in this
   * @throws ScanNotInitializedException If no scan has been initialized.
   * @throws IndexScanCompletedException If no more records, satisfying the scan
   *criteria, are left to be scanned.
   **/
  void scanNext(RecordId &outRid);  // returned record id

  /**
   * Terminate the current scan. Unpin any pinned pages. Reset scan specific
   *variables.
   * @throws ScanNotInitializedException If no scan has been initialized.
   **/
  void endScan();
};
}  // namespace badgerdb
