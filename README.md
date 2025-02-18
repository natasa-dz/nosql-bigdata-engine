# 🌟 Project: Database System with Optimized Compacting and Rate Limiting 🌟

## 🚀 Overview
This project involves implementing a highly optimized and scalable key-value store system. It covers various stages of data management, including memtable structures, rate-limiting, compaction strategies, and more. Different features such as Write Path, Read Path, Memtable Structures, Bloom Filters, and advanced configurations are covered in the system. The goal is to create a robust and efficient key-value store with features such as manual compaction, token bucket rate-limiting, and probabilistic data structures.

## 💡 Features
### 🔑 Write Path & Read Path
- The system implements optimized **Write Path** and **Read Path** to ensure efficient data insertion and retrieval.
- **Memtable structures** such as **HashMap**, **SkipList**, or **B-Tree** are supported to store data in memory, with the ability to switch between them based on configuration.

### ⚙️ Manual Compaction
- **Manual compaction** is available to reorganize and consolidate data at multiple levels.
- **Tombstone elements** are properly handled during the compaction process to ensure data integrity.
  
### 🛠️ Token Bucket Rate Limiting
- The **Token Bucket algorithm** is implemented to limit the rate of access to the system, helping to prevent overload and ensure fair usage.
- The configuration for rate limiting is stored and can be customized through an external configuration file.

### 🔐 Security Features
- Proper handling of **Tombstone elements** ensures that deleted or obsolete data does not persist in the system.
- **Merge sort** is used for efficient merging of data during compaction operations.

### 📊 Probabilistic Data Structures
- Support for **HyperLogLog (HLL)** and **Count-min Sketch (CMS)** to efficiently estimate cardinality and count frequencies of large datasets.
  
### 🔄 Range Scans & List Operations
- Implementing **RANGE SCAN** and **LIST operations** allows for flexible and efficient data queries, enabling operations like range-based searches and pagination.
- **Pagination** is supported for both **RANGE SCAN** and **LIST** operations, making it easy to retrieve large datasets in chunks.

### 🛠️ Configuration & Flexibility
- **External configuration management** allows users to define system parameters such as memory limits, compaction strategies, and more, in a simple and accessible format.
- **Merkle Trees** are used to verify the integrity of the data and ensure consistency across operations.

### 📈 Compaction Algorithms
- The system supports **Leveled compaction** and **Size-tiered compaction** strategies, with a minimal height for the LSM tree structure and the ability to adjust compaction levels.
- **Merge operations** occur automatically as data is consolidated, ensuring that the system remains efficient as it scales.

### 🔄 Multi-Level Compaction
- Compaction occurs across multiple levels of data hierarchy, starting from **Memtable** and moving to **SSTable** levels. As the system grows, data is merged and compacted across these levels.

### 🔐 Enhanced Data Structures
- The system includes support for **Bloom Filters** and **SimHash** for efficient approximate set membership checking and near-duplicate detection.
- These **probabilistic data structures** are optimized for space efficiency and fast lookups.


