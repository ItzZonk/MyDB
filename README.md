# MyDB - High-Performance Key-Value Store

**MyDB** is a high-performance, persistent Key-Value Store built with **modern C++** (C++20). It implements a Log-Structured Merge-Tree (LSM-Tree) architecture, featuring a lock-free SkipList MemTable, Write-Ahead Logging (WAL) for durability, and LevelDB-style SSTables.

![Dashboard Demo](https://via.placeholder.com/800x400.png?text=MyDB+Dashboard+Snapshot)

## 🚀 Key Features

*   **LSM-Tree Engine:** Fast writes and efficient storage.
*   **Time Travel Queries (MVCC):** Query data at any point in the past (`GET key AS OF timestamp`).
*   **TUI Dashboard:** Futuristic "Cockpit" interface for monitoring and management (built with FTXUI).
*   **Embedded Python:** Execute stored procedures directly within the database engine.
*   **High Performance:** Uses `io_uring` on Linux for async I/O (native Windows IOCP fallback included).

## 🛠️ Build & Run

### Requirements
*   CMake 3.15+
*   C++20 Compiler (MSVC, GCC, or Clang)

### Build
```bash
mkdir build
cd build
cmake ..
cmake --build . --config Release
```

### Run Server
```bash
./Release/mydb_server.exe
```

### Run Dashboard
```bash
./Release/mydb_dashboard.exe
```

## 📚 Usage

### CLI Client
```bash
# Connect
./Release/mydb_cli.exe

# Commands
mydb> PUT user:101 "Alice"
mydb> GET user:101
mydb> GET user:101 AS OF 1704067200
```

## 🏗️ Architecture

*   **MemTable:** Lock-free SkipList (RAM).
*   **WAL:** Append-only log for crash recovery.
*   **SSTable:** Sorted String Tables with Bloom Filters.
*   **Compaction:** Background thread for leveled compaction.

## 📄 License
MIT License.
