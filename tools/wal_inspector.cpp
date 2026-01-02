/**
 * @file wal_inspector.cpp
 * @brief Tool to inspect WAL file contents
 */

#include <mydb/engine/wal.hpp>
#include <iostream>
#include <filesystem>

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: wal_inspector <path_to_wal_file>" << std::endl;
        return 1;
    }

    std::string filename = argv[1];
    if (!std::filesystem::exists(filename)) {
        std::cerr << "File not found: " << filename << std::endl;
        return 1;
    }

    std::cout << "Inspecting WAL file: " << filename << std::endl;
    std::cout << "----------------------------------------" << std::endl;

    mydb::WALReader reader(filename);
    int count = 0;
    int errors = 0;

    auto result = reader.ForEach([&](const mydb::WALRecord& record) {
        count++;
        std::cout << "Seq: " << record.sequence 
                  << " | Type: " << (record.type == mydb::OperationType::kPut ? "PUT" : "DEL")
                  << " | Key: " << record.key 
                  << " | Val: " << (record.type == mydb::OperationType::kPut ? record.value : "<deleted>")
                  << std::endl;
        return mydb::Status::Ok();
    });

    if (!result.ok()) {
        std::cerr << "Error reading WAL: " << result.ToString() << std::endl;
    }

    std::cout << "----------------------------------------" << std::endl;
    std::cout << "Total records: " << count << std::endl;

    return 0;
}
