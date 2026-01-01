/**
 * MyDB Shell - Standard Interactive Command Line Interface
 * 
 * A clean, lightweight shell for MyDB with replxx for line editing and history.
 * No AI dependencies.
 */

#include <mydb/db.hpp>

#include <replxx.hxx>
#include <spdlog/spdlog.h>

#include <iostream>
#include <string>
#include <sstream>
#include <vector>
#include <filesystem>
#include <cstdlib>
#include <algorithm>

namespace fs = std::filesystem;

namespace mydb {

// Built-in commands
static const std::vector<std::string> kBuiltinCommands = {
    "help", "exit", "quit", "cd", "pwd", "history", "clear", "stats",
    "get", "set", "put", "del", "delete"
};

class Shell {
public:
    Shell() : cwd_(fs::current_path()) {}
    
    int Run() {
        // Open Database
        mydb::Options options;
        options.create_if_missing = true;
        // Construct path relative to CWD or use default
        // For now, assume local database in ./mydb_data
        
        auto db_res = mydb::Database::Open(options);
        if (!db_res.ok()) {
            std::cerr << "[-] Failed to open database: " << db_res.status().ToString() << "\n";
            std::cerr << "    Ensure you are in the project root or provide correct path.\n";
            // We continue without DB to allow basic shell usage, or maybe exit? 
            // Better to let user fix it.
            std::cerr << "    Shell running in limited mode (system commands only).\n";
        } else {
            db_ = std::move(db_res.value());
            std::cout << "[+] Database opened successfully.\n";
        }

        replxx::Replxx rx;
        
        // Configure replxx
        rx.set_max_history_size(1000);
        rx.set_word_break_characters(" \t\n\r\v\f");
        
        // Load history
        fs::path history_path = fs::path(".mydb_history");
        rx.history_load(history_path.string());
        
        // Set up completion
        rx.set_completion_callback([](const std::string& input, int& context_len) {
            std::vector<replxx::Replxx::Completion> completions;
            
            for (const auto& cmd : kBuiltinCommands) {
                if (cmd.rfind(input, 0) == 0) {
                    completions.push_back({cmd, replxx::Replxx::Color::DEFAULT});
                }
            }
            
            context_len = static_cast<int>(input.size());
            return completions;
        });
        
        PrintWelcome();
        
        std::string prompt = "mydb> ";
        const char* line = nullptr;
        
        while ((line = rx.input(prompt)) != nullptr) {
            std::string input(line);
            
            // Skip empty lines
            if (input.empty()) continue;
            
            // Add to history
            rx.history_add(input);
            
            // Execute command
            if (!Execute(input)) {
                break;  // exit requested
            }
            
            commands_executed_++;
        }
        
        // Save history
        rx.history_save(history_path.string());
        
        std::cout << "\nGoodbye!\n";
        return 0;
    }

private:
    void PrintWelcome() {
        std::cout << "\n";
        std::cout << "+-----------------------------------------------------------+\n";
        std::cout << "|                 MyDB Shell - Command Line                 |\n";
        std::cout << "+-----------------------------------------------------------+\n";
        std::cout << "|  Type commands or 'help' for information.                 |\n";
        std::cout << "+-----------------------------------------------------------+\n\n";
        std::cout << "[*] Working directory: " << cwd_.string() << "\n\n";
    }
    
    void PrintHelp() {
        std::cout << "\n";
        std::cout << "---------------------------------------------------------------\n";
        std::cout << "                        MyDB Shell Help\n";
        std::cout << "---------------------------------------------------------------\n\n";
        
        std::cout << "DATABASE COMMANDS:\n";
        std::cout << "  get <key>              Get a value\n";
        std::cout << "  set <key> <value>      Set a key-value pair\n";
        std::cout << "  del <key>              Delete a key\n\n";

        std::cout << "SYSTEM COMMANDS:\n";
        std::cout << "  <command>              Execute shell command directly\n";
        std::cout << "  cd <dir>               Change directory\n";
        std::cout << "  pwd                    Print working directory\n\n";
        
        std::cout << "BUILT-IN COMMANDS:\n";
        std::cout << "  help                   Show this help message\n";
        std::cout << "  exit, quit             Exit the shell\n";
        std::cout << "  history [n]            Show last n commands\n";
        std::cout << "  clear                  Clear the screen\n";
        std::cout << "  stats                  Show shell statistics\n\n";
        
        std::cout << "KEYBOARD SHORTCUTS:\n";
        std::cout << "  Tab                    Autocomplete\n";
        std::cout << "  Ctrl+C                 Cancel current input\n";
        std::cout << "  Ctrl+D                 Exit shell\n";
        std::cout << "  Up/Down                Navigate command history\n\n";
    }
    
    bool Execute(const std::string& input) {
        std::istringstream iss(input);
        std::string cmd;
        iss >> cmd;
        
        // Normalize command to lowercase for check? Keep case-sensitive or standard?
        // Standard interactive shells are usually case-sensitive but SQL is not. 
        // Let's allow mixed case for standard commands by comparing lowercase version if needed.
        // For now, strict 'get' 'set' match as per builtin list, or maybe allow uppercase GET.
        std::string cmd_lower = cmd;
        std::transform(cmd_lower.begin(), cmd_lower.end(), cmd_lower.begin(), ::tolower);

        // Check built-in commands
        if (cmd_lower == "exit" || cmd_lower == "quit") {
            return false;
        }
        
        if (cmd_lower == "help") {
            PrintHelp();
            return true;
        }
        
        if (cmd_lower == "pwd") {
            std::cout << cwd_.string() << "\n";
            return true;
        }
        
        if (cmd_lower == "cd") {
            std::string dir;
            iss >> dir;
            
            if (dir.empty()) {
#ifdef _WIN32
                dir = std::getenv("USERPROFILE") ? std::getenv("USERPROFILE") : ".";
#else
                dir = std::getenv("HOME") ? std::getenv("HOME") : ".";
#endif
            }
            
            fs::path new_path = dir;
            if (new_path.is_relative()) {
                new_path = cwd_ / new_path;
            }
            
            if (!fs::exists(new_path)) {
                std::cerr << "Directory not found: " << dir << "\n";
                return true;
            }
            
            cwd_ = fs::canonical(new_path);
            return true;
        }
        
        if (cmd_lower == "clear") {
#ifdef _WIN32
            system("cls");
#else
            std::cout << "\033[2J\033[H";
#endif
            return true;
        }
        
        if (cmd_lower == "stats") {
            std::cout << "\n=== Shell Statistics ===\n";
            std::cout << "Commands executed: " << commands_executed_ << "\n";
            std::cout << "Working directory: " << cwd_.string() << "\n";
            if (db_) {
                auto stats = db_->GetStats();
                std::cout << "DB Entries: " << stats.num_entries << "\n";
                std::cout << "DB Reads: " << stats.reads << "\n";
                std::cout << "DB Writes: " << stats.writes << "\n";
            } else {
                std::cout << "Database: Not connected\n";
            }
            std::cout << "\n";
            return true;
        }
        
        if (cmd_lower == "history") {
            std::cout << "Use Up/Down arrows to navigate history.\n";
            return true;
        }

        // DB Commands
        if (cmd_lower == "get") {
            if (!db_) { std::cerr << "Database not open.\n"; return true; }
            std::string key;
            iss >> key;
            if (key.empty()) { std::cerr << "Usage: get <key>\n"; return true; }
            
            auto res = db_->Get(key);
            if (res.ok()) {
                std::cout << res.value() << "\n";
            } else if (res.status().IsNotFound()) {
                std::cout << "(nil)\n";
            } else {
                std::cerr << "Error: " << res.status().ToString() << "\n";
            }
            return true;
        }

        if (cmd_lower == "set" || cmd_lower == "put") {
            if (!db_) { std::cerr << "Database not open.\n"; return true; }
            std::string key;
            iss >> key;
            if (key.empty()) { std::cerr << "Usage: set <key> <value>\n"; return true; }
            
            // Read rest of line as value
            std::string value;
            std::getline(iss, value);
            // Trim leading whitespace
            size_t first = value.find_first_not_of(" \t");
            if (first == std::string::npos) {
                 // Empty value or just spaces? Treat as empty string or error?
                 // Redis allows empty string.
                 value = "";
            } else {
                value = value.substr(first);
            }

            auto status = db_->Put(key, value);
            if (status.ok()) {
                std::cout << "OK\n";
            } else {
                std::cerr << "Error: " << status.ToString() << "\n";
            }
            return true;
        }

        if (cmd_lower == "del" || cmd_lower == "delete") {
            if (!db_) { std::cerr << "Database not open.\n"; return true; }
            std::string key;
            iss >> key;
            if (key.empty()) { std::cerr << "Usage: del <key>\n"; return true; }
            
            auto status = db_->Delete(key);
            if (status.ok()) {
                std::cout << "(integer) 1\n";
            } else if (status.IsNotFound()) {
                 std::cout << "(integer) 0\n";
            } else {
                std::cerr << "Error: " << status.ToString() << "\n";
            }
            return true;
        }
        
        // Execute as external command
        if (db_) {
             // If we have a DB, maybe we should warn before running system commands?
             // But valid system workflow might mix them.
             // Just run it.
        }
        ExecuteExternal(input);
        return true;
    }
    
    void ExecuteExternal(const std::string& command) {
        // Save current directory, change to cwd_, execute, restore
        auto old_cwd = fs::current_path();
        try {
            fs::current_path(cwd_);
        } catch(const fs::filesystem_error& e) {
             std::cerr << "Failed to set CWD: " << e.what() << "\n";
        }
        
        int result = system(command.c_str());
        
        try {
            fs::current_path(old_cwd);
        } catch(...) {}
        
        if (result != 0) {
            // Command failed or returned non-zero
        }
    }
    
    fs::path cwd_;
    uint64_t commands_executed_ = 0;
    std::unique_ptr<mydb::Database> db_;
};

}  // namespace mydb

// Main entry point
int main(int argc, char* argv[]) {
    // Simple argument handling
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--help" || arg == "-h") {
            std::cout << "Usage: " << argv[0] << " [options]\n\n";
            std::cout << "Options:\n";
            std::cout << "  --help, -h    Show this help message\n";
            return 0;
        }
    }
    
    mydb::Shell shell;
    return shell.Run();
}
