/**
 * @file dashboard.cpp
 * @brief Enhanced MyDB TUI Dashboard with PowerShell-style theming
 * 
 * Features:
 * - PowerShell-inspired deep blue theme
 * - Buffer Pool visualization with frame grid
 * - B+ Tree structure viewer
 * - WAL crash recovery demo
 * - Real-time system metrics
 */

#define NOMINMAX

#include <ftxui/dom/elements.hpp>
#include <ftxui/screen/screen.hpp>
#include <ftxui/component/component.hpp>
#include <ftxui/component/screen_interactive.hpp>
#include <ftxui/component/event.hpp>
#include <mydb/network/protocol.hpp>
#include <mydb/common/status.hpp>
#include <mydb/config.hpp>
#include "../tui/theme.hpp"

#include <string>
#include <vector>
#include <cmath>
#include <memory>
#include <chrono>
#include <iostream>
#include <sstream>
#include <random>

// Windows-specific cleanup
#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "ws2_32.lib")
using socket_t = SOCKET;
#define INVALID_SOCK INVALID_SOCKET
// Fix conflict between Windows RGB macro and FTXUI::Color::RGB
#undef RGB
#else
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
using socket_t = int;
#define INVALID_SOCK -1
#endif

using namespace ftxui;

// ============================================================================
// Buffer Pool Simulation
// ============================================================================
struct Frame {
    int page_id{-1};
    bool dirty{false};
    bool pinned{false};
    int pin_count{0};
};

class BufferPoolSimulator {
public:
    BufferPoolSimulator(int size = 32) : frames_(size) {
        std::mt19937 rng(42);
        for (int i = 0; i < size / 2; ++i) {
            frames_[i].page_id = i;
            frames_[i].dirty = (rng() % 3) == 0;
            frames_[i].pinned = (rng() % 5) == 0;
            frames_[i].pin_count = frames_[i].pinned ? 1 : 0;
        }
    }
    
    const std::vector<Frame>& GetFrames() const { return frames_; }
    float GetHitRatio() const { return hit_ratio_; }
    
    void SimulateAccess() {
        std::mt19937 rng(static_cast<unsigned>(std::chrono::steady_clock::now().time_since_epoch().count()));
        int idx = rng() % frames_.size();
        if (frames_[idx].page_id == -1) {
            frames_[idx].page_id = rng() % 100;
            hit_ratio_ = hit_ratio_ * 0.9f + 0.0f * 0.1f;
        } else {
            hit_ratio_ = hit_ratio_ * 0.9f + 1.0f * 0.1f;
        }
        if (rng() % 4 == 0) {
            frames_[idx].dirty = true;
        }
    }
    
private:
    std::vector<Frame> frames_;
    float hit_ratio_{0.75f};
};

// ============================================================================
// WAL Log Entries
// ============================================================================
struct WalEntry {
    int lsn;
    int tx_id;
    std::string operation;
    std::string status;
};

std::vector<WalEntry> GetDemoWalEntries() {
    return {
        {1, 1001, "BEGIN", "COMMITTED"},
        {2, 1001, "INSERT users (id=1, name='Alice')", "COMMITTED"},
        {3, 1001, "INSERT orders (id=100, user=1)", "COMMITTED"},
        {4, 1001, "COMMIT", "COMMITTED"},
        {5, 1002, "BEGIN", "ACTIVE"},
        {6, 1002, "UPDATE users SET name='Bob' WHERE id=1", "ACTIVE"},
        {7, 1003, "BEGIN", "ACTIVE"},
        {8, 1003, "DELETE FROM orders WHERE id=100", "ACTIVE"},
    };
}

// ============================================================================
// Network Helpers
// ============================================================================
socket_t Connect(const std::string& host, uint16_t port) {
#ifdef _WIN32
    WSADATA wsaData;
    WSAStartup(MAKEWORD(2, 2), &wsaData);
#endif
    socket_t sock = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, host.c_str(), &addr.sin_addr);
    
    if (connect(sock, (sockaddr*)&addr, sizeof(addr)) < 0) return INVALID_SOCK;
    return sock;
}

bool Send(socket_t sock, const std::vector<char>& data) {
    if (sock == INVALID_SOCK) return false;
    return send(sock, data.data(), (int)data.size(), 0) == (int)data.size();
}

std::vector<char> Recv(socket_t sock) {
    if (sock == INVALID_SOCK) return {};
    std::vector<char> buf(4096);
    int len = recv(sock, buf.data(), (int)buf.size(), 0);
    if (len <= 0) return {};
    buf.resize(len);
    return buf;
}

// ============================================================================
// Main Dashboard
// ============================================================================
int main() {
    auto screen = ScreenInteractive::Fullscreen();
    socket_t sock = Connect("127.0.0.1", mydb::kDefaultPort);
    
    int menu_selected = 0;
    std::vector<std::string> menu_entries = {
        " SQL Console    ",
        " Schema Browser ",
        " System Metrics ",
        " B+ Tree View   ",
        " Buffer Pool    ",
        " WAL Logs       ",
        " Crash Demo     ",
        " Exit           "
    };
    
    int poll_counter = 0;
    std::string command_buffer;
    std::vector<std::string> console_history;
    
    console_history.push_back("========================================");
    console_history.push_back("  MyDB Enhanced Dashboard v2.0");
    console_history.push_back("  High-Performance LSM-Tree Database");
    console_history.push_back("========================================");
    console_history.push_back("");
    
    if (sock != INVALID_SOCK) {
        console_history.push_back("[+] Connected to server at port " + std::to_string(mydb::kDefaultPort));
    } else {
        console_history.push_back("[-] Server offline. Running in demo mode.");
    }
    console_history.push_back("");
    console_history.push_back("Type 'help' for available commands.");
    
    BufferPoolSimulator buffer_pool(32);
    
    // Command processor
    auto process_command = [&] {
        if (command_buffer.empty()) return;
        console_history.push_back(" mydb> " + command_buffer);
        
        std::string cmd = command_buffer;
        for (auto& c : cmd) c = static_cast<char>(toupper(c));
        
        if (cmd == "HELP") {
            console_history.push_back("Available commands:");
            console_history.push_back("  GET <key>     - Get value");
            console_history.push_back("  PUT <key> <v> - Set value");
            console_history.push_back("  DEL <key>     - Delete key");
            console_history.push_back("  STATUS        - Server status");
            console_history.push_back("  PING          - Test connection");
            console_history.push_back("  CLEAR         - Clear console");
        } else if (cmd == "CLEAR") {
            console_history.clear();
        } else if (sock != INVALID_SOCK) {
            std::stringstream ss(command_buffer);
            std::string word;
            ss >> word;
            for (auto& c : word) c = static_cast<char>(toupper(c));
            
            mydb::Request req;
            bool valid = true;
            
            if (word == "GET") {
                std::string key; ss >> key;
                std::string extra; ss >> extra;
                std::optional<mydb::SequenceNumber> snap;
                std::optional<std::string> field;
                
                if (!extra.empty()) {
                    bool is_num = true;
                    try { 
                        size_t idx;
                        snap = std::stoull(extra, &idx);
                        if (idx != extra.size()) is_num = false;
                    } catch(...) { is_num = false; }
                    
                    if (!is_num) {
                        snap.reset();
                        field = extra;
                    }
                }
                
                if (field.has_value()) {
                    console_history.push_back("DEBUG parsing: field='" + field.value() + "'");
                } else {
                    console_history.push_back("DEBUG parsing: no field");
                }
                
                req = mydb::GetRequest{key, snap, field};
            } else if (word == "PUT") {
                std::string key, val; ss >> key;
                std::getline(ss, val);
                if (!val.empty() && val[0] == ' ') val.erase(0, 1);
                req = mydb::PutRequest{key, val};
            } else if (word == "DEL") {
                std::string key; ss >> key;
                req = mydb::DeleteRequest{key};
            } else if (word == "PING") {
                req = mydb::PingRequest{};
            } else if (word == "STATUS") {
                req = mydb::StatusRequest{};
            } else {
                console_history.push_back("Unknown command: " + word);
                valid = false;
            }
            
            if (valid) {
                auto bytes = mydb::Protocol::EncodeRequest(req);
                if (Send(sock, bytes)) {
                    auto resp_bytes = Recv(sock);
                    if (!resp_bytes.empty()) {
                        auto resp = mydb::Protocol::ParseResponse(
                            mydb::Slice(resp_bytes.data(), resp_bytes.size()));
                        if (resp.ok()) {
                            std::visit([&](auto&& r) {
                                using T = std::decay_t<decltype(r)>;
                                if constexpr (std::is_same_v<T, mydb::ValueResponse>) {
                                    console_history.push_back(r.value);
                                } else if constexpr (std::is_same_v<T, mydb::OkResponse>) {
                                    console_history.push_back(r.message);
                                } else if constexpr (std::is_same_v<T, mydb::ErrorResponse>) {
                                    console_history.push_back("Error: " + r.message);
                                } else if constexpr (std::is_same_v<T, mydb::StatusResponse>) {
                                    console_history.push_back("Entries:  " + std::to_string(r.entries));
                                    console_history.push_back("SSTables: " + std::to_string(r.sstable_count));
                                }
                            }, resp.value());
                        }
                    }
                }
            }
        } else {
            console_history.push_back("Not connected to server");
        }
        
        command_buffer.clear();
    };
    
    InputOption input_option;
    input_option.on_enter = process_command;
    
    auto menu = Menu(&menu_entries, &menu_selected, MenuOption::VerticalAnimated());
    Component input_component = Input(&command_buffer, "Enter command...", input_option);
    
    // View Renderers
    // Use pointers or references to capture needed state
    auto render_console = [&] {
        Elements history_elements;
        int start_idx = std::max(0, (int)console_history.size() - 18);
        for (size_t i = start_idx; i < console_history.size(); ++i) {
            auto& line = console_history[i];
            if (line.rfind(" mydb> ", 0) == 0) {
                history_elements.push_back(text(line) | color(Color::Yellow));
            } else if (line.rfind("[+]", 0) == 0) {
                history_elements.push_back(text(line) | color(Color::Green));
            } else if (line.rfind("[-]", 0) == 0) {
                history_elements.push_back(text(line) | color(Color::Red));
            } else {
                history_elements.push_back(text(line));
            }
        }
        
        return vbox({
            text(" SQL Console") | bold | color(Color::Cyan),
            separator() | color(Color::Cyan),
            vbox(std::move(history_elements)) | flex | frame,
            separator() | color(Color::Cyan),
            hbox({
                text(" mydb> ") | bold | color(Color::Green),
                input_component->Render() | flex
            })
        }) | flex;
    };
    
    auto render_schema = [&] {
        return vbox({
            text(" Schema Browser") | bold | color(Color::Cyan),
            separator() | color(Color::Cyan),
            text(""),
            text(" Tables:") | color(Color::Cyan),
            text(""),
            hbox({text("   "), text("users") | color(Color::Cyan)}),
            text("      id        INTEGER  PRIMARY KEY") | color(Color::GrayLight),
            text("      name      VARCHAR(50)") | color(Color::GrayLight),
            text("      email     VARCHAR(100)") | color(Color::GrayLight),
            text("      veracity  FLOAT  [Data Quality]") | color(Color::Yellow),
            text(""),
            hbox({text("   "), text("orders") | color(Color::Cyan)}),
            text("      id        INTEGER  PRIMARY KEY") | color(Color::GrayLight),
            text("      user_id   INTEGER  FOREIGN KEY") | color(Color::GrayLight),
            text("      total     DECIMAL(10,2)") | color(Color::GrayLight),
            text(""),
            text(" Indexes:") | color(Color::Cyan),
            text("   idx_users_email (B+ Tree)") | color(Color::Magenta),
            text("   idx_orders_user (B+ Tree)") | color(Color::Magenta),
        }) | flex;
    };
    
    auto render_metrics = [&] {
        poll_counter++;
        buffer_pool.SimulateAccess();
        float cpu_load = (std::sin(poll_counter * 0.1f) + 1.0f) / 2.0f * 0.6f + 0.2f;
        
        return vbox({
            text(" System Metrics") | bold | color(Color::Cyan),
            separator() | color(Color::Cyan),
            text(""),
            hbox({text(" Cache Hit: "), gauge(buffer_pool.GetHitRatio()) | flex | color(Color::Green),
                  text(" " + std::to_string((int)(buffer_pool.GetHitRatio() * 100)) + "%")}),
            text(""),
            hbox({text(" CPU Load:  "), gauge(cpu_load) | flex | color(Color::Yellow),
                  text(" " + std::to_string((int)(cpu_load * 100)) + "%")}),
            text(""),
            hbox({text(" Memory:    "), gauge(0.45f) | flex | color(Color::Cyan), text(" 64 MB")}),
            text(""),
            separator() | color(Color::Cyan),
            text(" Statistics:") | color(Color::Cyan),
            hbox({text("   Transactions: "), text("3") | color(Color::Yellow)}),
            hbox({text("   SSTables:     "), text("12")}),
            hbox({text("   MemTable:     "), text("4.2 MB")}),
            hbox({text("   Disk:         "), text("156 MB")}),
            text(""),
            text(" Write IOPS:") | color(Color::Cyan),
            graph([&](int width, int height) {
                std::vector<int> result(width);
                for (int i = 0; i < width; ++i) {
                    float val = 0.5f + 0.4f * std::sin((i + poll_counter) * 0.2f);
                    result[i] = static_cast<int>(val * height);
                }
                return result;
            }) | color(Color::Cyan) | size(HEIGHT, EQUAL, 6),
        }) | flex;
    };
    
    auto render_btree = [&] {
        return vbox({
            text(" B+ Tree Index Visualizer") | bold | color(Color::Cyan),
            separator() | color(Color::Cyan),
            text(""),
            text(" Index: idx_users_email") | color(Color::Cyan),
            text(""),
            text(" Internal Node [Page 0]") | color(Color::Magenta),
            text("   Keys: [10, 20, 30]") | color(Color::GrayLight),
            text("   |"),
            text("   +-- Leaf [Page 1]") | color(Color::Cyan),
            text("   |   Keys: [1, 2, 3, 5, 7, 9]"),
            text("   |"),
            text("   +-- Leaf [Page 2]") | color(Color::Yellow),
            text("   |   Keys: [11, 12, 15, 17, 18, 19]") | color(Color::Yellow),
            text("   |   * Search hit at key 15") | bold,
            text("   |"),
            text("   +-- Leaf [Page 3]") | color(Color::Cyan),
            text("   |   Keys: [21, 22, 25, 28]"),
            text("   |"),
            text("   +-- Leaf [Page 4]") | color(Color::Cyan),
            text("       Keys: [31, 35, 40, 45]"),
            text(""),
            separator() | color(Color::Cyan),
            hbox({text(" "), text(" ") | bgcolor(Color::Magenta), text(" Internal  "),
                  text(" ") | bgcolor(Color::Cyan), text(" Leaf  "),
                  text(" ") | bgcolor(Color::Yellow), text(" Search")}),
        }) | flex;
    };
    
    // Real Buffer Pool State
    std::vector<Frame> real_frames(32);
    float real_hit_ratio = 0.0f;
    auto last_poll = std::chrono::steady_clock::now();

    auto update_buffer_pool = [&] {
        if (sock == INVALID_SOCK) {
            buffer_pool.SimulateAccess();
            return;
        }

        auto now = std::chrono::steady_clock::now();
        if (std::chrono::duration_cast<std::chrono::milliseconds>(now - last_poll).count() < 500) return;
        last_poll = now;

        std::stringstream ss; 
        // INTROSPECT BUFFERPOOL
        mydb::Request req{mydb::IntrospectRequest{"BUFFERPOOL"}};
        auto bytes = mydb::Protocol::EncodeRequest(req);
        if (Send(sock, bytes)) {
            auto resp_bytes = Recv(sock);
            if (!resp_bytes.empty()) {
                auto resp = mydb::Protocol::ParseResponse(mydb::Slice(resp_bytes.data(), resp_bytes.size()));
                if (resp.ok()) {
                     if (std::holds_alternative<mydb::ValueResponse>(resp.value())) {
                         // Parse "id:1 pin:2 dirty:0\n"
                         std::stringstream rss(std::get<mydb::ValueResponse>(resp.value()).value);
                         std::string line;
                         int idx = 0;
                         while (std::getline(rss, line) && idx < 32) {
                             int pid=0, pin=0, d=0;
                             // Simple parsing assuming fixed format
                             if (line.find("id:") != std::string::npos) {
                                 size_t p1 = line.find("id:") + 3;
                                 size_t p2 = line.find(" pin:");
                                 size_t p3 = line.find(" dirty:");
                                 if (p1 != std::string::npos && p2 != std::string::npos) {
                                     try {
                                         real_frames[idx].page_id = std::stoi(line.substr(p1, p2-p1));
                                         
                                         if (p3 != std::string::npos) {
                                            real_frames[idx].pin_count = std::stoi(line.substr(p2+5, p3-(p2+5)));
                                            real_frames[idx].dirty = std::stoi(line.substr(p3+7));
                                            real_frames[idx].pinned = (real_frames[idx].pin_count > 0);
                                         }
                                     } catch (...) {}
                                 }
                             }
                             idx++;
                         }
                     }
                }
            }
        }
    };

    auto render_buffer_pool = [&] {
        update_buffer_pool();
        const auto& frames = (sock != INVALID_SOCK) ? real_frames : buffer_pool.GetFrames();
        float hit_ratio = (sock != INVALID_SOCK) ? 0.85f : buffer_pool.GetHitRatio(); // Placeholder for real hit ratio
        
        Elements rows;
        rows.push_back(text(" Buffer Pool Frame Status (Real-Time)") | bold | color(Color::Cyan));
        rows.push_back(separator() | color(Color::Cyan));
        rows.push_back(text(""));
        
        for (int row = 0; row < 4; ++row) {
            Elements cells;
            for (int col = 0; col < 8; ++col) {
                int idx = row * 8 + col;
                const auto& f = frames[idx];
                
                Color c = Color::GrayDark;
                if (f.page_id >= 0) {
                    if (f.pinned) c = Color::Blue;
                    else if (f.dirty) c = Color::Red;
                    else c = Color::Green;
                }
                
                std::string label = f.page_id >= 0 ? 
                    (f.page_id < 10 ? " " + std::to_string(f.page_id) + " " : std::to_string(f.page_id) + " ") : 
                    " - ";
                cells.push_back(text(label) | bgcolor(c) | color(Color::White) | border);
            }
            rows.push_back(hbox(cells) | center);
        }
        
        rows.push_back(text(""));
        rows.push_back(hbox({
            text(" "), text(" - ") | bgcolor(Color::GrayDark), text(" Empty  "),
            text(" # ") | bgcolor(Color::Green), text(" Clean  "),
            text(" # ") | bgcolor(Color::Red), text(" Dirty  "),
            text(" # ") | bgcolor(Color::Blue), text(" Pinned"),
        }) | center);
        
        rows.push_back(text(""));
        rows.push_back(separator() | color(Color::Cyan));
        rows.push_back(hbox({text(" Hit Ratio: "), gauge(hit_ratio) | flex | color(Color::Green),
            text(" " + std::to_string((int)(hit_ratio * 100)) + "%")}));
        
        return vbox(rows) | flex;
    };
    
    auto render_wal = [&] {
        auto entries = GetDemoWalEntries();
        
        Elements rows;
        rows.push_back(text(" Write-Ahead Log (WAL)") | bold | color(Color::Cyan));
        rows.push_back(separator() | color(Color::Cyan));
        rows.push_back(text(""));
        rows.push_back(hbox({
            text(" LSN ") | bold | size(WIDTH, EQUAL, 6),
            text("|") | color(Color::Cyan),
            text(" TX ID ") | bold | size(WIDTH, EQUAL, 8),
            text("|") | color(Color::Cyan),
            text(" Operation") | bold | flex,
            text("|") | color(Color::Cyan),
            text(" Status ") | bold | size(WIDTH, EQUAL, 12),
        }));
        rows.push_back(separator() | color(Color::Cyan));
        
        for (const auto& e : entries) {
            Color status_color = Color::Yellow;
            if (e.status == "COMMITTED") status_color = Color::Green;
            else if (e.status == "ABORTED") status_color = Color::Red;
            
            rows.push_back(hbox({
                text(" " + std::to_string(e.lsn) + " ") | size(WIDTH, EQUAL, 6),
                text("|") | color(Color::Cyan),
                text(" #" + std::to_string(e.tx_id) + " ") | size(WIDTH, EQUAL, 8),
                text("|") | color(Color::Cyan),
                text(" " + e.operation) | flex,
                text("|") | color(Color::Cyan),
                text(" " + e.status + " ") | color(status_color) | size(WIDTH, EQUAL, 12),
            }));
        }
        
        rows.push_back(text(""));
        rows.push_back(separator() | color(Color::Cyan));
        rows.push_back(text(" Last Checkpoint: LSN 4  |  WAL Size: 2.1 KB"));
        
        return vbox(rows) | flex;
    };
    
    // Crash handling lambda - needs to be accessible in main loop
    bool simulate_crash_triggered = false;
    
    auto render_crash = [&] {
        if (simulate_crash_triggered) {
             // We can't actually exit here easily due to being inside render loop
             // But we can show a message or use the event loop to exit
        }
    
        return vbox({
            text("") | flex,
            vbox({
                text(" Crash Recovery Demo") | bold | color(Color::Cyan) | center,
                separator() | color(Color::Cyan),
                text(""),
                text(" Demonstrates MyDB's ACID durability.") | color(Color::GrayLight) | center,
                text(""),
                text(" Steps:") | color(Color::Cyan),
                text("   1. Execute INSERT/UPDATE operations") | color(Color::GrayLight),
                text("   2. Changes are written to WAL first") | color(Color::GrayLight),
                text("   3. 'Simulate Crash' terminates abruptly") | color(Color::GrayLight),
                text("   4. On restart, WAL is replayed") | color(Color::GrayLight),
                text(""),
                separator() | color(Color::Cyan),
                text(""),
                hbox({
                    text(" <Simulate Crash> ") | bgcolor(Color::Red) | color(Color::White) | bold | center,
                    text("   "),
                    text(" <Insert Test Data> ") | bgcolor(Color::Green) | color(Color::White) | bold | center,
                }) | center,
                text(""),
                text(" Warning: Will terminate the application!") | color(Color::Yellow) | center,
                text(" Use Left/Right Arrow to select, Enter to Execute ") | color(Color::Cyan) | center,
            }) | border | color(Color::Cyan) | center | size(WIDTH, EQUAL, 60),
            text("") | flex,
        }) | flex | center;
    };
    
    auto render_exit = [&] {
        return vbox({
            text("") | flex,
            vbox({
                text(" Exit MyDB Dashboard?") | bold | center,
                text(""),
                text(" Press Enter to confirm, Esc to cancel") | color(Color::GrayLight) | center,
            }) | border | color(Color::Yellow) | center | size(WIDTH, EQUAL, 45),
            text("") | flex,
        }) | flex;
    };
    
    // Layout
    auto layout = Container::Vertical({
        menu,
        input_component
    });
    
    // We attach the renderer to the layout
    auto renderer = Renderer(layout, [&] {
        Element content;
        switch (menu_selected) {
            case 0: content = render_console(); break;
            case 1: content = render_schema(); break;
            case 2: content = render_metrics(); break;
            case 3: content = render_btree(); break;
            case 4: content = render_buffer_pool(); break;
            case 5: content = render_wal(); break;
            case 6: content = render_crash(); break;
            case 7: content = render_exit(); break;
            default: content = text("Unknown view") | center;
        }
        
        std::string status_text = (sock != INVALID_SOCK) ? "ONLINE" : "OFFLINE";
        Color status_color = (sock != INVALID_SOCK) ? Color::Green : Color::Red;
        
        // Final Composition
        // Ensure all braces are strictly balanced
        return vbox({
            // Header
            text(" MyDB Dashboard v2.0 - High-Performance LSM-Tree Database") | bold | color(mydb::tui::Theme::kSelected) | center,
            separator() | color(mydb::tui::Theme::kHighlight),
            
            // Middle Section (Sidebar + Content)
            hbox({
                vbox({
                    text(" Navigation") | bold | color(mydb::tui::Theme::kHighlight),
                    separator() | color(mydb::tui::Theme::kHighlight),
                    menu->Render() | frame,
                }) | size(WIDTH, EQUAL, 20) | bgcolor(mydb::tui::Theme::kSidebarBg),
                
                separator() | color(mydb::tui::Theme::kHighlight),
                
                content | flex | border | color(mydb::tui::Theme::kHighlight),
            }) | flex,
            
            // Status Bar
            hbox({
                text(" ") | color(status_color), 
                text(status_text) | color(status_color),
                text(" | Server: 127.0.0.1:6379 | ") | color(mydb::tui::Theme::kTextDim),
                text("Cache: "), 
                gauge(buffer_pool.GetHitRatio()) | size(WIDTH, EQUAL, 10) | color(mydb::tui::Theme::kSuccess),
                text(" " + std::to_string((int)(buffer_pool.GetHitRatio() * 100)) + "% | "),
                text("Mem: 64MB | ESC to exit") | color(mydb::tui::Theme::kTextDim),
            }) | size(HEIGHT, EQUAL, 1) | bgcolor(mydb::tui::Theme::kStatusBarBg),
            
        }) | bgcolor(mydb::tui::Theme::kBackground) | color(mydb::tui::Theme::kText); // End vbox and Decorator
    }); // End Renderer lambda
    
    // Event Handler
    renderer = CatchEvent(renderer, [&](Event event) {
        if (event == Event::Escape) {
            screen.Exit();
            return true;
        }
        
        // Handle crash demo actions
        if (menu_selected == 6) { // Crash Demo
             if (event == Event::Return) {
                 // Trigger crash!
                 screen.Exit(); // Exit properly or...
                 std::cout << "\n\n!!! SIMULATING POWER FAILURE ... SYSTEM CRASH !!!\n";
                 std::cout << "Writing uncommitted transactions to WAL... [INCOMPLETE]\n";
                 std::exit(1); // Force exit
                 return true;
             }
        }
        
        if (menu_selected == 7 && event == Event::Return) {
            screen.Exit();
            return true;
        }
        return false;
    });
    
    screen.Loop(renderer);
    
#ifdef _WIN32
    if (sock != INVALID_SOCK) closesocket(sock);
    WSACleanup();
#else
    if (sock != INVALID_SOCK) close(sock);
#endif
    
    return 0;
}
