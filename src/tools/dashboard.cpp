#define NOMINMAX // Fix macro conflicts with Windows headers

#include <ftxui/dom/elements.hpp>
#include <ftxui/screen/screen.hpp>
#include <ftxui/component/component.hpp>
#include <ftxui/component/screen_interactive.hpp>
#include <ftxui/component/event.hpp>
#include <mydb/network/protocol.hpp>
#include <mydb/common/status.hpp>
#include <mydb/config.hpp>

#include <string>
#include <vector>
#include <cmath>
#include <memory>
#include <chrono>
#include <iostream>
#include <sstream>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "ws2_32.lib")
using socket_t = SOCKET;
#define INVALID_SOCK INVALID_SOCKET
#else
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
using socket_t = int;
#define INVALID_SOCK -1
#endif

using namespace ftxui;

// Theme Colors (Ghent University inspired: Blue & Yellow)
auto ThemeColor_Bg = Color::Blue;
auto ThemeColor_Fg = Color::White;
auto ThemeColor_Accent = Color::Yellow;
auto ThemeColor_Metric = Color::Cyan;

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
// Main
// ============================================================================
int main() {
    auto screen = ScreenInteractive::Fullscreen();
    socket_t sock = Connect("127.0.0.1", mydb::kDefaultPort);
    
    // State
    int tab_selected = 0;
    std::vector<std::string> tab_entries = { "  Console  ", "  Metrics  ", "  Logs     ", "  Settings " };
    int poll_counter = 0;
    
    std::string command_buffer;
    std::vector<std::string> console_history;
    console_history.push_back("MyDB Dashboard v1.0.0 initialized.");
    
    if (sock != INVALID_SOCK) {
         console_history.push_back("Connected to local server at port " + std::to_string(mydb::kDefaultPort));
    } else {
         console_history.push_back("Failed to connect to server! Is it running?");
    }

    // Input Logic
    auto process_command = [&] {
        if (command_buffer.empty()) return;
        console_history.push_back(" > " + command_buffer);
        
        if (sock == INVALID_SOCK) {
            console_history.push_back("Error: Not connected");
            command_buffer.clear();
            return;
        }

        std::stringstream ss(command_buffer);
        std::string cmd;
        ss >> cmd;
        for (auto& c : cmd) c = toupper(c);
        
        mydb::Request req;
        bool valid = true;
        
        if (cmd == "GET") {
            std::string key; ss >> key;
            std::string as_kw, of_kw, ts_str;
            std::optional<mydb::SequenceNumber> snap;
            if (ss >> as_kw >> of_kw >> ts_str) {
                 if ((as_kw == "AS" || as_kw == "as") && (of_kw == "OF" || of_kw == "of")) {
                     try { snap = std::stoull(ts_str); } catch(...) {}
                 }
            }
            req = mydb::GetRequest{key, snap};
        }
        else if (cmd == "PUT") {
            std::string key, val; ss >> key;
            std::getline(ss, val);
            if (!val.empty() && val[0] == ' ') val.erase(0, 1);
            req = mydb::PutRequest{key, val};
        }
        else if (cmd == "DEL") {
            std::string key; ss >> key;
            req = mydb::DeleteRequest{key};
        }
        else if (cmd == "PING") {
            req = mydb::PingRequest{};
        }
        else if (cmd == "STATUS") {
            req = mydb::StatusRequest{};
        }
        else if (cmd == "FLUSH") {
            req = mydb::FlushRequest{};
        }
        else if (cmd == "COMPACT") {
             int lvl = -1; ss >> lvl;
             req = mydb::CompactRequest{lvl};
        }
        else {
            console_history.push_back("Unknown command");
            valid = false;
        }
        
        if (valid) {
            auto bytes = mydb::Protocol::EncodeRequest(req);
            if (Send(sock, bytes)) {
                auto resp_bytes = Recv(sock);
                if (resp_bytes.empty()) {
                    console_history.push_back("No response / Connection closed");
                } else {
                    auto resp = mydb::Protocol::ParseResponse(mydb::Slice(resp_bytes.data(), resp_bytes.size()));
                    if (resp.ok()) {
                        std::visit([&](auto&& r) {
                            using T = std::decay_t<decltype(r)>;
                            if constexpr (std::is_same_v<T, mydb::ValueResponse>) {
                                console_history.push_back(r.value);
                            } else if constexpr (std::is_same_v<T, mydb::OkResponse>) {
                                console_history.push_back(r.message);
                            } else if constexpr (std::is_same_v<T, mydb::ErrorResponse>) {
                                console_history.push_back("Error (" + std::to_string(r.code) + "): " + r.message);
                            } else if constexpr (std::is_same_v<T, mydb::StatusResponse>) {
                                console_history.push_back("Entries: " + std::to_string(r.entries));
                                console_history.push_back("SSTables: " + std::to_string(r.sstable_count));
                            }
                        }, resp.value());
                    } else {
                        console_history.push_back("Protocol Error: " + resp.status().message());
                    }
                }
            } else {
                 console_history.push_back("Send failed");
            }
        }
        command_buffer.clear();
    };

    InputOption input_option;
    input_option.on_enter = process_command;
    
    // Components
    auto menu = Menu(&tab_entries, &tab_selected, MenuOption::VerticalAnimated());
    Component input_component = Input(&command_buffer, "Enter command...", input_option);

    // Renderer Helpers
    auto render_console = [&] {
        Elements history_elements;
        int start_idx = std::max(0, (int)console_history.size() - 15);
        for (int i = start_idx; i < console_history.size(); ++i) {
            auto& line = console_history[i];
            if (line.rfind(" > ", 0) == 0) {
                history_elements.push_back(text(line) | color(ThemeColor_Accent));
            } else {
                history_elements.push_back(text(line));
            }
        }

        return vbox({
            text("Interactive Console") | bold | color(ThemeColor_Accent),
            separator(),
            vbox(std::move(history_elements)) | flex,
            separator(),
            hbox({
                text(" mydb> ") | bold | color(ThemeColor_Accent),
                input_component->Render()
            })
        });
    };

    auto render_metrics = [&] {
        poll_counter++;
        float progress = (std::sin(poll_counter * 0.1f) + 1.0f) / 2.0f;
        
        return vbox({
             text("System Vitality") | bold | center | color(ThemeColor_Metric),
             separator(),
             hbox({
                 gauge(0.45) | color(Color::Green) | flex,
                 text("  RAM Usage: 64MB")
             }),
             text(" "),
             hbox({
                 gauge(progress) | color(Color::Red) | flex,
                 text("  CPU Load")
             }),
             text(" "),
             hbox({
                 text("SSTables:    ") | bold, text("12"),
             }),
             hbox({
                 text("MemTable:    ") | bold, text("4.2 MB"),
             }),
             separator(),
             text("Write IOPS History") | center,
             graph([&](int width, int height) {
                 std::vector<int> result(width);
                 for (int i = 0; i < width; ++i) {
                     float val = 0.5f + 0.4f * std::sin((i + poll_counter) * 0.2f);
                     result[i] = static_cast<int>(val * height);
                 }
                 return result;
             }) | color(ThemeColor_Metric) | flex
        });
    };

    auto render_status = [&] {
        return vbox({
             text(" STATUS ") | bold | bgcolor(Color::Green) | color(Color::Black) | center,
             separator(),
             text(" Uptime: ") | bold, text(" 00:15:23"),
             text(" Connection: ") | bold, text(sock != INVALID_SOCK ? " ONLINE " : " OFFLINE ") | color(sock != INVALID_SOCK ? Color::Green : Color::Red),
             text(" Version: ") | bold, text(" 1.0.0-beta"),
             separator(),
             text(" Tips: ") | bold | color(ThemeColor_Accent),
             text("Press ESC to exit"),
             text("Use UP/DOWN for menu"),
        });
    };

    auto layout = Container::Vertical({
        menu,
        input_component
    });

    auto renderer = Renderer(layout, [&] {
        return hbox({
            vbox({
                text(" MYDB ") | bold | color(ThemeColor_Accent) | center,
                separator(),
                menu->Render()
            }) | border | size(WIDTH, EQUAL, 20),

            (tab_selected == 0 ? render_console() : 
             tab_selected == 1 ? render_metrics() :
             text("Not implemented yet") | center) | flex | border,

            render_status() | border | size(WIDTH, EQUAL, 25)
        });
    });

    screen.Loop(renderer);
    return 0;
}
