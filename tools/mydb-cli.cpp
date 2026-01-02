/**
 * @file mydb-cli.cpp
 * @brief MyDB Command Line Client
 */

#include <mydb/network/protocol.hpp>
#include <mydb/common/types.hpp>
#include <mydb/config.hpp>

#include <iostream>
#include <string>
#include <sstream>
#include <vector>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "ws2_32.lib")
using socket_t = SOCKET;
#else
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
using socket_t = int;
#endif

namespace {

socket_t connect_to_server(const std::string& host, uint16_t port) {
#ifdef _WIN32
    WSADATA wsaData;
    int wsaResult = WSAStartup(MAKEWORD(2, 2), &wsaData);
    if (wsaResult != 0) {
        std::cerr << "WSAStartup failed: " << wsaResult << std::endl;
        return INVALID_SOCKET;
    }
#endif

    socket_t sock = socket(AF_INET, SOCK_STREAM, 0);
#ifdef _WIN32
    if (sock == INVALID_SOCKET) {
        std::cerr << "Socket creation failed: " << WSAGetLastError() << std::endl;
        return INVALID_SOCKET;
    }
#else
    if (sock < 0) {
        return -1;
    }
#endif

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, host.c_str(), &addr.sin_addr);

    if (connect(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
#ifdef _WIN32
        std::cerr << "Connect failed: " << WSAGetLastError() << std::endl;
        closesocket(sock);
        return INVALID_SOCKET;
#else
        close(sock);
        return -1;
#endif
    }

    return sock;
}

void close_socket(socket_t sock) {
#ifdef _WIN32
    closesocket(sock);
    WSACleanup();
#else
    close(sock);
#endif
}

bool send_request(socket_t sock, const std::vector<char>& data) {
#ifdef _WIN32
    int sent = send(sock, data.data(), static_cast<int>(data.size()), 0);
    if (sent == SOCKET_ERROR) {
        std::cerr << "Send error: " << WSAGetLastError() << std::endl;
        return false;
    }
#else
    ssize_t sent = send(sock, data.data(), data.size(), 0);
    if (sent < 0) {
        return false;
    }
#endif
    return sent == static_cast<decltype(sent)>(data.size());
}

std::vector<char> receive_response(socket_t sock) {
    std::vector<char> buffer(4096);
#ifdef _WIN32
    int received = recv(sock, buffer.data(), static_cast<int>(buffer.size()), 0);
#else
    ssize_t received = recv(sock, buffer.data(), buffer.size(), 0);
#endif
    if (received <= 0) {
        return {};
    }
    buffer.resize(received);
    return buffer;
}

std::vector<std::string> split(const std::string& s) {
    std::vector<std::string> tokens;
    std::istringstream iss(s);
    std::string token;
    while (iss >> token) {
        tokens.push_back(token);
    }
    return tokens;
}

void print_help() {
    std::cout << "Commands:" << std::endl;
    std::cout << "  GET <key>           - Get the value for a key" << std::endl;
    std::cout << "  PUT <key> <value>   - Set a key-value pair" << std::endl;
    std::cout << "  DEL <key>           - Delete a key" << std::endl;
    std::cout << "  PING                - Check server connectivity" << std::endl;
    std::cout << "  STATUS              - Get server status" << std::endl;
    std::cout << "  FLUSH               - Flush MemTable to disk" << std::endl;
    std::cout << "  COMPACT [level]     - Trigger compaction" << std::endl;
    std::cout << "  EXEC <script>       - Execute Python script" << std::endl;
    std::cout << "  HELP                - Show this help" << std::endl;
    std::cout << "  QUIT                - Exit the client" << std::endl;
}

}  // namespace

int main(int argc, char* argv[]) {
    std::string host = "127.0.0.1";
    uint16_t port = mydb::kDefaultPort;

    // Parse arguments
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if ((arg == "-h" || arg == "--host") && i + 1 < argc) {
            host = argv[++i];
        } else if ((arg == "-p" || arg == "--port") && i + 1 < argc) {
            port = static_cast<uint16_t>(std::stoi(argv[++i]));
        } else if (arg == "--help") {
            std::cout << "Usage: mydb-cli [options]" << std::endl;
            std::cout << "  -h, --host <host>  Server hostname (default: 127.0.0.1)" << std::endl;
            std::cout << "  -p, --port <port>  Server port (default: 6379)" << std::endl;
            return 0;
        }
    }

    std::cout << "MyDB CLI v" << mydb::kVersion << std::endl;
    std::cout << "Connecting to " << host << ":" << port << "..." << std::endl;

    socket_t sock = connect_to_server(host, port);
#ifdef _WIN32
    if (sock == INVALID_SOCKET) {
#else
    if (sock < 0) {
#endif
        std::cerr << "Failed to connect to server" << std::endl;
        return 1;
    }

    std::cout << "Connected! Type 'HELP' for available commands." << std::endl;
    std::cout << std::endl;

    std::string line;
    while (true) {
        std::cout << "mydb> ";
        if (!std::getline(std::cin, line)) {
            break;
        }

        auto tokens = split(line);
        if (tokens.empty()) {
            continue;
        }

        std::string cmd = tokens[0];
        for (auto& c : cmd) c = toupper(c);

        if (cmd == "QUIT" || cmd == "EXIT") {
            break;
        }

        if (cmd == "HELP") {
            print_help();
            continue;
        }

        mydb::Request request;

        if (cmd == "GET") {
            if (tokens.size() < 2) {
                std::cout << "Usage: GET <key> [AS OF <timestamp>]" << std::endl;
                continue;
            }
            std::string key = tokens[1];
            std::optional<mydb::SequenceNumber> snapshot;
            
            std::optional<std::string> field;
            
            if (tokens.size() >= 5 && tokens[2] == "AS" && tokens[3] == "OF") {
                try {
                    snapshot = std::stoull(tokens[4]);
                } catch (...) {
                    std::cout << "Invalid timestamp: " << tokens[4] << std::endl;
                    continue;
                }
            } else if (tokens.size() >= 3) {
                 // Try as field (if not a number/snapshot logic -- simplistic here)
                 // The Protocol::ParseRequest logic tries snapshot first. 
                 // Let's match typical CLI usage: GET key [field]
                 field = tokens[2];
            }
            
            request = mydb::GetRequest{key, snapshot, field};
        }
        else if (cmd == "PUT" || cmd == "SET") {
            if (tokens.size() < 3) {
                std::cout << "Usage: PUT <key> <value>" << std::endl;
                continue;
            }
            // Join remaining tokens as value
            std::string value;
            for (size_t i = 2; i < tokens.size(); ++i) {
                if (i > 2) value += " ";
                value += tokens[i];
            }
            request = mydb::PutRequest{tokens[1], value};
        }
        else if (cmd == "DEL" || cmd == "DELETE") {
            if (tokens.size() < 2) {
                std::cout << "Usage: DEL <key>" << std::endl;
                continue;
            }
            request = mydb::DeleteRequest{tokens[1]};
        }
        else if (cmd == "PING") {
            request = mydb::PingRequest{};
        }
        else if (cmd == "STATUS" || cmd == "INFO") {
            request = mydb::StatusRequest{};
        }
        else if (cmd == "FLUSH") {
            request = mydb::FlushRequest{};
        }
        else if (cmd == "COMPACT") {
            int level = -1;
            if (tokens.size() > 1) {
                level = std::stoi(tokens[1]);
            }
            request = mydb::CompactRequest{level};
        }
        else if (cmd == "EXEC") {
            if (tokens.size() < 2) {
                std::cout << "Usage: EXEC <python script>" << std::endl;
                continue;
            }
            // Join remaining tokens as script
            std::string script;
            for (size_t i = 1; i < tokens.size(); ++i) {
                if (i > 1) script += " ";
                script += tokens[i];
            }
            request = mydb::ExecPythonRequest{script};
        }
        else {
            std::cout << "Unknown command: " << cmd << std::endl;
            continue;
        }

        // Send request
        auto encoded = mydb::Protocol::EncodeRequest(request);
        if (!send_request(sock, encoded)) {
            std::cerr << "Failed to send request" << std::endl;
            continue;
        }

        // Receive response
        auto response_data = receive_response(sock);
        if (response_data.empty()) {
            std::cerr << "No response from server" << std::endl;
            continue;
        }

        auto response = mydb::Protocol::ParseResponse(
            mydb::Slice(response_data.data(), response_data.size())
        );

        if (response.ok()) {
            std::visit([](auto&& resp) {
                using T = std::decay_t<decltype(resp)>;
                if constexpr (std::is_same_v<T, mydb::OkResponse>) {
                    std::cout << resp.message << std::endl;
                }
                else if constexpr (std::is_same_v<T, mydb::ValueResponse>) {
                    std::cout << resp.value << std::endl;
                }
                else if constexpr (std::is_same_v<T, mydb::ErrorResponse>) {
                    std::cout << "Error: " << resp.message << std::endl;
                }
                else if constexpr (std::is_same_v<T, mydb::StatusResponse>) {
                    std::cout << "Entries: " << resp.entries << std::endl;
                    std::cout << "MemTable: " << resp.memtable_size << " bytes" << std::endl;
                    std::cout << "SSTables: " << resp.sstable_count << std::endl;
                    std::cout << "Version: " << resp.version << std::endl;
                }
            }, response.value());
        } else {
            std::cout << "Parse error: " << response.status().ToString() << std::endl;
        }
    }

    close_socket(sock);
    std::cout << "Goodbye!" << std::endl;
    return 0;
}
