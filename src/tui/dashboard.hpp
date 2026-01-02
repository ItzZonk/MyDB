/**
 * @file dashboard.hpp
 * @brief Main TUI Dashboard Controller
 * 
 * Implements the PowerShell-style dashboard with sidebar navigation,
 * active view rendering, and status bar.
 */

#pragma once

#include "theme.hpp"

#include <ftxui/component/component.hpp>
#include <ftxui/component/screen_interactive.hpp>
#include <ftxui/dom/elements.hpp>

#include <mydb/db.hpp>

#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace mydb::tui {

/**
 * @brief View identifier for dashboard tabs
 */
enum class ViewId {
    SqlConsole,
    SchemaInspector,
    SystemMetrics,
    BPlusTreeVisualizer,
    BufferPoolView,
    WalLogs,
    CrashDemo,
    Exit
};

/**
 * @brief Status bar data
 */
struct DashboardStatus {
    bool connected{false};
    std::string server_address{"localhost:6379"};
    uint64_t transaction_id{0};
    size_t memory_usage_mb{0};
    float cache_hit_ratio{0.0f};
    std::string last_error;
};

/**
 * @brief Main dashboard controller
 * 
 * Manages the PowerShell-style TUI with:
 * - Sidebar navigation
 * - Tab-based content views
 * - Status bar with live metrics
 */
class Dashboard {
public:
    /**
     * @brief Construct dashboard
     * @param db Database instance (optional, can be nullptr for standalone mode)
     */
    explicit Dashboard(Database* db = nullptr) : db_(db) {
        InitializeMenuEntries();
    }

    /**
     * @brief Run the dashboard (blocking)
     */
    int Run() {
        using namespace ftxui;
        
        auto screen = ScreenInteractive::Fullscreen();
        
        // Sidebar menu
        auto menu = Menu(&menu_entries_, &selected_menu_);
        
        // Main content area (changes based on selection)
        auto content = Renderer([&] {
            return RenderActiveView();
        });
        
        // Layout: sidebar | content
        auto layout = Container::Horizontal({
            menu,
            content | flex,
        });
        
        // Main renderer with theming
        auto renderer = Renderer(layout, [&] {
            return vbox({
                // Header
                RenderHeader(),
                separator() | color(Theme::kHighlight),
                // Main area
                hbox({
                    // Sidebar
                    vbox({
                        text(" Navigation") | bold | color(Theme::kHighlight),
                        separator(),
                        menu->Render() | frame | size(WIDTH, EQUAL, 22),
                    }) | bgcolor(Theme::kSidebarBg),
                    separator() | color(Theme::kHighlight),
                    // Content
                    RenderActiveView() | flex,
                }) | flex,
                // Status bar
                separator() | color(Theme::kHighlight),
                RenderStatusBar(),
            }) | bgcolor(Theme::kBackground) | color(Theme::kText);
        });
        
        // Handle exit
        renderer = CatchEvent(renderer, [&](Event event) {
            if (event == Event::Escape || 
                (selected_menu_ == static_cast<int>(ViewId::Exit) && 
                 event == Event::Return)) {
                screen.Exit();
                return true;
            }
            return false;
        });
        
        screen.Loop(renderer);
        return 0;
    }

    /**
     * @brief Update status
     */
    void UpdateStatus(const DashboardStatus& status) {
        status_ = status;
    }

private:
    void InitializeMenuEntries() {
        menu_entries_ = {
            " 📝 SQL Console",
            " 📋 Schema Inspector",
            " 📊 System Metrics",
            " 🌳 B+ Tree Visualizer",
            " 💾 Buffer Pool",
            " 📜 WAL Logs",
            " 💥 Crash Demo",
            " 🚪 Exit"
        };
    }

    ftxui::Element RenderHeader() {
        using namespace ftxui;
        return hbox({
            text("  ") | bgcolor(Theme::kSidebarBg),
            text("╔══════════════════════════════════════════════════════════════════════╗") | color(Theme::kHighlight),
        }) | size(HEIGHT, EQUAL, 1) |
        hbox({
            text("  ") | bgcolor(Theme::kSidebarBg),
            text("║") | color(Theme::kHighlight),
            text("  MyDB Dashboard  ") | bold | color(Theme::kSelected),
            text("- High-Performance LSM-Tree Database with RDBMS Features") | color(Theme::kTextDim),
            filler(),
            text("║") | color(Theme::kHighlight),
        }) |
        hbox({
            text("  ") | bgcolor(Theme::kSidebarBg),
            text("╚══════════════════════════════════════════════════════════════════════╝") | color(Theme::kHighlight),
        });
    }

    ftxui::Element RenderActiveView() {
        using namespace ftxui;
        
        ViewId view = static_cast<ViewId>(selected_menu_);
        
        switch (view) {
            case ViewId::SqlConsole:
                return RenderSqlConsole();
            case ViewId::SchemaInspector:
                return RenderSchemaInspector();
            case ViewId::SystemMetrics:
                return RenderSystemMetrics();
            case ViewId::BPlusTreeVisualizer:
                return RenderBPlusTreeVisualizer();
            case ViewId::BufferPoolView:
                return RenderBufferPoolView();
            case ViewId::WalLogs:
                return RenderWalLogs();
            case ViewId::CrashDemo:
                return RenderCrashDemo();
            case ViewId::Exit:
                return RenderExitConfirm();
            default:
                return text("Unknown view") | center;
        }
    }

    ftxui::Element RenderSqlConsole() {
        using namespace ftxui;
        return vbox({
            text(" SQL Console") | bold | color(Theme::kHighlight),
            separator(),
            text(" Enter SQL commands. Type 'help' for available commands.") | color(Theme::kTextDim),
            text(""),
            hbox({text("mydb> ") | color(Theme::kSuccess), text("_") | blink}),
            filler(),
            text(" Press 'q' to return to dashboard") | color(Theme::kTextDim),
        }) | flex | border | color(Theme::kHighlight);
    }

    ftxui::Element RenderSchemaInspector() {
        using namespace ftxui;
        
        std::vector<Element> tables;
        tables.push_back(text(" Tables in Database:") | bold);
        tables.push_back(separator());
        
        if (db_) {
            // TODO: Get tables from catalog
            tables.push_back(text("  • users (id, name, email)"));
            tables.push_back(text("  • orders (id, user_id, total)"));
        } else {
            tables.push_back(text("  No database connected") | color(Theme::kWarning));
        }
        
        return vbox(tables) | flex | border | color(Theme::kHighlight);
    }

    ftxui::Element RenderSystemMetrics() {
        using namespace ftxui;
        
        return vbox({
            text(" System Metrics") | bold | color(Theme::kHighlight),
            separator(),
            text(""),
            hbox({text(" Cache Hit Ratio: "), gauge(status_.cache_hit_ratio) | flex | color(Theme::kSuccess)}),
            text(""),
            hbox({text(" Memory Usage:    "), text(std::to_string(status_.memory_usage_mb) + " MB") | color(Theme::kInfo)}),
            hbox({text(" Active TX ID:    "), text("#" + std::to_string(status_.transaction_id)) | color(Theme::kInfo)}),
            text(""),
            text(" Buffer Pool Stats:") | color(Theme::kTextDim),
            hbox({text("   Total Frames: "), text("128")}),
            hbox({text("   Dirty Pages:  "), text("15") | color(Theme::kWarning)}),
            hbox({text("   Pinned:       "), text("3") | color(Theme::kInfo)}),
        }) | flex | border | color(Theme::kHighlight);
    }

    ftxui::Element RenderBPlusTreeVisualizer() {
        using namespace ftxui;
        
        return vbox({
            text(" B+ Tree Index Structure") | bold | color(Theme::kHighlight),
            separator(),
            text(""),
            text(" ▼ Internal Node [Page 0]") | color(Theme::kNodeInternal),
            text("   Keys: [10, 20, 30]") | color(Theme::kTextDim),
            text("   ├─▶ Leaf [Page 1]: [1,2,3,5,7,9]") | color(Theme::kNodeLeaf),
            text("   ├─▶ Leaf [Page 2]: [11,12,15,18]") | color(Theme::kNodeLeaf),
            text("   ├─▶ Leaf [Page 3]: [21,22,25,28]") | color(Theme::kNodeLeaf),
            text("   └─▶ Leaf [Page 4]: [31,35,40,45]") | color(Theme::kNodeLeaf),
            text(""),
            text(" Legend:") | color(Theme::kTextDim),
            hbox({text("   "), text("■") | color(Theme::kNodeInternal), text(" Internal  "), 
                  text("■") | color(Theme::kNodeLeaf), text(" Leaf  "),
                  text("■") | color(Theme::kSearchPath), text(" Search Path")}),
        }) | flex | border | color(Theme::kHighlight);
    }

    ftxui::Element RenderBufferPoolView() {
        using namespace ftxui;
        
        // Create a grid of frames
        std::vector<Element> rows;
        rows.push_back(text(" Buffer Pool Frame Status") | bold | color(Theme::kHighlight));
        rows.push_back(separator());
        rows.push_back(text(""));
        
        // 8x4 grid of frames
        for (int row = 0; row < 4; ++row) {
            std::vector<Element> cells;
            for (int col = 0; col < 8; ++col) {
                int frame = row * 8 + col;
                // Simulate different states
                Color c = Theme::kFrameEmpty;
                if (frame < 10) c = Theme::kFrameClean;
                else if (frame < 15) c = Theme::kFrameDirty;
                else if (frame < 18) c = Theme::kFramePinned;
                
                cells.push_back(text(" " + std::to_string(frame) + " ") | bgcolor(c) | border);
            }
            rows.push_back(hbox(cells) | center);
        }
        
        rows.push_back(text(""));
        rows.push_back(hbox({
            text(" "), text("■") | bgcolor(Theme::kFrameEmpty), text(" Empty  "),
            text("■") | bgcolor(Theme::kFrameClean), text(" Clean  "),
            text("■") | bgcolor(Theme::kFrameDirty), text(" Dirty  "),
            text("■") | bgcolor(Theme::kFramePinned), text(" Pinned"),
        }));
        
        return vbox(rows) | flex | border | color(Theme::kHighlight);
    }

    ftxui::Element RenderWalLogs() {
        using namespace ftxui;
        
        return vbox({
            text(" Write-Ahead Log (WAL)") | bold | color(Theme::kHighlight),
            separator(),
            text(""),
            hbox({text(" LSN "), text(" | "), text("TX ID"), text(" | "), text("Operation"), text(" | "), text("Status")}),
            separator(),
            hbox({text(" 001 "), text(" | "), text("#1001"), text(" | "), text("INSERT users"), text(" | "), text("COMMITTED") | color(Theme::kTxCommitted)}),
            hbox({text(" 002 "), text(" | "), text("#1001"), text(" | "), text("INSERT orders"), text(" | "), text("COMMITTED") | color(Theme::kTxCommitted)}),
            hbox({text(" 003 "), text(" | "), text("#1002"), text(" | "), text("UPDATE users"), text(" | "), text("ACTIVE") | color(Theme::kTxActive)}),
            hbox({text(" 004 "), text(" | "), text("#1002"), text(" | "), text("DELETE items"), text(" | "), text("ACTIVE") | color(Theme::kTxActive)}),
            text(""),
            text(" Last Checkpoint: LSN 001") | color(Theme::kTextDim),
        }) | flex | border | color(Theme::kHighlight);
    }

    ftxui::Element RenderCrashDemo() {
        using namespace ftxui;
        
        return vbox({
            text(" 💥 Crash Recovery Demo") | bold | color(Theme::kHighlight),
            separator(),
            text(""),
            text(" This demonstrates MyDB's ACID durability guarantees.") | color(Theme::kTextDim),
            text(""),
            text(" Steps:") | color(Theme::kInfo),
            text("   1. Execute some INSERT operations"),
            text("   2. Click 'Simulate Crash' to terminate abruptly"),
            text("   3. Restart the application"),
            text("   4. Watch the WAL recovery animation"),
            text(""),
            text(""),
            hbox({
                text(" [ Simulate Crash ]") | bold | bgcolor(Theme::kError) | color(Theme::kText),
                text("   "),
                text(" [ Insert Test Data ]") | bold | bgcolor(Theme::kSuccess) | color(Theme::kText),
            }) | center,
            text(""),
            text(" Warning: This will terminate the application!") | color(Theme::kWarning) | center,
        }) | flex | border | color(Theme::kHighlight);
    }

    ftxui::Element RenderExitConfirm() {
        using namespace ftxui;
        
        return vbox({
            text(""),
            text(" Exit MyDB Dashboard?") | bold | center,
            text(""),
            text(" Press Enter to confirm, Esc to cancel") | color(Theme::kTextDim) | center,
        }) | center | border | color(Theme::kWarning);
    }

    ftxui::Element RenderStatusBar() {
        using namespace ftxui;
        
        return hbox({
            StatusIndicator("Connected", status_.connected),
            text("  │  "),
            text("Server: " + status_.server_address) | color(Theme::kTextDim),
            text("  │  "),
            text("TX: #" + std::to_string(status_.transaction_id)) | color(Theme::kInfo),
            text("  │  "),
            text("Memory: " + std::to_string(status_.memory_usage_mb) + "MB"),
            text("  │  "),
            text("Cache: "),
            gauge(status_.cache_hit_ratio) | size(WIDTH, EQUAL, 10) | color(Theme::kSuccess),
            text(" " + std::to_string(static_cast<int>(status_.cache_hit_ratio * 100)) + "%"),
            filler(),
            status_.last_error.empty() ? text("") : text(status_.last_error) | color(Theme::kError),
        }) | bgcolor(Theme::kStatusBarBg) | size(HEIGHT, EQUAL, 1);
    }

    Database* db_;
    DashboardStatus status_;
    std::vector<std::string> menu_entries_;
    int selected_menu_{0};
};

}  // namespace mydb::tui
