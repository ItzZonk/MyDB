/**
 * @file theme.hpp
 * @brief PowerShell-style theming for MyDB TUI
 * 
 * Defines color schemes and styling constants for the dashboard.
 * Inspired by Windows PowerShell and Windows Terminal aesthetics.
 */

#pragma once

#include <ftxui/dom/elements.hpp>
#include <ftxui/screen/color.hpp>

namespace mydb::tui {

/**
 * @brief PowerShell-inspired color theme
 */
struct Theme {
    // Background colors
    static const ftxui::Color kBackground;      // Deep blue
    static const ftxui::Color kSidebarBg;       // Darker blue
    static const ftxui::Color kStatusBarBg;     // Lighter blue
    
    // Foreground colors
    static const ftxui::Color kText;
    static const ftxui::Color kTextDim;
    static const ftxui::Color kHighlight;
    static const ftxui::Color kSelected;
    
    // Status colors
    static const ftxui::Color kSuccess;
    static const ftxui::Color kWarning;
    static const ftxui::Color kError;
    static const ftxui::Color kInfo;
    
    // Buffer Pool visualization colors
    static const ftxui::Color kFrameEmpty;
    static const ftxui::Color kFrameClean;
    static const ftxui::Color kFrameDirty;
    static const ftxui::Color kFramePinned;
    
    // B+ Tree visualization colors
    static const ftxui::Color kNodeInternal;
    static const ftxui::Color kNodeLeaf;
    static const ftxui::Color kNodeSplit;
    static const ftxui::Color kSearchPath;
    
    // WAL visualization colors
    static const ftxui::Color kTxCommitted;
    static const ftxui::Color kTxActive;
    static const ftxui::Color kTxAborted;
};

// Define constants inline (C++17) or in cpp
inline const ftxui::Color Theme::kBackground = ftxui::Color::RGB(1, 36, 86);
inline const ftxui::Color Theme::kSidebarBg = ftxui::Color::RGB(0, 30, 70);
inline const ftxui::Color Theme::kStatusBarBg = ftxui::Color::RGB(0, 50, 100);
inline const ftxui::Color Theme::kText = ftxui::Color::White;
inline const ftxui::Color Theme::kTextDim = ftxui::Color::GrayLight;
inline const ftxui::Color Theme::kHighlight = ftxui::Color::Cyan;
inline const ftxui::Color Theme::kSelected = ftxui::Color::Yellow;
inline const ftxui::Color Theme::kSuccess = ftxui::Color::Green;
inline const ftxui::Color Theme::kWarning = ftxui::Color::Yellow;
inline const ftxui::Color Theme::kError = ftxui::Color::Red;
inline const ftxui::Color Theme::kInfo = ftxui::Color::Cyan;
inline const ftxui::Color Theme::kFrameEmpty = ftxui::Color::GrayDark;
inline const ftxui::Color Theme::kFrameClean = ftxui::Color::Green;
inline const ftxui::Color Theme::kFrameDirty = ftxui::Color::Red;
inline const ftxui::Color Theme::kFramePinned = ftxui::Color::Blue;
inline const ftxui::Color Theme::kNodeInternal = ftxui::Color::Magenta;
inline const ftxui::Color Theme::kNodeLeaf = ftxui::Color::Cyan;
inline const ftxui::Color Theme::kNodeSplit = ftxui::Color::RedLight;
inline const ftxui::Color Theme::kSearchPath = ftxui::Color::Yellow;
inline const ftxui::Color Theme::kTxCommitted = ftxui::Color::Green;
inline const ftxui::Color Theme::kTxActive = ftxui::Color::Yellow;
inline const ftxui::Color Theme::kTxAborted = ftxui::Color::Red;

/**
 * @brief Apply PowerShell theme to an element
 */
inline ftxui::Element ApplyTheme(ftxui::Element element) {
    using namespace ftxui;
    return element | bgcolor(Theme::kBackground) | color(Theme::kText);
}

/**
 * @brief Create a themed border
 */
inline ftxui::Element ThemedBorder(ftxui::Element content, const std::string& title = "") {
    using namespace ftxui;
    if (title.empty()) {
        return content | border | color(Theme::kHighlight);
    }
    return window(text(title) | color(Theme::kHighlight), content);
}

/**
 * @brief Create a status indicator
 */
inline ftxui::Element StatusIndicator(const std::string& label, bool active) {
    using namespace ftxui;
    auto dot = active ? text("●") | color(Theme::kSuccess) 
                      : text("○") | color(Theme::kError);
    return hbox({dot, text(" " + label)});
}

/**
 * @brief Create a gauge with theme colors
 */
inline ftxui::Element ThemedGauge(float value, const std::string& label = "") {
    using namespace ftxui;
    auto g = gauge(value) | color(Theme::kHighlight) | flex;
    if (label.empty()) {
        return g;
    }
    return hbox({text(label + " ") | color(Theme::kTextDim), g});
}

/**
 * @brief Create a highlighted text element
 */
inline ftxui::Element Highlight(const std::string& text_str) {
    using namespace ftxui;
    return text(text_str) | color(Theme::kHighlight) | bold;
}

/**
 * @brief Create an error message
 */
inline ftxui::Element ErrorText(const std::string& message) {
    using namespace ftxui;
    return text("ERROR: " + message) | color(Theme::kError);
}

/**
 * @brief Create a success message
 */
inline ftxui::Element SuccessText(const std::string& message) {
    using namespace ftxui;
    return text(message) | color(Theme::kSuccess);
}

}  // namespace mydb::tui
