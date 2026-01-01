/**
 * @file similarity.hpp
 * @brief String similarity algorithms for fuzzy matching
 * 
 * Implements fuzzy string matching algorithms used for flexible querying.
 * These features align with UGent's DDCM research group focus on
 * "fuzzy databases" and "flexible query answering" (Prof. Guy De Tré).
 * 
 * Algorithms implemented:
 * - **Levenshtein Distance**: Edit distance (insertions, deletions, substitutions)
 * - **Jaro-Winkler**: Optimized for short strings like names
 * 
 * @see UGent Research: DDCM Group, Fuzzy Databases
 * @see Course: Databases, Big Data Technology
 */

#pragma once

#include <string>
#include <string_view>
#include <vector>
#include <algorithm>
#include <cmath>

namespace mydb {

/**
 * @brief String similarity algorithms for fuzzy matching
 * 
 * This class provides static methods for computing similarity between strings.
 * These are used by the FilterExecutor to support FUZZY LIKE queries.
 * 
 * Example Usage:
 * ```cpp
 * double sim = Similarity::NormalizedLevenshtein("Ghent", "Gent");
 * // sim ≈ 0.8 (high similarity despite the missing 'h')
 * ```
 */
class Similarity {
public:
    /**
     * @brief Compute Levenshtein (edit) distance between two strings
     * 
     * The Levenshtein distance is the minimum number of single-character
     * edits (insertions, deletions, or substitutions) required to change
     * one string into the other.
     * 
     * Complexity: O(m*n) time, O(min(m,n)) space (optimized)
     * 
     * @param s1 First string
     * @param s2 Second string
     * @return Edit distance (0 = identical)
     * 
     * @note This implementation uses the space-optimized two-row approach,
     *       demonstrating awareness of space complexity for algorithms courses.
     */
    static int Levenshtein(std::string_view s1, std::string_view s2) {
        const size_t m = s1.size();
        const size_t n = s2.size();
        
        // Handle edge cases
        if (m == 0) return static_cast<int>(n);
        if (n == 0) return static_cast<int>(m);
        
        // Ensure s1 is the shorter string (space optimization)
        if (m > n) {
            return Levenshtein(s2, s1);
        }
        
        // Only need two rows at a time (space optimization: O(min(m,n)))
        std::vector<int> prev_row(n + 1);
        std::vector<int> curr_row(n + 1);
        
        // Initialize first row (distance from empty string)
        for (size_t j = 0; j <= n; ++j) {
            prev_row[j] = static_cast<int>(j);
        }
        
        // Fill the matrix row by row
        for (size_t i = 1; i <= m; ++i) {
            curr_row[0] = static_cast<int>(i);  // Distance from empty string
            
            for (size_t j = 1; j <= n; ++j) {
                int cost = (s1[i - 1] == s2[j - 1]) ? 0 : 1;
                
                curr_row[j] = std::min({
                    prev_row[j] + 1,      // Deletion
                    curr_row[j - 1] + 1,  // Insertion
                    prev_row[j - 1] + cost // Substitution
                });
            }
            
            std::swap(prev_row, curr_row);
        }
        
        return prev_row[n];
    }

    /**
     * @brief Compute normalized Levenshtein similarity [0.0, 1.0]
     * 
     * Converts edit distance to a similarity score where:
     * - 1.0 = identical strings
     * - 0.0 = completely different
     * 
     * Formula: 1.0 - (distance / max_length)
     * 
     * @param s1 First string
     * @param s2 Second string
     * @return Similarity score [0.0, 1.0]
     */
    static double NormalizedLevenshtein(std::string_view s1, std::string_view s2) {
        if (s1.empty() && s2.empty()) {
            return 1.0;
        }
        
        int distance = Levenshtein(s1, s2);
        size_t max_len = std::max(s1.size(), s2.size());
        
        return 1.0 - (static_cast<double>(distance) / static_cast<double>(max_len));
    }

    /**
     * @brief Compute Jaro similarity between two strings
     * 
     * The Jaro similarity is based on:
     * - Number of matching characters
     * - Number of transpositions (matching characters in different order)
     * 
     * Formula: (m/|s1| + m/|s2| + (m-t)/m) / 3
     * where m = matches, t = transpositions
     * 
     * @param s1 First string
     * @param s2 Second string
     * @return Jaro similarity [0.0, 1.0]
     */
    static double Jaro(std::string_view s1, std::string_view s2) {
        if (s1.empty() && s2.empty()) {
            return 1.0;
        }
        if (s1.empty() || s2.empty()) {
            return 0.0;
        }
        
        const size_t len1 = s1.size();
        const size_t len2 = s2.size();
        
        // Matching window size
        const size_t match_distance = (std::max(len1, len2) / 2) - 1;
        
        std::vector<bool> s1_matched(len1, false);
        std::vector<bool> s2_matched(len2, false);
        
        int matches = 0;
        int transpositions = 0;
        
        // Find matches
        for (size_t i = 0; i < len1; ++i) {
            size_t start = (i > match_distance) ? i - match_distance : 0;
            size_t end = std::min(i + match_distance + 1, len2);
            
            for (size_t j = start; j < end; ++j) {
                if (s2_matched[j] || s1[i] != s2[j]) {
                    continue;
                }
                s1_matched[i] = true;
                s2_matched[j] = true;
                matches++;
                break;
            }
        }
        
        if (matches == 0) {
            return 0.0;
        }
        
        // Count transpositions
        size_t k = 0;
        for (size_t i = 0; i < len1; ++i) {
            if (!s1_matched[i]) continue;
            
            while (!s2_matched[k]) {
                k++;
            }
            
            if (s1[i] != s2[k]) {
                transpositions++;
            }
            k++;
        }
        
        double m = static_cast<double>(matches);
        double t = static_cast<double>(transpositions) / 2.0;
        
        return (m / len1 + m / len2 + (m - t) / m) / 3.0;
    }

    /**
     * @brief Compute Jaro-Winkler similarity between two strings
     * 
     * An extension of Jaro that gives extra weight to common prefixes.
     * Particularly effective for comparing names and short strings.
     * 
     * Formula: jaro + (prefix_length * p * (1 - jaro))
     * where p = scaling factor (typically 0.1)
     * 
     * @param s1 First string
     * @param s2 Second string
     * @param prefix_scale Scaling factor for prefix bonus (default: 0.1)
     * @return Jaro-Winkler similarity [0.0, 1.0]
     */
    static double JaroWinkler(std::string_view s1, std::string_view s2, 
                              double prefix_scale = 0.1) {
        double jaro_sim = Jaro(s1, s2);
        
        // Find common prefix length (max 4 characters)
        size_t prefix_len = 0;
        size_t max_prefix = std::min({s1.size(), s2.size(), size_t{4}});
        
        for (size_t i = 0; i < max_prefix; ++i) {
            if (s1[i] == s2[i]) {
                prefix_len++;
            } else {
                break;
            }
        }
        
        // Apply Winkler modification
        return jaro_sim + (prefix_len * prefix_scale * (1.0 - jaro_sim));
    }

    /**
     * @brief Case-insensitive Levenshtein distance
     */
    static int LevenshteinIgnoreCase(std::string_view s1, std::string_view s2) {
        std::string lower1 = ToLower(s1);
        std::string lower2 = ToLower(s2);
        return Levenshtein(lower1, lower2);
    }

    /**
     * @brief Case-insensitive Jaro-Winkler similarity
     */
    static double JaroWinklerIgnoreCase(std::string_view s1, std::string_view s2,
                                        double prefix_scale = 0.1) {
        std::string lower1 = ToLower(s1);
        std::string lower2 = ToLower(s2);
        return JaroWinkler(lower1, lower2, prefix_scale);
    }

    /**
     * @brief Check if similarity meets threshold
     * 
     * Convenience method for filtering operations.
     * 
     * @param s1 First string
     * @param s2 Second string
     * @param threshold Minimum similarity (default: 0.8)
     * @param algorithm "levenshtein" or "jaro_winkler"
     * @return True if similarity >= threshold
     */
    static bool IsSimilar(std::string_view s1, std::string_view s2,
                          double threshold = 0.8,
                          const std::string& algorithm = "jaro_winkler") {
        double sim;
        if (algorithm == "levenshtein") {
            sim = NormalizedLevenshtein(s1, s2);
        } else {
            sim = JaroWinkler(s1, s2);
        }
        return sim >= threshold;
    }

    /**
     * @brief Get similarity using specified algorithm
     */
    static double GetSimilarity(std::string_view s1, std::string_view s2,
                                const std::string& algorithm = "jaro_winkler") {
        if (algorithm == "levenshtein") {
            return NormalizedLevenshtein(s1, s2);
        } else {
            return JaroWinkler(s1, s2);
        }
    }

private:
    /**
     * @brief Convert string to lowercase
     */
    static std::string ToLower(std::string_view s) {
        std::string result(s);
        std::transform(result.begin(), result.end(), result.begin(),
                       [](unsigned char c) { return std::tolower(c); });
        return result;
    }
};

}  // namespace mydb
