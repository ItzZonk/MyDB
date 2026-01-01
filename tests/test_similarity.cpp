/**
 * @file test_similarity.cpp
 * @brief Unit tests for fuzzy string matching algorithms
 * 
 * Tests Levenshtein and Jaro-Winkler similarity functions.
 * These algorithms are crucial for the FUZZY LIKE query feature.
 */

#include <gtest/gtest.h>
#include <mydb/util/similarity.hpp>

using namespace mydb;

// ============================================================================
// Levenshtein Distance Tests
// ============================================================================

TEST(SimilarityTest, LevenshteinIdenticalStrings) {
    EXPECT_EQ(Similarity::Levenshtein("hello", "hello"), 0);
    EXPECT_EQ(Similarity::Levenshtein("", ""), 0);
    EXPECT_EQ(Similarity::Levenshtein("test", "test"), 0);
}

TEST(SimilarityTest, LevenshteinEmptyStrings) {
    EXPECT_EQ(Similarity::Levenshtein("", "hello"), 5);
    EXPECT_EQ(Similarity::Levenshtein("hello", ""), 5);
    EXPECT_EQ(Similarity::Levenshtein("abc", ""), 3);
}

TEST(SimilarityTest, LevenshteinSingleOperations) {
    // Single insertion
    EXPECT_EQ(Similarity::Levenshtein("hello", "helo"), 1);
    // Single deletion
    EXPECT_EQ(Similarity::Levenshtein("helo", "hello"), 1);
    // Single substitution
    EXPECT_EQ(Similarity::Levenshtein("hello", "hallo"), 1);
}

TEST(SimilarityTest, LevenshteinMultipleOperations) {
    EXPECT_EQ(Similarity::Levenshtein("kitten", "sitting"), 3);
    EXPECT_EQ(Similarity::Levenshtein("saturday", "sunday"), 3);
    EXPECT_EQ(Similarity::Levenshtein("Ghent", "Gent"), 1);
}

TEST(SimilarityTest, LevenshteinSymmetric) {
    // Distance should be the same in both directions
    EXPECT_EQ(Similarity::Levenshtein("abc", "def"), Similarity::Levenshtein("def", "abc"));
    EXPECT_EQ(Similarity::Levenshtein("Ghent", "Gent"), Similarity::Levenshtein("Gent", "Ghent"));
}

// ============================================================================
// Normalized Levenshtein Tests
// ============================================================================

TEST(SimilarityTest, NormalizedLevenshteinIdentical) {
    EXPECT_DOUBLE_EQ(Similarity::NormalizedLevenshtein("hello", "hello"), 1.0);
    EXPECT_DOUBLE_EQ(Similarity::NormalizedLevenshtein("", ""), 1.0);
}

TEST(SimilarityTest, NormalizedLevenshteinRange) {
    double sim = Similarity::NormalizedLevenshtein("hello", "world");
    EXPECT_GE(sim, 0.0);
    EXPECT_LE(sim, 1.0);
}

TEST(SimilarityTest, NormalizedLevenshteinGhent) {
    // "Ghent" vs "Gent" - 1 edit, max length 5
    double sim = Similarity::NormalizedLevenshtein("Ghent", "Gent");
    EXPECT_DOUBLE_EQ(sim, 0.8);  // 1 - 1/5 = 0.8
}

// ============================================================================
// Jaro Distance Tests
// ============================================================================

TEST(SimilarityTest, JaroIdenticalStrings) {
    EXPECT_DOUBLE_EQ(Similarity::Jaro("hello", "hello"), 1.0);
}

TEST(SimilarityTest, JaroEmptyStrings) {
    EXPECT_DOUBLE_EQ(Similarity::Jaro("", ""), 1.0);
    EXPECT_DOUBLE_EQ(Similarity::Jaro("", "hello"), 0.0);
    EXPECT_DOUBLE_EQ(Similarity::Jaro("hello", ""), 0.0);
}

TEST(SimilarityTest, JaroSimilarStrings) {
    double sim = Similarity::Jaro("MARTHA", "MARHTA");
    EXPECT_GT(sim, 0.9);  // High similarity
}

// ============================================================================
// Jaro-Winkler Distance Tests
// ============================================================================

TEST(SimilarityTest, JaroWinklerIdentical) {
    EXPECT_DOUBLE_EQ(Similarity::JaroWinkler("hello", "hello"), 1.0);
}

TEST(SimilarityTest, JaroWinklerPrefixBonus) {
    // Jaro-Winkler should give higher score when prefix matches
    double jw = Similarity::JaroWinkler("MARTHA", "MARHTA");
    double j = Similarity::Jaro("MARTHA", "MARHTA");
    
    // Jaro-Winkler >= Jaro (prefix bonus)
    EXPECT_GE(jw, j);
}

TEST(SimilarityTest, JaroWinklerTypicalNames) {
    // Common misspellings
    EXPECT_GT(Similarity::JaroWinkler("Jon", "John"), 0.8);
    EXPECT_GT(Similarity::JaroWinkler("Smith", "Smyth"), 0.8);
    EXPECT_GT(Similarity::JaroWinkler("Alice", "Alyce"), 0.8);
}

// ============================================================================
// IsSimilar Tests
// ============================================================================

TEST(SimilarityTest, IsSimilarBasic) {
    EXPECT_TRUE(Similarity::IsSimilar("Jon", "John", 0.8));
    EXPECT_TRUE(Similarity::IsSimilar("hello", "hello", 1.0));
    EXPECT_FALSE(Similarity::IsSimilar("abc", "xyz", 0.5));
}

TEST(SimilarityTest, IsSimilarWithAlgorithm) {
    EXPECT_TRUE(Similarity::IsSimilar("hello", "hallo", 0.8, "levenshtein"));
    EXPECT_TRUE(Similarity::IsSimilar("hello", "hallo", 0.8, "jaro_winkler"));
}

// ============================================================================
// Case Insensitive Tests
// ============================================================================

TEST(SimilarityTest, LevenshteinIgnoreCase) {
    EXPECT_EQ(Similarity::LevenshteinIgnoreCase("Hello", "hello"), 0);
    EXPECT_EQ(Similarity::LevenshteinIgnoreCase("GHENT", "ghent"), 0);
    EXPECT_EQ(Similarity::LevenshteinIgnoreCase("ABC", "abc"), 0);
}

TEST(SimilarityTest, JaroWinklerIgnoreCase) {
    EXPECT_DOUBLE_EQ(Similarity::JaroWinklerIgnoreCase("Hello", "hello"), 1.0);
    EXPECT_DOUBLE_EQ(Similarity::JaroWinklerIgnoreCase("GHENT", "ghent"), 1.0);
}

// ============================================================================
// Edge Cases
// ============================================================================

TEST(SimilarityTest, SingleCharacterStrings) {
    EXPECT_EQ(Similarity::Levenshtein("a", "b"), 1);
    EXPECT_EQ(Similarity::Levenshtein("a", "a"), 0);
    // Single char JaroWinkler is edge case, just verify > 0
    EXPECT_GT(Similarity::JaroWinkler("a", "a"), 0.0);
}

TEST(SimilarityTest, VeryDifferentStrings) {
    double sim = Similarity::NormalizedLevenshtein("abcdef", "ghijkl");
    EXPECT_LT(sim, 0.5);  // Very different
}

TEST(SimilarityTest, LongStrings) {
    std::string s1 = "The quick brown fox jumps over the lazy dog";
    std::string s2 = "The quick brown fox jumped over the lazy dogs";
    
    double sim = Similarity::NormalizedLevenshtein(s1, s2);
    EXPECT_GT(sim, 0.9);  // Very similar
}
