/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.query;

import org.apache.lucene.util.automaton.RegExp;

/**
 * Builds the Lance SQL predicate for a {@code wildcard}, {@code regexp}
 * or {@code prefix} query on a Utf8 column, so the same SQL is produced
 * whether the query arrives through the field type
 * ({@code LanceTextFieldType.wildcardQuery} and friends) or through
 * the query planner's printer (the coordinator's count only scan,
 * {@code lance_knn.filter}, the FTS prefilter of a {@code bool}).
 *
 * <p>The predicate is evaluated by Lance's scan filter (DataFusion SQL)
 * against the raw stored string of every row, not against analyzed
 * tokens: Lance's inverted index has no wildcard or regexp query type
 * and does not expose its term dictionary, and a raw string scan is
 * what {@code pylance} users get from the same filter. Consequences
 * the caller documents: matching is case sensitive unless the query
 * asks otherwise, and the pattern has to match the whole stored value
 * the way Lucene's {@code WildcardQuery} / {@code RegexpQuery} match
 * the whole term.
 *
 * <ul>
 *   <li>wildcard: Lucene {@code *} (any string) and {@code ?} (one
 *       character) become SQL {@code LIKE}'s {@code %} and {@code _};
 *       a literal {@code %}, {@code _} or {@code \} in the pattern is
 *       escaped with {@code \}, and {@code ESCAPE '\'} names that
 *       escape (DataFusion accepts the backslash only). Lucene's
 *       {@code \x} escape yields the literal {@code x}.
 *       {@code case_insensitive} switches to {@code ILIKE}.</li>
 *   <li>regexp: DataFusion's {@code regexp_like} runs the Rust
 *       {@code regex} crate, whose syntax covers the part of Lucene's
 *       {@link RegExp} grammar users write most ({@code .}, {@code *},
 *       {@code +}, {@code ?}, <code>{n,m}</code>, {@code [...]},
 *       {@code (...)}, {@code |}, {@code \d} and the other predefined
 *       classes). That part is passed through unchanged, wrapped in
 *       {@code ^(?:...)$} because Lucene's regexp matches the whole
 *       term while {@code regexp_like} searches for a substring. The
 *       operators only Lucene has ({@code ~} complement, {@code &}
 *       intersection, {@code <n-m>} numeric interval, {@code @} any
 *       string, {@code #} empty language, {@code "..."} quoted
 *       literal) are rejected with {@link IllegalArgumentException}
 *       when the query's syntax flags enable them, since Rust would
 *       read them as literal characters and silently return a
 *       different answer. Anything else Rust's parser rejects fails
 *       when Lance plans the scan. {@code case_insensitive} becomes a
 *       leading {@code (?i)}.</li>
 *   <li>prefix: {@code starts_with(column, 'value')};
 *       {@code case_insensitive} lowers both sides.</li>
 * </ul>
 */
public final class LanceStringPatternSql {

    private static final String LIKE_ESCAPE_CLAUSE = " ESCAPE '\\'";

    private LanceStringPatternSql() {}

    /**
     * SQL predicate for a Lucene wildcard pattern on {@code column}:
     * {@code column LIKE '<pattern>' ESCAPE '\'} ({@code ILIKE} when
     * {@code caseInsensitive}).
     */
    public static String wildcard(String column, String wildcard, boolean caseInsensitive) {
        String operator = caseInsensitive ? " ILIKE " : " LIKE ";
        return column + operator + stringLiteral(likePattern(wildcard)) + LIKE_ESCAPE_CLAUSE;
    }

    /**
     * Translate a Lucene wildcard pattern into a SQL {@code LIKE}
     * pattern whose escape character is {@code \}. {@code *} becomes
     * {@code %}, {@code ?} becomes {@code _}, a literal {@code %},
     * {@code _} or {@code \} is prefixed with {@code \}, and a Lucene
     * escape {@code \x} yields the literal {@code x} (escaped again
     * when {@code x} is one of the three). A trailing lone backslash
     * is a literal backslash, as in Lucene's lenient parsing.
     */
    static String likePattern(String wildcard) {
        StringBuilder sb = new StringBuilder(wildcard.length() + 8);
        for (int i = 0; i < wildcard.length(); i++) {
            char c = wildcard.charAt(i);
            switch (c) {
                case '*' -> sb.append('%');
                case '?' -> sb.append('_');
                case '\\' -> {
                    if (i + 1 < wildcard.length()) {
                        i++;
                        appendLikeLiteral(sb, wildcard.charAt(i));
                    } else {
                        appendLikeLiteral(sb, '\\');
                    }
                }
                default -> appendLikeLiteral(sb, c);
            }
        }
        return sb.toString();
    }

    private static void appendLikeLiteral(StringBuilder sb, char c) {
        if (c == '%' || c == '_' || c == '\\') {
            sb.append('\\');
        }
        sb.append(c);
    }

    /**
     * SQL predicate for a Lucene regexp on {@code column}:
     * {@code regexp_like(column, '^(?:<pattern>)$')}, with a leading
     * {@code (?i)} when {@code matchFlags} asks for case insensitive
     * matching.
     *
     * @param syntaxFlags the Lucene {@link RegExp} syntax flags of the
     *     query ({@code RegExp.ALL} by default); an operator is only
     *     rejected when its flag is set, because with the flag off
     *     Lucene reads the character literally just like Rust does
     * @param matchFlags the Lucene match flags; {@code CASE_INSENSITIVE}
     *     and {@code ASCII_CASE_INSENSITIVE} both map to {@code (?i)}
     * @throws IllegalArgumentException when the pattern uses an
     *     operator only Lucene's grammar has
     */
    public static String regexp(String column, String pattern, int syntaxFlags, int matchFlags) {
        rejectLuceneOnlyOperators(column, pattern, syntaxFlags);
        boolean caseInsensitive = (matchFlags & (RegExp.CASE_INSENSITIVE | RegExp.ASCII_CASE_INSENSITIVE)) != 0;
        String anchored = (caseInsensitive ? "(?i)" : "") + "^(?:" + pattern + ")$";
        return "regexp_like(" + column + ", " + stringLiteral(anchored) + ")";
    }

    /**
     * Walk the pattern the way Lucene's {@link RegExp} parser does
     * (a {@code \} escapes the next character, {@code [...]} is a
     * character class whose content is literal) and refuse the
     * operators only Lucene has. Each is refused only when the
     * corresponding syntax flag is on; {@code "..."} is refused
     * unconditionally because Lucene always reads it as a quoted
     * literal.
     */
    private static void rejectLuceneOnlyOperators(String column, String pattern, int syntaxFlags) {
        boolean inClass = false;
        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            if (c == '\\') {
                i++;
                continue;
            }
            if (inClass) {
                if (c == ']') {
                    inClass = false;
                }
                continue;
            }
            String operator = switch (c) {
                case '[' -> {
                    inClass = true;
                    yield null;
                }
                case '"' -> "\"...\" (quoted literal)";
                case '~' -> (syntaxFlags & RegExp.DEPRECATED_COMPLEMENT) != 0 ? "~ (complement)" : null;
                case '&' -> (syntaxFlags & RegExp.INTERSECTION) != 0 ? "& (intersection)" : null;
                case '<' -> (syntaxFlags & (RegExp.INTERVAL | RegExp.AUTOMATON)) != 0 ? "<n-m> (numeric interval)" : null;
                case '@' -> (syntaxFlags & RegExp.ANYSTRING) != 0 ? "@ (any string)" : null;
                case '#' -> (syntaxFlags & RegExp.EMPTY) != 0 ? "# (empty language)" : null;
                default -> null;
            };
            if (operator != null) {
                throw new IllegalArgumentException(
                    "regexp on ["
                        + column
                        + "] is evaluated by Lance as a Rust regex over the stored string, which has no Lucene operator "
                        + operator
                        + " (position "
                        + i
                        + " of ["
                        + pattern
                        + "]); use character classes, groups, alternation and repetition, or drop the operator's flag"
                );
            }
        }
    }

    /**
     * SQL predicate for a prefix on {@code column}:
     * {@code starts_with(column, 'value')}, or
     * {@code starts_with(lower(column), lower('value'))} when
     * {@code caseInsensitive}.
     */
    public static String prefix(String column, String value, boolean caseInsensitive) {
        if (caseInsensitive) {
            return "starts_with(lower(" + column + "), lower(" + stringLiteral(value) + "))";
        }
        return "starts_with(" + column + ", " + stringLiteral(value) + ")";
    }

    /**
     * Single quoted SQL string literal with the embedded single quotes
     * doubled, the escape DataFusion's parser accepts. Backslashes are
     * left alone: Lance parses filters with a dialect that has no
     * backslash escapes inside string literals.
     */
    public static String stringLiteral(String value) {
        return "'" + value.replace("'", "''") + "'";
    }
}
