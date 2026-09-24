/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.translate;

import org.lance.ipc.FullTextQuery;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.DisMaxQueryBuilder;
import org.opensearch.index.query.MatchPhraseQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.MultiMatchQueryBuilder;
import org.opensearch.index.query.Operator;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.lance.mapper.LanceTextFieldMapper;
import org.opensearch.lance.query.LanceMatchPhraseQueryBuilder;
import org.opensearch.lance.query.LanceMatchQueryBuilder;
import org.opensearch.lance.query.LanceMultiMatchQueryBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Rewrites OpenSearch's stock full text clauses on {@code lance_text}
 * fields into the plugin's Lance FTS clauses, so one request body in
 * stock syntax plans and executes exactly as the same body written with
 * the {@code lance_*} queries. The coordinator applies it to the top
 * level query of every target before planning, once per Lance backed
 * index (the mapping decides per index which fields are
 * {@code lance_text}), and ships the rewritten query to the executors;
 * the explain endpoint applies the same rewrite so it plans what the
 * coordinator plans.
 *
 * <p>The rewrite is a mapping of parameters, not a change of meaning:
 * <ul>
 *   <li>{@code match} becomes {@code lance_match} with the same text,
 *       {@code operator}, {@code prefix_length}, {@code max_expansions},
 *       {@code boost} and {@code _name}; {@code fuzziness} becomes the
 *       edit distance {@link LanceTextFieldMapper.LanceTextFieldType#fuzzyDistance}
 *       derives ({@code AUTO} on the length of the whole text).</li>
 *   <li>{@code match_phrase} becomes {@code lance_match_phrase} with the
 *       same text and {@code slop}.</li>
 *   <li>{@code multi_match} of type {@code best_fields} (the default)
 *       with the default tie breaker becomes {@code lance_multi_match}
 *       with the same fields, per field boosts and {@code operator}:
 *       Lance's multi match takes the best column's score, as
 *       {@code best_fields} does. The other types ({@code most_fields}
 *       sums, {@code cross_fields} blends, the phrase and prefix types
 *       need the token stream) keep their stock form.</li>
 *   <li>{@code bool} and {@code dis_max} are rebuilt around their
 *       rewritten clauses with every other parameter kept.</li>
 * </ul>
 * A clause the rewrite leaves alone runs through the field type as
 * before: a stock clause on a field that is not {@code lance_text}, a
 * clause with an explicit {@code analyzer} (the caller asked for
 * OpenSearch side analysis), a {@code fuzziness} on {@code multi_match}
 * (the Lance multi match has none), an empty query text, or any clause
 * inside a compound the rewrite does not descend into
 * ({@code function_score}, {@code nested}, {@code constant_score},
 * ...). Parameters without a Lance counterpart
 * ({@code minimum_should_match}, {@code zero_terms_query},
 * {@code lenient}, {@code fuzzy_transpositions}, {@code fuzzy_rewrite},
 * {@code auto_generate_synonyms_phrase_query}) are dropped by the
 * rewrite exactly as the field type path ignored them, because both
 * paths hand Lance the whole text as one query.
 */
public final class StockTextQueryRewriter {

    private StockTextQueryRewriter() {}

    /**
     * {@code query} with every stock full text clause on one of
     * {@code lanceTextFields} rewritten; the same instance when nothing
     * was rewritten (a null query stays null).
     *
     * @param lanceTextFields the fields the target's mapping types as
     *     {@code lance_text}
     */
    public static QueryBuilder rewrite(QueryBuilder query, Set<String> lanceTextFields) {
        if (query == null || lanceTextFields.isEmpty()) {
            return query;
        }
        if (query instanceof MatchQueryBuilder match) {
            return rewriteMatch(match, lanceTextFields);
        }
        if (query instanceof MatchPhraseQueryBuilder phrase) {
            return rewriteMatchPhrase(phrase, lanceTextFields);
        }
        if (query instanceof MultiMatchQueryBuilder multi) {
            return rewriteMultiMatch(multi, lanceTextFields);
        }
        if (query instanceof BoolQueryBuilder bool) {
            return rewriteBool(bool, lanceTextFields);
        }
        if (query instanceof DisMaxQueryBuilder disMax) {
            return rewriteDisMax(disMax, lanceTextFields);
        }
        return query;
    }

    private static QueryBuilder rewriteMatch(MatchQueryBuilder match, Set<String> lanceTextFields) {
        if (!lanceTextFields.contains(match.fieldName()) || match.analyzer() != null) {
            return match;
        }
        String text = textOf(match.value());
        if (text.isEmpty()) {
            return match;
        }
        LanceMatchQueryBuilder rewritten = new LanceMatchQueryBuilder(match.fieldName(), text).operator(operatorOf(match.operator()))
            .prefixLength(match.prefixLength())
            .maxExpansions(match.maxExpansions());
        if (match.fuzziness() != null) {
            rewritten.fuzziness(LanceTextFieldMapper.LanceTextFieldType.fuzzyDistance(match.fuzziness(), text));
        }
        return withEnvelope(rewritten, match);
    }

    private static QueryBuilder rewriteMatchPhrase(MatchPhraseQueryBuilder phrase, Set<String> lanceTextFields) {
        if (!lanceTextFields.contains(phrase.fieldName()) || phrase.analyzer() != null) {
            return phrase;
        }
        String text = textOf(phrase.value());
        if (text.isEmpty()) {
            return phrase;
        }
        return withEnvelope(new LanceMatchPhraseQueryBuilder(phrase.fieldName(), text).slop(phrase.slop()), phrase);
    }

    private static QueryBuilder rewriteMultiMatch(MultiMatchQueryBuilder multi, Set<String> lanceTextFields) {
        if (multi.type() != MultiMatchQueryBuilder.Type.BEST_FIELDS
            || multi.analyzer() != null
            || multi.fuzziness() != null
            || multi.fields().isEmpty()
            || (multi.tieBreaker() != null && multi.tieBreaker() != MultiMatchQueryBuilder.Type.BEST_FIELDS.tieBreaker())) {
            return multi;
        }
        String text = textOf(multi.value());
        if (text.isEmpty()) {
            return multi;
        }
        List<String> fields = new ArrayList<>(multi.fields().size());
        List<Float> boosts = new ArrayList<>(multi.fields().size());
        boolean anyBoost = false;
        for (Map.Entry<String, Float> field : multi.fields().entrySet()) {
            // A wildcard pattern or a field outside the mapping keeps the
            // stock expansion; the Lance multi match names columns.
            if (!lanceTextFields.contains(field.getKey())) {
                return multi;
            }
            fields.add(field.getKey());
            float boost = field.getValue() == null ? 1f : field.getValue();
            anyBoost |= boost != 1f;
            boosts.add(boost);
        }
        LanceMultiMatchQueryBuilder rewritten = new LanceMultiMatchQueryBuilder(fields, text).operator(operatorOf(multi.operator()));
        if (anyBoost) {
            rewritten.boosts(boosts);
        }
        return withEnvelope(rewritten, multi);
    }

    private static QueryBuilder rewriteBool(BoolQueryBuilder bool, Set<String> lanceTextFields) {
        List<QueryBuilder> must = rewriteAll(bool.must(), lanceTextFields);
        List<QueryBuilder> should = rewriteAll(bool.should(), lanceTextFields);
        List<QueryBuilder> filter = rewriteAll(bool.filter(), lanceTextFields);
        List<QueryBuilder> mustNot = rewriteAll(bool.mustNot(), lanceTextFields);
        if (must == null && should == null && filter == null && mustNot == null) {
            return bool;
        }
        BoolQueryBuilder rewritten = new BoolQueryBuilder();
        for (QueryBuilder clause : must == null ? bool.must() : must) {
            rewritten.must(clause);
        }
        for (QueryBuilder clause : should == null ? bool.should() : should) {
            rewritten.should(clause);
        }
        for (QueryBuilder clause : filter == null ? bool.filter() : filter) {
            rewritten.filter(clause);
        }
        for (QueryBuilder clause : mustNot == null ? bool.mustNot() : mustNot) {
            rewritten.mustNot(clause);
        }
        rewritten.minimumShouldMatch(bool.minimumShouldMatch());
        rewritten.adjustPureNegative(bool.adjustPureNegative());
        return withEnvelope(rewritten, bool);
    }

    private static QueryBuilder rewriteDisMax(DisMaxQueryBuilder disMax, Set<String> lanceTextFields) {
        List<QueryBuilder> inner = rewriteAll(disMax.innerQueries(), lanceTextFields);
        if (inner == null) {
            return disMax;
        }
        DisMaxQueryBuilder rewritten = new DisMaxQueryBuilder().tieBreaker(disMax.tieBreaker());
        for (QueryBuilder clause : inner) {
            rewritten.add(clause);
        }
        return withEnvelope(rewritten, disMax);
    }

    /** The rewritten list, or null when no clause changed. */
    private static List<QueryBuilder> rewriteAll(List<QueryBuilder> clauses, Set<String> lanceTextFields) {
        List<QueryBuilder> rewritten = null;
        for (int i = 0; i < clauses.size(); i++) {
            QueryBuilder clause = clauses.get(i);
            QueryBuilder replacement = rewrite(clause, lanceTextFields);
            if (replacement != clause && rewritten == null) {
                rewritten = new ArrayList<>(clauses.subList(0, i));
            }
            if (rewritten != null) {
                rewritten.add(replacement);
            }
        }
        return rewritten;
    }

    private static <T extends QueryBuilder> T withEnvelope(T rewritten, QueryBuilder original) {
        rewritten.boost(original.boost());
        rewritten.queryName(original.queryName());
        return rewritten;
    }

    private static String textOf(Object value) {
        return value == null ? "" : value.toString();
    }

    private static FullTextQuery.Operator operatorOf(Operator operator) {
        return operator == Operator.AND ? FullTextQuery.Operator.AND : FullTextQuery.Operator.OR;
    }
}
