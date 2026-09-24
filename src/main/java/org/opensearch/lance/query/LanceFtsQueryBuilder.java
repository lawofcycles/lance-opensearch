/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.query;

import org.lance.ipc.FullTextQuery;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * A DSL query builder that translates into a Lance {@link FullTextQuery}
 * tree. Lets {@code lance_fts_boost} and {@code lance_fts_bool} compose
 * sub-clauses at the FullTextQuery level rather than the Lucene Query
 * level. Lance's native BM25 scoring can compose across FTS variants,
 * but only when the entire tree is handed to it as one FullTextQuery;
 * wrapping each leaf in a separate Lucene Query would lose that
 * cross-clause scoring.
 *
 * <p>Every DSL query in this package that ultimately maps onto
 * {@code FullTextQuery} implements this so parents (boost / bool) can
 * treat child clauses uniformly.
 */
public interface LanceFtsQueryBuilder {

    /**
     * Validate the referenced fields against the mapping and return the
     * {@link FullTextQuery} to push down to Lance. The clause's own
     * {@code boost} is not part of the tree: at the top level Lucene's
     * {@code BoostQuery} applies it, inside a composing clause
     * {@link #boosted} folds it into the tree.
     *
     * @throws IllegalArgumentException if any referenced field is missing,
     *     mapped as a non-{@code lance_text} type, or dropped from the
     *     underlying Lance table
     */
    FullTextQuery toLanceFullTextQuery(QueryShardContext context);

    /**
     * The field names the clause references, in declaration order and
     * without consulting the mapping: what the query says, not what it
     * resolves to. The query planner reads this to name the columns of
     * a full-text plan node.
     */
    Set<String> referencedFields();

    /**
     * Whether {@code clause}'s {@code boost} can travel inside a Lance
     * tree: Lance scales a match query and the per column matches of a
     * multi match, and nothing else (a phrase, a boolean, a boosting
     * query carry no factor). True for a boost of 1 on any clause.
     */
    static boolean boostRepresentable(QueryBuilder clause) {
        return clause.boost() == AbstractQueryBuilder.DEFAULT_BOOST
            || clause instanceof LanceMatchQueryBuilder
            || clause instanceof LanceMultiMatchQueryBuilder;
    }

    /**
     * {@code query} with {@code boost} folded into its Lance factor:
     * a match query's boost is multiplied, a multi match's per column
     * boosts are multiplied (defaulting to 1 per column), and any other
     * tree is refused, because Lance has no factor for it and dropping
     * the boost would silently change the ranking. A boost of 1 returns
     * {@code query} unchanged.
     *
     * @param owner the composing DSL name for the refusal message
     */
    static FullTextQuery boosted(FullTextQuery query, float boost, String owner) {
        if (boost == AbstractQueryBuilder.DEFAULT_BOOST) {
            return query;
        }
        if (query instanceof FullTextQuery.MatchQuery match) {
            return FullTextQuery.match(
                match.getQueryText(),
                match.getColumn(),
                match.getBoost() * boost,
                match.getFuzziness(),
                match.getMaxExpansions(),
                match.getOperator(),
                match.getPrefixLength()
            );
        }
        if (query instanceof FullTextQuery.MultiMatchQuery multi) {
            List<Float> boosts = new ArrayList<>(multi.getColumns().size());
            for (int i = 0; i < multi.getColumns().size(); i++) {
                float column = multi.getBoosts().isPresent() ? multi.getBoosts().get().get(i) : 1f;
                boosts.add(column * boost);
            }
            return FullTextQuery.multiMatch(multi.getQueryText(), multi.getColumns(), boosts, multi.getOperator());
        }
        throw new IllegalArgumentException(
            "["
                + owner
                + "] boost ["
                + boost
                + "] on a ["
                + query.getType()
                + "] clause is not supported: Lance scales match and multi_match clauses only;"
                + " move the boost to a lance_match clause or drop it"
        );
    }
}
