/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.query;

import org.lance.ipc.FullTextQuery;
import org.opensearch.index.query.QueryShardContext;

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
     * {@link FullTextQuery} to push down to Lance.
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
}
