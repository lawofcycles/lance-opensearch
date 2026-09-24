/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.opensearch.common.document.DocumentField;
import org.opensearch.index.fieldvisitor.FieldsVisitor;
import org.opensearch.search.SearchHit;
import org.opensearch.search.fetch.FetchContext;
import org.opensearch.search.fetch.FetchPhase;
import org.opensearch.search.fetch.FetchSubPhase;
import org.opensearch.search.fetch.FetchSubPhaseProcessor;
import org.opensearch.search.fetch.subphase.ExplainPhase;
import org.opensearch.search.fetch.subphase.FetchDocValuesPhase;
import org.opensearch.search.fetch.subphase.FetchFieldsPhase;
import org.opensearch.search.fetch.subphase.FetchSourcePhase;
import org.opensearch.search.lookup.SearchLookup;

/**
 * The fetch phase of the fragment executor: renders the {@link SearchHit}
 * of every doc of a page through OpenSearch's stored fields visitor and
 * the stock fetch sub phases, so a hit carries what the stock search path's
 * fetch phase attaches for the same body. The sub phases are the ones
 * {@code SearchModule} registers for the elements the executor serves,
 * in the module's order: {@link ExplainPhase} ({@code explain}),
 * {@link FetchDocValuesPhase} ({@code docvalue_fields}),
 * {@link FetchSourcePhase} ({@code _source}, its {@code includes} and
 * {@code excludes} over the leaf reader's synthesised source) and
 * {@link FetchFieldsPhase} ({@code fields}). {@code stored_fields} is
 * the stored fields visitor itself, which {@link FetchPhase} builds from
 * the context ({@code _none_} yields no visitor and a hit without
 * {@code _id}, a named list yields the fields the leaf reader stores,
 * that is {@code _id} and {@code _source}).
 *
 * <p>Extends {@link FetchPhase} for {@code createStoredFieldsVisitor}
 * only. {@code FetchPhase.execute} itself is not driven: it builds a
 * {@link FetchContext} whose {@code getIndexName} reads
 * {@code SearchContext.indexShard().shardId()}, and the fragment
 * executor runs on nodes without a shard copy, so
 * {@link LanceFragmentSearchContext#indexShard()} is null. The loop
 * below is the non nested branch of {@code FetchPhase.execute} with a
 * {@link FetchContext} that names the index from the shard target: docs
 * visited in doc id order, one {@code setNextReader} per leaf, the
 * stored fields loaded through the leaf reader (the fragment leaf
 * reader synthesises {@code _id} and {@code _source} from the Lance row
 * the caller prefetched), the source handed to the hit's
 * {@code SourceLookup}, then every processor run over the hit. Hits are
 * root docs by construction (the executor confines the query to parent
 * docs and the Lance scorers map row addresses to parent doc ids), so
 * the nested hit branch is not needed.
 *
 * <p>The hits come back in the page's order with the top level doc id
 * as {@link SearchHit#docId()}; the caller sets the score and the sort
 * values, as {@code SearchPhaseController} does on the stock search path.
 */
final class FragmentFetchPhase extends FetchPhase {

    private final List<FetchSubPhase> subPhases;

    FragmentFetchPhase() {
        this(List.of(new ExplainPhase(), new FetchDocValuesPhase(), new FetchSourcePhase(), new FetchFieldsPhase()));
    }

    private FragmentFetchPhase(List<FetchSubPhase> subPhases) {
        super(subPhases);
        this.subPhases = List.copyOf(subPhases);
    }

    /**
     * Render the hits of {@code docIds} (top level doc ids of
     * {@code context.searcher()}'s reader, in page order).
     *
     * @return one hit per doc id, in the same order
     */
    SearchHit[] fetch(LanceFragmentSearchContext context, int[] docIds) throws IOException {
        SearchHit[] hits = new SearchHit[docIds.length];
        if (docIds.length == 0) {
            return hits;
        }
        Integer[] order = new Integer[docIds.length];
        for (int i = 0; i < order.length; i++) {
            order[i] = i;
        }
        // Doc id order: one setNextReader per leaf, and the leaf's
        // stored fields are read in the order the rows were fetched.
        Arrays.sort(order, (a, b) -> Integer.compare(docIds[a], docIds[b]));

        Map<String, Set<String>> storedToRequestedFields = new HashMap<>();
        FieldsVisitor fieldsVisitor = createStoredFieldsVisitor(context, storedToRequestedFields);
        String indexName = context.shardTarget().getIndex();
        FetchContext fetchContext = new FetchContext(context) {
            @Override
            public String getIndexName() {
                return indexName;
            }
        };
        List<FetchSubPhaseProcessor> processors = new ArrayList<>(subPhases.size());
        for (FetchSubPhase subPhase : subPhases) {
            FetchSubPhaseProcessor processor = subPhase.getProcessor(fetchContext);
            if (processor != null) {
                processors.add(processor);
            }
        }
        SearchLookup lookup = fetchContext.searchLookup();
        List<LeafReaderContext> leaves = context.searcher().getIndexReader().leaves();
        int currentLeaf = -1;
        LeafReaderContext leaf = null;
        for (int index : order) {
            context.cancellation().checkCancelled();
            int docId = docIds[index];
            int leafIndex = ReaderUtil.subIndex(docId, leaves);
            if (leafIndex != currentLeaf) {
                leaf = leaves.get(leafIndex);
                currentLeaf = leafIndex;
                for (FetchSubPhaseProcessor processor : processors) {
                    processor.setNextReader(leaf);
                }
            }
            FetchSubPhase.HitContext hit = prepareHitContext(context, lookup, fieldsVisitor, docId, storedToRequestedFields, leaf);
            for (FetchSubPhaseProcessor processor : processors) {
                processor.process(hit);
            }
            hits[index] = hit.hit();
        }
        return hits;
    }

    /**
     * The hit of one root doc before the sub phases run: the stored
     * fields the visitor asks for are read from the leaf ({@code _id},
     * {@code _source} when the visitor loads it, both synthesised by the
     * fragment leaf reader), the hit is created with the requested
     * stored fields split into document and metadata fields, and the
     * source bytes are handed to the hit's {@code SourceLookup} for the
     * source, doc value and fields sub phases. A null visitor
     * ({@code stored_fields: _none_}) yields a hit without {@code _id}
     * and without stored fields, as on the stock search path.
     */
    private static FetchSubPhase.HitContext prepareHitContext(
        LanceFragmentSearchContext context,
        SearchLookup lookup,
        FieldsVisitor fieldsVisitor,
        int docId,
        Map<String, Set<String>> storedToRequestedFields,
        LeafReaderContext leaf
    ) throws IOException {
        int subDocId = docId - leaf.docBase;
        if (fieldsVisitor == null) {
            SearchHit hit = new SearchHit(docId, null, null, null);
            return new FetchSubPhase.HitContext(hit, leaf, subDocId, lookup.source());
        }
        fieldsVisitor.reset();
        leaf.reader().storedFields().document(subDocId, fieldsVisitor);
        fieldsVisitor.postProcess(context::fieldType);
        SearchHit hit;
        if (fieldsVisitor.fields().isEmpty()) {
            hit = new SearchHit(docId, fieldsVisitor.id(), Map.of(), Map.of());
        } else {
            Map<String, DocumentField> docFields = new HashMap<>();
            Map<String, DocumentField> metaFields = new HashMap<>();
            fillDocAndMetaFields(context, fieldsVisitor, storedToRequestedFields, docFields, metaFields);
            hit = new SearchHit(docId, fieldsVisitor.id(), docFields, metaFields);
        }
        FetchSubPhase.HitContext hitContext = new FetchSubPhase.HitContext(hit, leaf, subDocId, lookup.source());
        if (fieldsVisitor.source() != null) {
            hitContext.sourceLookup().setSource(fieldsVisitor.source());
        }
        return hitContext;
    }

    /**
     * Split the stored fields the visitor collected into the hit's
     * document fields and metadata fields, under the names the request
     * asked for ({@code storedToRequestedFields} maps a stored field to
     * the request's names or patterns that selected it). Same rule as
     * {@code FetchPhase.fillDocAndMetaFields}, which is private there.
     */
    private static void fillDocAndMetaFields(
        LanceFragmentSearchContext context,
        FieldsVisitor fieldsVisitor,
        Map<String, Set<String>> storedToRequestedFields,
        Map<String, DocumentField> docFields,
        Map<String, DocumentField> metaFields
    ) {
        for (Map.Entry<String, List<Object>> entry : fieldsVisitor.fields().entrySet()) {
            String storedField = entry.getKey();
            List<Object> storedValues = entry.getValue();
            Set<String> requested = storedToRequestedFields.getOrDefault(storedField, new HashSet<>(List.of(storedField)));
            for (String requestedField : requested) {
                Map<String, DocumentField> target = context.mapperService().isMetadataField(requestedField) ? metaFields : docFields;
                target.put(requestedField, new DocumentField(requestedField, storedValues));
            }
        }
    }
}
