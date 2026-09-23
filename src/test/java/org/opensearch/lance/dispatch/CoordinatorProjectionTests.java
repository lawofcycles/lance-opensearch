/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.util.List;

import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.StoredFieldsContext;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.test.OpenSearchTestCase;

/**
 * {@link TransportLanceCoordinatorAction#resolveProjection} copies the
 * per hit projections of a search body onto the wire record and applies
 * the {@code stored_fields: _none_} conflict checks of
 * {@code SearchService.parseSource}.
 */
public class CoordinatorProjectionTests extends OpenSearchTestCase {

    public void testEmptyBodyIsTheEmptyProjection() {
        assertEquals(HitProjection.NONE, TransportLanceCoordinatorAction.resolveProjection(null));
        assertEquals(HitProjection.NONE, TransportLanceCoordinatorAction.resolveProjection(new SearchSourceBuilder()));
        assertTrue(TransportLanceCoordinatorAction.resolveProjection(new SearchSourceBuilder()).isEmpty());
    }

    public void testEveryElementIsCopied() {
        SearchSourceBuilder source = new SearchSourceBuilder().fetchSource(new String[] { "id" }, new String[] { "body" })
            .storedFields(List.of("id"))
            .docValueField("ts", "yyyy")
            .fetchField("ti*")
            .explain(true);
        HitProjection projection = TransportLanceCoordinatorAction.resolveProjection(source);
        assertFalse(projection.isEmpty());
        assertArrayEquals(new String[] { "id" }, projection.fetchSource().includes());
        assertArrayEquals(new String[] { "body" }, projection.fetchSource().excludes());
        assertEquals(List.of("id"), projection.storedFields().fieldNames());
        assertEquals("ts", projection.docValueFields().get(0).field);
        assertEquals("yyyy", projection.docValueFields().get(0).format);
        assertEquals("ti*", projection.fetchFields().get(0).field);
        assertTrue(projection.explain());
    }

    public void testStoredFieldsNoneRefusesARequestedSourceAndFields() {
        SearchSourceBuilder withSource = new SearchSourceBuilder().storedFields(StoredFieldsContext.fromList(List.of("_none_")))
            .fetchSource(new FetchSourceContext(true));
        IllegalArgumentException sourceConflict = expectThrows(
            IllegalArgumentException.class,
            () -> TransportLanceCoordinatorAction.resolveProjection(withSource)
        );
        assertEquals("[stored_fields] cannot be disabled if [_source] is requested", sourceConflict.getMessage());

        SearchSourceBuilder withFields = new SearchSourceBuilder().storedFields(StoredFieldsContext.fromList(List.of("_none_")))
            .fetchField("id");
        IllegalArgumentException fieldsConflict = expectThrows(
            IllegalArgumentException.class,
            () -> TransportLanceCoordinatorAction.resolveProjection(withFields)
        );
        assertEquals("[stored_fields] cannot be disabled when using the [fields] option", fieldsConflict.getMessage());

        // _none_ alone, or next to _source: false, is a hit without _id.
        SearchSourceBuilder alone = new SearchSourceBuilder().storedFields(StoredFieldsContext.fromList(List.of("_none_")))
            .fetchSource(false);
        assertFalse(TransportLanceCoordinatorAction.resolveProjection(alone).storedFields().fetchFields());
    }
}
