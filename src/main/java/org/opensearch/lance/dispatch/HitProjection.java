/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.io.IOException;
import java.util.List;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.search.fetch.StoredFieldsContext;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.search.fetch.subphase.FieldAndFormat;

/**
 * The per hit projections of a search body, as the coordinator copies
 * them from the {@code SearchSourceBuilder} for the fragment executors:
 * {@code _source} ({@link FetchSourceContext}, null when the body has
 * none), {@code stored_fields} ({@link StoredFieldsContext}, null when
 * absent), {@code docvalue_fields} and {@code fields} (each a list of
 * {@link FieldAndFormat}, empty when absent) and {@code explain}. The
 * executor hands them to the stock fetch sub phases over its search
 * context, so a hit carries the same fields, filtered source and
 * explanation the stock search path's fetch phase would attach.
 *
 * @param fetchSource the body's {@code _source} element, null when absent
 * @param storedFields the body's {@code stored_fields} element, null when absent
 * @param docValueFields the body's {@code docvalue_fields}, empty when absent
 * @param fetchFields the body's {@code fields}, empty when absent
 * @param explain whether the body asked for {@code "explain": true}
 */
public record HitProjection(FetchSourceContext fetchSource, StoredFieldsContext storedFields, List<FieldAndFormat> docValueFields, List<
    FieldAndFormat> fetchFields, boolean explain) implements Writeable {

    /** No projection: the full {@code _source} and {@code _id} of every hit, as a body without these elements asks. */
    public static final HitProjection NONE = new HitProjection(null, null, List.of(), List.of(), false);

    public HitProjection {
        docValueFields = docValueFields == null ? List.of() : List.copyOf(docValueFields);
        fetchFields = fetchFields == null ? List.of() : List.copyOf(fetchFields);
    }

    public static HitProjection read(StreamInput in) throws IOException {
        FetchSourceContext fetchSource = in.readOptionalWriteable(FetchSourceContext::new);
        StoredFieldsContext storedFields = in.readOptionalWriteable(StoredFieldsContext::new);
        List<FieldAndFormat> docValueFields = in.readList(FieldAndFormat::new);
        List<FieldAndFormat> fetchFields = in.readList(FieldAndFormat::new);
        boolean explain = in.readBoolean();
        return new HitProjection(fetchSource, storedFields, docValueFields, fetchFields, explain);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(fetchSource);
        out.writeOptionalWriteable(storedFields);
        out.writeList(docValueFields);
        out.writeList(fetchFields);
        out.writeBoolean(explain);
    }

    /** Whether the body asked for anything beyond the default {@code _id} and full {@code _source}. */
    public boolean isEmpty() {
        return fetchSource == null && storedFields == null && docValueFields.isEmpty() && fetchFields.isEmpty() && !explain;
    }
}
