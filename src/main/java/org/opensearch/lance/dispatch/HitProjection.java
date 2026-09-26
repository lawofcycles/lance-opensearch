/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.index.mapper.SourceFieldMapper;
import org.opensearch.lance.engine.LanceFragmentSchema;
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

    /**
     * The columns the fragment reader's row take has to project so the
     * stock fetch phase renders this projection, following the rules
     * {@code FetchPhase.createStoredFieldsVisitor} applies to a body.
     *
     * <p>{@code stored_fields: _none_} reads no stored field, so no
     * column is taken. Otherwise {@code _id} is always rendered. The
     * {@code _source} bytes are loaded when the body asks for
     * {@code _source} (no {@code _source} element and no
     * {@code stored_fields} list, {@code _source: true}, an includes or
     * excludes filter, or {@code _source} named in {@code stored_fields})
     * or when it carries {@code fields}, which reads the unfiltered
     * source. The filter narrows the take to the columns it keeps, and
     * the {@code fields} patterns add theirs; {@code docvalue_fields}
     * add none, because the doc values phase reads the fragment
     * reader's doc values and never the source.
     */
    public LanceFragmentSchema.TakeProjection takeProjection(LanceFragmentSchema schema) {
        if (storedFields != null && storedFields.fetchFields() == false) {
            return schema.takeProjection(false, List.of(), List.of(), List.of());
        }
        FetchSourceContext source = fetchSource;
        if (storedFields != null && storedFields.fieldNames() != null && storedFields.fieldNames().contains(SourceFieldMapper.NAME)) {
            FetchSourceContext named = source == null ? FetchSourceContext.FETCH_SOURCE : source;
            source = new FetchSourceContext(true, named.includes(), named.excludes());
        } else if (source == null && storedFields == null) {
            source = FetchSourceContext.FETCH_SOURCE;
        }
        List<String> fieldPatterns = new ArrayList<>(fetchFields.size());
        for (FieldAndFormat field : fetchFields) {
            fieldPatterns.add(field.field);
        }
        boolean renderSource = source != null && source.fetchSource();
        List<String> includes = renderSource && source.includes().length > 0 ? List.of(source.includes()) : renderSource ? null : List.of();
        List<String> excludes = renderSource ? List.of(source.excludes()) : List.of();
        return schema.takeProjection(true, includes, excludes, fieldPatterns);
    }
}
