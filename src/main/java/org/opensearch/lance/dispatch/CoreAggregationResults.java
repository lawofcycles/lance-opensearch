/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;

import org.opensearch.common.Rounding;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.composite.CompositeKey;
import org.opensearch.search.aggregations.bucket.composite.InternalComposite;
import org.opensearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.opensearch.search.aggregations.bucket.missing.MissingOrder;

/**
 * Builds the aggregation results whose constructors OpenSearch keeps
 * package private: {@link InternalComposite} with its buckets and keys,
 * and {@link InternalDateHistogram} for a nested level. The aggregators
 * build them from inside their package; the pushdown builds the same
 * objects from Lance group rows and has no other way in.
 *
 * <p>The handles are resolved once at class load through
 * {@code getDeclaredConstructor} and {@code setAccessible}, the same
 * way {@link TransportLanceFragmentQueryAction} reaches
 * {@code IndexService#getReaderWrapper()}; the plugin's security policy
 * already grants {@code ReflectPermission "suppressAccessChecks"}. A
 * missing constructor means the plugin was built against an OpenSearch
 * version that changed these classes, and refusing to load is more
 * useful than a wrong answer at request time.
 */
final class CoreAggregationResults {

    private CoreAggregationResults() {}

    private static final String DATE_HISTOGRAM_EMPTY_BUCKET_INFO = InternalDateHistogram.class.getName() + "$EmptyBucketInfo";
    private static final String DATE_HISTOGRAM_BOUNDS = "org.opensearch.search.aggregations.bucket.histogram.LongBounds";

    private static final Constructor<CompositeKey> COMPOSITE_KEY = constructor(CompositeKey.class, Comparable[].class);
    private static final Constructor<InternalComposite.InternalBucket> COMPOSITE_BUCKET = constructor(
        InternalComposite.InternalBucket.class,
        List.class,
        List.class,
        CompositeKey.class,
        int[].class,
        MissingOrder[].class,
        long.class,
        InternalAggregations.class
    );
    private static final Constructor<InternalComposite> COMPOSITE = constructor(
        InternalComposite.class,
        String.class,
        int.class,
        List.class,
        List.class,
        List.class,
        CompositeKey.class,
        int[].class,
        MissingOrder[].class,
        boolean.class,
        Map.class
    );
    private static final Class<?> EMPTY_BUCKET_INFO_CLASS = coreClass(DATE_HISTOGRAM_EMPTY_BUCKET_INFO);
    private static final Constructor<?> DATE_HISTOGRAM_EMPTY_BUCKET_INFO_CONSTRUCTOR = constructor(
        EMPTY_BUCKET_INFO_CLASS,
        Rounding.class,
        InternalAggregations.class,
        coreClass(DATE_HISTOGRAM_BOUNDS)
    );
    private static final Constructor<InternalDateHistogram> DATE_HISTOGRAM = constructor(
        InternalDateHistogram.class,
        String.class,
        List.class,
        BucketOrder.class,
        long.class,
        long.class,
        EMPTY_BUCKET_INFO_CLASS,
        DocValueFormat.class,
        boolean.class,
        Map.class
    );

    /** A composite key over the raw bucket values ({@code BytesRef}, {@code Long} or {@code Double} per source). */
    static CompositeKey compositeKey(Comparable<?>[] values) {
        return newInstance(COMPOSITE_KEY, (Object) values);
    }

    static InternalComposite.InternalBucket compositeBucket(
        List<String> sourceNames,
        List<DocValueFormat> formats,
        CompositeKey key,
        int[] reverseMuls,
        MissingOrder[] missingOrders,
        long docCount,
        InternalAggregations aggregations
    ) {
        return newInstance(COMPOSITE_BUCKET, sourceNames, formats, key, reverseMuls, missingOrders, docCount, aggregations);
    }

    static InternalComposite composite(
        String name,
        int size,
        List<String> sourceNames,
        List<DocValueFormat> formats,
        List<InternalComposite.InternalBucket> buckets,
        CompositeKey afterKey,
        int[] reverseMuls,
        MissingOrder[] missingOrders,
        Map<String, Object> metadata
    ) {
        // earlyTerminated false: the executor never skips documents
        // through a sorted docs producer.
        return newInstance(COMPOSITE, name, size, sourceNames, formats, buckets, afterKey, reverseMuls, missingOrders, false, metadata);
    }

    /**
     * A date histogram result as the aggregator builds it.
     * {@code emptySubAggregations} is only read when {@code minDocCount}
     * is 0: the reduce fills the empty buckets with it.
     */
    static InternalDateHistogram dateHistogram(
        String name,
        List<InternalDateHistogram.Bucket> buckets,
        BucketOrder order,
        long minDocCount,
        Rounding rounding,
        InternalAggregations emptySubAggregations,
        DocValueFormat format,
        boolean keyed,
        Map<String, Object> metadata
    ) {
        Object emptyBucketInfo = null;
        if (minDocCount == 0L) {
            // No extended bounds: the pushdown refuses date histograms with bounds.
            emptyBucketInfo = newInstance(DATE_HISTOGRAM_EMPTY_BUCKET_INFO_CONSTRUCTOR, rounding, emptySubAggregations, null);
        }
        return newInstance(DATE_HISTOGRAM, name, buckets, order, minDocCount, rounding.offset(), emptyBucketInfo, format, keyed, metadata);
    }

    private static Class<?> coreClass(String name) {
        try {
            return Class.forName(name, false, InternalDateHistogram.class.getClassLoader());
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(
                name + " is not available on this OpenSearch build; the aggregation pushdown cannot build its results",
                e
            );
        }
    }

    @SuppressForbidden(reason = "the composite and date histogram result constructors are package private in core; "
        + "the pushdown builds the same results the aggregators build and has no public way to construct them")
    private static <T> Constructor<T> constructor(Class<T> type, Class<?>... parameterTypes) {
        try {
            Constructor<T> constructor = type.getDeclaredConstructor(parameterTypes);
            constructor.setAccessible(true);
            return constructor;
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException(
                type.getName()
                    + " has no constructor with the expected parameters on this OpenSearch build; the aggregation pushdown cannot build its results",
                e
            );
        }
    }

    private static <T> T newInstance(Constructor<T> constructor, Object... arguments) {
        try {
            return constructor.newInstance(arguments);
        } catch (InstantiationException | IllegalAccessException e) {
            throw new IllegalStateException("cannot invoke " + constructor, e);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new IllegalStateException(constructor + " failed", cause);
        }
    }
}
