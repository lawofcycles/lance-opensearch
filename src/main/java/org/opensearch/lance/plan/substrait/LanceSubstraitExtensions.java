/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.substrait;

import java.util.LinkedHashMap;
import java.util.Map;

import io.substrait.extension.SimpleExtension;

/**
 * The function declarations the Substrait producers emit references
 * to. The standard catalog carries everything except the few names
 * below, which a small inline extension supplies: {@code date_trunc}
 * (DataFusion's datetime UDF, not a Substrait standard function),
 * {@code is_true} (declared in the standard comparison catalog but
 * without an isthmus mapping from the Calcite operator, so the
 * producers spell it themselves), {@code ilike} (DataFusion's
 * consumer resolves the name to a case insensitive {@code LIKE}) and
 * {@code regexp_like} (DataFusion's regex UDF, which Lance's session
 * registers). Lance's consumers read only the name before the
 * signature colon and ignore the extension URIs, so the first variant
 * of a name is as good as any, and an argument count that differs
 * from the declared one (the {@code ESCAPE} operand of {@code like})
 * is not checked on either side.
 */
final class LanceSubstraitExtensions {

    private static final String LANCE_FUNCTIONS = """
        %YAML 1.2
        ---
        scalar_functions:
          - name: "date_trunc"
            description: >-
              DataFusion's date_trunc: the first instant of the calendar
              unit that contains the timestamp, as a timestamp of the
              same unit.
            impls:
              - args:
                  - name: unit
                    value: string
                  - name: value
                    value: timestamp
                return: timestamp
              - args:
                  - name: unit
                    value: string
                  - name: value
                    value: timestamp_tz
                return: timestamp_tz
          - name: "is_true"
            description: >-
              True when the argument is true, false when it is false or
              null; the two valued collapse a must_not clause needs.
            impls:
              - args:
                  - name: value
                    value: boolean?
                return: boolean
          - name: "ilike"
            description: >-
              Case insensitive LIKE with an escape character, as
              DataFusion's consumer spells it.
            impls:
              - args:
                  - name: input
                    value: string
                  - name: match
                    value: string
                  - name: escape
                    value: string
                return: boolean
          - name: "regexp_like"
            description: >-
              DataFusion's regexp_like: whether the regular expression
              matches anywhere in the input.
            impls:
              - args:
                  - name: input
                    value: string
                  - name: pattern
                    value: string
                return: boolean
        """;

    /** The standard catalog merged with the inline declarations above. */
    static final SimpleExtension.ExtensionCollection COLLECTION = SimpleExtension.loadDefaults()
        .merge(SimpleExtension.load("extension:org.opensearch.lance:functions_lance", LANCE_FUNCTIONS));

    private static final Map<String, SimpleExtension.ScalarFunctionVariant> SCALARS = indexScalars();
    private static final Map<String, SimpleExtension.AggregateFunctionVariant> AGGREGATES = indexAggregates();

    private static Map<String, SimpleExtension.ScalarFunctionVariant> indexScalars() {
        Map<String, SimpleExtension.ScalarFunctionVariant> byName = new LinkedHashMap<>();
        for (SimpleExtension.ScalarFunctionVariant variant : COLLECTION.scalarFunctions()) {
            byName.putIfAbsent(variant.name(), variant);
        }
        return byName;
    }

    private static Map<String, SimpleExtension.AggregateFunctionVariant> indexAggregates() {
        Map<String, SimpleExtension.AggregateFunctionVariant> byName = new LinkedHashMap<>();
        for (SimpleExtension.AggregateFunctionVariant variant : COLLECTION.aggregateFunctions()) {
            byName.putIfAbsent(variant.name(), variant);
        }
        return byName;
    }

    /** The first declared variant of the scalar function {@code name}. */
    static SimpleExtension.ScalarFunctionVariant scalar(String name) {
        SimpleExtension.ScalarFunctionVariant variant = SCALARS.get(name);
        if (variant == null) {
            throw new IllegalArgumentException("no declaration for scalar function " + name);
        }
        return variant;
    }

    /** The first declared variant of the aggregate function {@code name}. */
    static SimpleExtension.AggregateFunctionVariant aggregate(String name) {
        SimpleExtension.AggregateFunctionVariant variant = AGGREGATES.get(name);
        if (variant == null) {
            throw new IllegalArgumentException("no declaration for aggregate function " + name);
        }
        return variant;
    }

    private LanceSubstraitExtensions() {}
}
