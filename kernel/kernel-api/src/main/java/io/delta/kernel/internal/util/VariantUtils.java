/*
 * Copyright (2024) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.kernel.internal.util;

import java.util.Arrays;
import java.util.List;

import io.delta.kernel.client.ExpressionHandler;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.expressions.*;
import io.delta.kernel.types.*;

import io.delta.kernel.internal.ExtractedVariantOptions;

/**
 * Doc comment
 */
public class VariantUtils {

    /**
     * Doc comment
     */
    public static ColumnarBatch withExtractedVariantFields(
            ExpressionHandler expressionHandler,
            ColumnarBatch dataBatch,
            List<ExtractedVariantOptions> extractedVariantFields) {
        for (ExtractedVariantOptions opts : extractedVariantFields) {
            // TODO: how does this work with column mapping? We're searching for the fieldName in
            // the delta schema
            String varColName = opts.path.getNames()[0];
            int colIdx = dataBatch.getSchema().indexOf(varColName);
            if (colIdx == -1) {
                System.out.println("TOP LEVEL VARIANT IS NOT FOUND IN SCHEMA");
                assert false;
            }

            ExpressionEvaluator evaluator = expressionHandler.getEvaluator(
                getVariantGetExprSchema(varColName),
                new ScalarExpression(
                    "variant_get",
                    Arrays.asList(
                        new Column(varColName),
                        Literal.ofString(String.join(".", opts.path.getNames())),
                        // TODO: Does "toString" work on more complex types
                        Literal.ofString(opts.type.toString()))
                ),
                opts.type
            );

            dataBatch = dataBatch.withNewColumn(
                dataBatch.getSchema().length(),
                // TODO: set this to the right datatype.
                new StructField(opts.fieldName, StringType.STRING, true),
                // TODO: we don't have to pass in the whole data batch, we only need the
                // variant column.
                extractVariantField(evaluator, dataBatch, opts)
            );
        }

        return dataBatch;
    }

    private static ColumnVector extractVariantField(
            ExpressionEvaluator evaluator,
            ColumnarBatch batchWithVariantCol,
            ExtractedVariantOptions options) {
        return evaluator.eval(batchWithVariantCol);
    }

    private static StructType getVariantGetExprSchema(String variantColName) {
        return new StructType()
            .add(new StructField(variantColName, VariantType.VARIANT, true))
            .add(new StructField("col_1", StringType.STRING, false))
            .add(new StructField("col_2", StringType.STRING, false));
    }
}
