/*
 * Copyright (2023) The Delta Lake Project Authors.
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

package io.delta.kernel.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import io.delta.kernel.Scan;
import io.delta.kernel.ScanBuilder;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.*;

import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.replay.LogReplay;

/**
 * Implementation of {@link ScanBuilder}.
 */
public class ScanBuilderImpl
    implements ScanBuilder {

    private final Path dataPath;
    private final Protocol protocol;
    private final Metadata metadata;
    private final StructType snapshotSchema;
    private final LogReplay logReplay;
    private final TableClient tableClient;

    private StructType readSchema;
    private Optional<Predicate> predicate;
    private List<ExtractedVariantOptions> extractedVariantFields;

    public ScanBuilderImpl(
            Path dataPath,
            Protocol protocol,
            Metadata metadata,
            StructType snapshotSchema,
            LogReplay logReplay,
            TableClient tableClient) {
        this.dataPath = dataPath;
        this.protocol = protocol;
        this.metadata = metadata;
        this.snapshotSchema = snapshotSchema;
        this.logReplay = logReplay;
        this.tableClient = tableClient;
        this.readSchema = snapshotSchema;
        this.predicate = Optional.empty();
        this.extractedVariantFields = new ArrayList();
    }

    @Override
    public ScanBuilder withFilter(TableClient tableClient, Predicate predicate) {
        if (this.predicate.isPresent()) {
            throw new IllegalArgumentException("There already exists a filter in current builder");
        }
        this.predicate = Optional.of(predicate);
        return this;
    }

    @Override
    public ScanBuilder withReadSchema(TableClient tableClient, StructType readSchema) {
        // TODO: validate the readSchema is a subset of the table schema
        this.readSchema = readSchema;
        return this;
    }

    @Override
    public ScanBuilder withExtractedVariantField(
            TableClient tableClient,
            String path,
            DataType type,
            String extractedFieldName) {
        String[] splitPath = splitVariantPath(path);
        extractedVariantFields.add(new ExtractedVariantOptions(
            new Column(splitPath), type, extractedFieldName));

        // TODO: were attaching the actual variant column name right now.
        // Will this work with column mapping/is there a more robust way?
        if (readSchema.indexOf(splitPath[0]) == -1) {
            readSchema = readSchema.add(StructField.internallyAddedVariantSchema(splitPath[0]));
        }

        return this;
    }

    @Override
    public Scan build() {
        return new ScanImpl(
            snapshotSchema,
            readSchema,
            protocol,
            metadata,
            logReplay,
            predicate,
            extractedVariantFields,
            dataPath);
    }

    private String[] splitVariantPath(String path) {
        // TODO: account for square brackets and array indices later.
        return path.split("\\.");
    }
}
