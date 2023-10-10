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
package io.delta.kernel.client;

import io.delta.kernel.annotation.Evolving;

/**
 * Represents a request to read a range of bytes from a given file.
 */
@Evolving
public interface FileReadRequest {
    /**
     * Get the fully qualified path of the file from which to read the data.
     */
    String getPath();

    /**
     * Get the start offset in the file from where to start reading the data.
     */
    int getStartOffset();

    /**
     * Get the length of the data to read from the file starting at the <i>startOffset</i>.
     */
    int getReadLength();
}
