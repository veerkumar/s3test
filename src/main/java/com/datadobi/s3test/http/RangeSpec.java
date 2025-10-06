/*
 *
 *  Copyright 2025 Datadobi
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software

 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package com.datadobi.s3test.http;

import javax.annotation.Nullable;

public record RangeSpec(@Nullable Long start, @Nullable Long end) {
    public RangeSpec {
        if (start == null && end == null) {
            throw new IllegalArgumentException("Both start and end cannot be null");
        }
    }

    @Override
    public String toString() {
        StringBuilder header = new StringBuilder();
        if (start != null) {
            header.append(start);
        }
        header.append('-');
        if (end != null) {
            header.append(end);
        }
        return header.toString();
    }
}
