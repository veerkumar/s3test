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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public record ContentRange(String unit, @Nullable Long start, @Nullable Long end, @Nullable Long size) {
    public ContentRange(String unit, @Nullable Long start, @Nullable Long end, @Nullable Long size) {
        this.unit = unit;
        if (start == null && size == null) {
            throw new IllegalArgumentException("start and size cannot both be null");
        }

        this.start = start;
        this.end = end;
        this.size = size;
    }

    // Content-Range       = range-unit SP
    //     ( range-resp / unsatisfied-range )
    //
    // range-resp          = incl-range "/" ( complete-length / "*" )
    // incl-range          = first-pos "-" last-pos
    // unsatisfied-range   = "*/" complete-length
    //
    // complete-length     = 1*DIGIT
    private static final Pattern CONTENT_RANGE = Pattern.compile(
            "(?<unit>[^ ]+) (?:\\*/(?<unsatisfied>[0-9]+)|(?<first>[0-9]+)-(?<last>[0-9]+)/(?<length>[0-9]+|\\*))");

    @Nullable
    static public ContentRange parseRange(@Nullable String rangeHeader) {
        if (rangeHeader == null) {
            return null;
        }

        Matcher matcher = CONTENT_RANGE.matcher(rangeHeader);
        if (!matcher.matches()) {
            return null;
        }

        String unit = matcher.group("unit");

        try {
            String unsatisfied = matcher.group("unsatisfied");
            if (unsatisfied != null) {
                return new ContentRange(unit, null, null, Long.parseLong(unsatisfied));
            } else {
                long first = Long.parseLong(matcher.group("first"));
                long last = Long.parseLong(matcher.group("last"));
                String length = matcher.group("length");
                return new ContentRange(unit, first, last, length.equals("*") ? null : Long.parseLong(length));
            }
        } catch (NumberFormatException e) {
            return null;
        }
    }

    @Override
    public String toString() {
        StringBuilder header = new StringBuilder(unit);
        header.append(" ");

        if (start == null) {
            header.append("*");
        } else {
            header.append(start);
            header.append("-");
            header.append(end);
        }

        header.append("/");
        if (size == null) {
            header.append("*");
        } else {
            header.append(size);
        }

        return header.toString();
    }
}
