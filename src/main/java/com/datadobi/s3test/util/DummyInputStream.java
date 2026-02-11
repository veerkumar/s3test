/*
 *
 *  Copyright Datadobi
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
package com.datadobi.s3test.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

public class DummyInputStream extends InputStream {
    private final long size;
    private long position;

    public DummyInputStream(long size) {
        this.size = size;
        position = 0;
    }

    @Override
    public void close() {
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (position >= size) {
            return -1;
        }

        int remaining = (int) Math.min(len, size - position);
        Arrays.fill(b, off, off + remaining, (byte) 0);
        position += remaining;
        return remaining;
    }

    @Override
    public int read() throws IOException {
        if (position >= size) {
            return -1;
        }
        position++;
        return 0;
    }
}