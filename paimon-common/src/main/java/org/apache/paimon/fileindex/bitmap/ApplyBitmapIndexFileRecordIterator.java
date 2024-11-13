/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.fileindex.bitmap;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.utils.RoaringBitmap32;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * A {@link FileRecordIterator} wraps a {@link FileRecordIterator} and {@link BitmapIndexResult}.
 */
public class ApplyBitmapIndexFileRecordIterator implements FileRecordIterator<InternalRow> {

    private final FileRecordIterator<InternalRow> iterator;
    private final RoaringBitmap32 bitmap;
    private final int last;

    public ApplyBitmapIndexFileRecordIterator(
            FileRecordIterator<InternalRow> iterator, BitmapIndexResult fileIndexResult) {
        this.iterator = iterator;
        this.bitmap = fileIndexResult.get();
        this.last = bitmap.last();
    }

    @Override
    public long returnedPosition() {
        return iterator.returnedPosition();
    }

    @Override
    public Path filePath() {
        return iterator.filePath();
    }

    @Nullable
    @Override
    public InternalRow next() throws IOException {
        while (true) {
            InternalRow next = iterator.next();
            if (next == null) {
                return null;
            }
            int position = (int) returnedPosition();
            if (position > last) {
                return null;
            }
            if (bitmap.contains(position)) {
                return next;
            }
        }
    }

    @Override
    public void releaseBatch() {
        iterator.releaseBatch();
    }
}
