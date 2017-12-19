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
package org.apache.cassandra.db;

import java.io.IOException;

import com.google.common.base.Objects;
import com.google.common.hash.Hasher;

import org.apache.cassandra.cache.IMeasurableMemory;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.HashingUtils;
import org.apache.cassandra.utils.ObjectSizes;

import static java.lang.Math.min;

/**
 * Information on deletion of a storage engine object.
 */
public class DeletionTime implements Comparable<DeletionTime>, IMeasurableMemory
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new DeletionTime(0, 0));

    /**
     * A special DeletionTime that signifies that there is no top-level (row) tombstone.
     */
    public static final DeletionTime LIVE = new DeletionTime(Long.MIN_VALUE, Long.MAX_VALUE);

    public static final Serializer serializer = new Serializer();

    public static final Serializer legacySerializer = new LegacySerializer();

    private final long markedForDeleteAt;
    private final long localDeletionTime;

    public DeletionTime(long markedForDeleteAt, long localDeletionTime)
    {
        this.markedForDeleteAt = markedForDeleteAt;
        this.localDeletionTime = localDeletionTime;
    }

    /**
     * A timestamp (typically in microseconds since the unix epoch, although this is not enforced) after which
     * data should be considered deleted. If set to Long.MIN_VALUE, this implies that the data has not been marked
     * for deletion at all.
     */
    public long markedForDeleteAt()
    {
        return markedForDeleteAt;
    }

    /**
     * The local server timestamp, in seconds since the unix epoch, at which this tombstone was created. This is
     * only used for purposes of purging the tombstone after gc_grace_seconds have elapsed.
     */
    public long localDeletionTime()
    {
        return localDeletionTime;
    }

    /**
     * Returns whether this DeletionTime is live, that is deletes no columns.
     */
    public boolean isLive()
    {
        return markedForDeleteAt() == Long.MIN_VALUE && localDeletionTime() == Long.MAX_VALUE;
    }

    public void digest(Hasher hasher)
    {
        // localDeletionTime is basically a metadata of the deletion time that tells us when it's ok to purge it.
        // It's thus intrinsically a local information and shouldn't be part of the digest (which exists for
        // cross-nodes comparisons).
        HashingUtils.updateWithLong(hasher, markedForDeleteAt());
    }

    @Override
    public boolean equals(Object o)
    {
        if(!(o instanceof DeletionTime))
            return false;
        DeletionTime that = (DeletionTime)o;
        return markedForDeleteAt() == that.markedForDeleteAt() && localDeletionTime() == that.localDeletionTime();
    }

    @Override
    public final int hashCode()
    {
        return Objects.hashCode(markedForDeleteAt(), localDeletionTime());
    }

    @Override
    public String toString()
    {
        return String.format("deletedAt=%d, localDeletion=%d", markedForDeleteAt(), localDeletionTime());
    }

    public int compareTo(DeletionTime dt)
    {
        if (markedForDeleteAt() < dt.markedForDeleteAt())
            return -1;
        else if (markedForDeleteAt() > dt.markedForDeleteAt())
            return 1;
        else if (localDeletionTime() < dt.localDeletionTime())
            return -1;
        else if (localDeletionTime() > dt.localDeletionTime())
            return 1;
        else
            return 0;
    }

    public boolean supersedes(DeletionTime dt)
    {
        return markedForDeleteAt() > dt.markedForDeleteAt() || (markedForDeleteAt() == dt.markedForDeleteAt() && localDeletionTime() > dt.localDeletionTime());
    }

    public boolean deletes(LivenessInfo info)
    {
        return deletes(info.timestamp());
    }

    public boolean deletes(Cell cell)
    {
        return deletes(cell.timestamp());
    }

    public boolean deletes(long timestamp)
    {
        return timestamp <= markedForDeleteAt();
    }

    public int dataSize()
    {
        return 16;
    }

    public long unsharedHeapSize()
    {
        return EMPTY_SIZE;
    }


    public ISerializer<DeletionTime> getSerializer(Version version)
    {
        if (version.hasLongLocalDeletionTime())
        {
            return new Serializer();
        }
        else
        {
            return new LegacySerializer();
        }

    }

    public static class Serializer implements ISerializer<DeletionTime>
    {
        public void serialize(DeletionTime delTime, DataOutputPlus out) throws IOException
        {
            out.writeLong(delTime.localDeletionTime());
            out.writeLong(delTime.markedForDeleteAt());
        }

        public DeletionTime deserialize(DataInputPlus in) throws IOException
        {
            long ldt = in.readLong();
            long mfda = in.readLong();
            return mfda == Long.MIN_VALUE && ldt == Long.MAX_VALUE
                 ? LIVE
                 : new DeletionTime(mfda, ldt);
        }

        public void skip(DataInputPlus in) throws IOException
        {
            in.skipBytesFully(8 + 8);
        }

        public long serializedSize(DeletionTime delTime)
        {
            return TypeSizes.sizeof(delTime.localDeletionTime())
                 + TypeSizes.sizeof(delTime.markedForDeleteAt());
        }
    }

    public static class LegacySerializer extends Serializer
    {
        public void serialize(DeletionTime delTime, DataOutputPlus out) throws IOException
        {
            int ldt = delTime.localDeletionTime == Long.MAX_VALUE ? Integer.MAX_VALUE : (int)min(delTime.localDeletionTime, (long)Integer.MAX_VALUE - 1);
            out.writeLong(delTime.localDeletionTime());
            out.writeInt(ldt);
        }

        public DeletionTime deserialize(DataInputPlus in) throws IOException
        {
            long ldt = in.readInt();
            long mfda = in.readLong();
            return mfda == Long.MIN_VALUE && ldt == Integer.MAX_VALUE
                 ? LIVE
                 : new DeletionTime(mfda, ldt);
        }

        public void skip(DataInputPlus in) throws IOException
        {
            in.skipBytesFully(8 + 4);
        }

        public long serializedSize(DeletionTime delTime)
        {
            return TypeSizes.sizeof(delTime.localDeletionTime())
                 + TypeSizes.sizeof(Integer.MAX_VALUE);
        }
    }
}
