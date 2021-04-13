package webdata.storage;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Allows (de)serialization of some generic type in a constant amount of bytes
 */
public interface FixedSizeRecordFactory<Record> {
    /** The number of bytes taken by a record. Must not change */
    int sizeBytes();

    Record deserialize(ByteBuffer buf) throws IOException;

    void serialize(Record record, DataOutputStream dos) throws IOException;


}
