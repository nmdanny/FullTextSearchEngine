package webdata.storage;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class IntSerializableFactory implements SerializableFactory<Integer> {
    @Override
    public Integer deserialize(DataInputStream dis) throws IOException {
        return dis.readInt();
    }

    @Override
    public void serialize(Integer element, DataOutputStream dos) throws IOException {
        dos.writeInt(element);
    }
}
