package webdata.storage;

import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.*;

class SerializableFactoryTest {
    @Test
    void testSerializeStream() throws IOException {
        var sfactory = new IntSerializableFactory();
        var baos = new ByteArrayOutputStream();
        var dos = new DataOutputStream(baos);

        sfactory.serialize(1, dos);
        sfactory.serialize(2, dos);
        sfactory.serialize(3, dos);

        var dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));

        var result = StreamSupport.stream(sfactory.deserializeStream(dis), false)
                .collect(Collectors.toList());

        assertIterableEquals(List.of(1, 2, 3), result);

    }
}