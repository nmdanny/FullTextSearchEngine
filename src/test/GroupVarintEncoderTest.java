package test;

import org.junit.jupiter.api.Test;
import webdata.compression.GroupVarintEncoder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

class GroupVarintEncoderTest {
    @Test
    void canWriteFullGroup() throws IOException
    {
        var os = new ByteArrayOutputStream();
        var encoder = new GroupVarintEncoder(os);
        encoder.write(10);
        encoder.write(990);
        encoder.write(99000);
        encoder.write(1);
        encoder.flush();

        var binary = os.toByteArray();
        assertEquals(8, binary.length);

        assertEquals("11000 1010 11 11011110 1 10000010 10111000 1",
                IntStream.range(0, binary.length).map(idx -> binary[idx])
                      .mapToObj(i -> Integer.toBinaryString(i & 0xff))
                      .collect(Collectors.joining(" "))
        );

    }

}