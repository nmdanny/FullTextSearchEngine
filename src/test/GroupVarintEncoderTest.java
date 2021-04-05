package test;

import org.junit.jupiter.api.Test;
import webdata.compression.GroupVarintDecoder;
import webdata.compression.GroupVarintEncoder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

class GroupVarintEncoderTest {
    @Test
    void canEncodeAndDecodeFullGroup() throws IOException
    {
        var os = new ByteArrayOutputStream();
        var encoder = new GroupVarintEncoder(os);
        encoder.write(10); // takes 1 byte
        encoder.write(990); // takes 2 bytes
        encoder.write(99000); // takes 3 bytes
        encoder.write(1); // takes 1 byte
        encoder.flush();

        var binary = os.toByteArray();
        assertEquals(8, binary.length);

        assertEquals("11000 1010 11 11011110 1 10000010 10111000 1",
                IntStream.range(0, binary.length).map(idx -> binary[idx])
                      .mapToObj(i -> Integer.toBinaryString(i & 0xff))
                      .collect(Collectors.joining(" "))
        );

        var is = new ByteArrayInputStream(binary);
        var decoder = new GroupVarintDecoder(is);

        int num1 = decoder.read();
        assertEquals(10, num1);
        int num2 = decoder.read();
        assertEquals(990, num2);
        int num3 = decoder.read();
        assertEquals(99000, num3);
        int num4 = decoder.read();
        assertEquals(1, num4);

        int next = decoder.read();
        assertEquals(-1, next);

    }

    @Test
    void canEncodeAndDecodePartialGroup() throws IOException
    {
        var os = new ByteArrayOutputStream();
        var encoder = new GroupVarintEncoder(os);
        encoder.write(10);
        encoder.write(990);
        encoder.flush();

        var binary = os.toByteArray();
        assertEquals(6, binary.length);

        assertEquals("10000 1010 11 11011110 0 0",
                IntStream.range(0, binary.length).map(idx -> binary[idx])
                        .mapToObj(i -> Integer.toBinaryString(i & 0xff))
                        .collect(Collectors.joining(" "))
        );

        var is = new ByteArrayInputStream(binary);
        var decoder = new GroupVarintDecoder(is);

        int num1 = decoder.read();
        assertEquals(10, num1);
        int num2 = decoder.read();
        assertEquals(990, num2);

        int num3 = decoder.read();
        assertEquals(-1, num3);
        int num4 = decoder.read();
        assertEquals(-1, num4);

        int num5 = decoder.read();
        assertEquals(-1, num5);
    }

}