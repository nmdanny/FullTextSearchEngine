package webdata.compression;//package test;

import org.junit.jupiter.api.Test;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;


import webdata.compression.GroupVarintDecoder;
import webdata.compression.GroupVarintEncoder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class GroupVarintEncoderTest {
    static void ensureCanEncodeAndDecodeIntegers(List<Integer> numbers) throws IOException {
        var os = new ByteArrayOutputStream();
        var encoder = new GroupVarintEncoder(os);

        for (var num: numbers) {
            encoder.write(num);
        }

        encoder.flush();
        var is = new ByteArrayInputStream(os.toByteArray());
        var decoder = new GroupVarintDecoder(is);

        for(int i=0; i < numbers.size(); ++i) {
            var decoded = decoder.read();
            assertEquals(numbers.get(i), decoded);
        }

        int numZeros = 0;
        int decoded;
        do {
            decoded = decoder.read();
            numZeros++;
        } while (decoded == 0);
        numZeros--;
        assertEquals(-1, decoded);
        assertTrue(numZeros <= 3);
    }


    private static Stream<List<Integer>> provideNumbers() {

        var lists = Stream.of(
                List.<Integer>of(),
                List.of(10, 99),
                List.of(1,2,3,4,5,6,7,8,9,10),
                List.of(9,1000,100,5000,100000,125255600)
        );

        return lists.flatMap(list -> {
            return IntStream.range(1, list.size() + 1).mapToObj(length -> list
                    .stream()
                    .limit(length)
                    .collect(Collectors.toList()));
        });

    }

    @ParameterizedTest
    @MethodSource("provideNumbers")
    void canEncodeAndDecodeIntegers(List<Integer> numbers) throws IOException {
        ensureCanEncodeAndDecodeIntegers(numbers);
    }


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
        assertEquals(0, num3);

        int num4 = decoder.read();
        assertEquals(0, num4);

        int num5 = decoder.read();
        assertEquals(-1, num5);
    }

    @Test
    void canEncodeAndDecodeFullGroupHuge() throws IOException
    {
        var os = new ByteArrayOutputStream();
        var encoder = new GroupVarintEncoder(os);
        encoder.write(2147483647);
        encoder.write(2011185518);
        encoder.write(16777216);
        encoder.write(176414542);
        encoder.flush();

        var binary = os.toByteArray();
        assertEquals(17, binary.length);

        assertEquals(255, binary[0] & 0xff);

        var is = new ByteArrayInputStream(binary);
        var decoder = new GroupVarintDecoder(is);

        int num1 = decoder.read();
        assertEquals(2147483647, num1);
        int num2 = decoder.read();
        assertEquals(2011185518, num2);
        int num3 = decoder.read();
        assertEquals(16777216, num3);
        int num4 = decoder.read();
        assertEquals(176414542, num4);

        int next = decoder.read();
        assertEquals(-1, next);

    }
}