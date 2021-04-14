package webdata.compression;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class FrontCodingEncoderTest {



    @Test
    void canEncodeAndDecode() throws IOException {

        var charset = StandardCharsets.ISO_8859_1;
        int maxPrefixes = 4;
        var os = new ByteArrayOutputStream();
        var encoder = new FrontCodingEncoder(maxPrefixes, charset, os);


        var res1 = encoder.encodeString("jezebel");

        assertEquals(0, res1.prefixLengthChars);
        assertEquals("jezebel".length(), res1.suffixLengthBytes);

        var res2 = encoder.encodeString("jezer");
        assertEquals(4, res2.prefixLengthChars);
        assertEquals(1, res2.suffixLengthBytes);

        var res3 = encoder.encodeString("jezerit");
        assertEquals(5, res3.prefixLengthChars);
        assertEquals(2, res3.suffixLengthBytes);

        var res4 = encoder.encodeString("jeziah");
        assertEquals(3, res4.prefixLengthChars);
        assertEquals(3, res4.suffixLengthBytes);


        var res5 = encoder.encodeString("jeziel");
        assertEquals(0, res5.prefixLengthChars);
        assertEquals(6, res5.suffixLengthBytes);

        var expectedStreamEncoding = charset.encode("jezebelritiahjeziel");
        assertArrayEquals(expectedStreamEncoding.array(), os.toByteArray());

        var is = new ByteArrayInputStream(os.toByteArray());
        var decoder = new FrontCodingDecoder(maxPrefixes, charset, is);

        assertEquals("jezebel", decoder.decodeElement(res1));
        assertEquals("jezer", decoder.decodeElement(res2));
        assertEquals("jezerit", decoder.decodeElement(res3));
        assertEquals("jeziah", decoder.decodeElement(res4));
        assertEquals("jeziel", decoder.decodeElement(res5));

        // ensure resetting works
        decoder.reset(new ByteArrayInputStream(os.toByteArray()));
        assertEquals("jezebel", decoder.decodeElement(res1));
        assertEquals("jezer", decoder.decodeElement(res2));
        assertEquals("jezerit", decoder.decodeElement(res3));
        assertEquals("jeziah", decoder.decodeElement(res4));
        assertEquals("jeziel", decoder.decodeElement(res5));
    }

    static Stream<List<String>> groupsProvider() {
        return Stream.of(
                List.of("abc", "abd", "abde", "adel", "armadillo", "armaverse"),
                List.of("אבג", "אבגד", "אבגה", "אבפל", "בנה", "בנהנה")
        );
    }

    @ParameterizedTest
    @MethodSource("groupsProvider")
    void canEncodeAndDecodeUTF8(List<String> strings) throws IOException {
        var charset = StandardCharsets.UTF_8;
        int maxPrefixes = 4;
        var os = new ByteArrayOutputStream();
        var encoder = new FrontCodingEncoder(maxPrefixes, charset, os);

        var results = new ArrayList<FrontCodingResult>();
        for (var string: strings) {
            results.add(encoder.encodeString(string));
        }

        var is = new ByteArrayInputStream(os.toByteArray());
        var decoder = new FrontCodingDecoder(maxPrefixes, charset, is);

        for (int i=0; i < strings.size(); ++i) {
            assertEquals(strings.get(i), decoder.decodeElement(results.get(i)));
        }
    }
}