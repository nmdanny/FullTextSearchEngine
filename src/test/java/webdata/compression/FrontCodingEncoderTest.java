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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class FrontCodingEncoderTest {


    static Stream<List<String>> groupsProvider() {
        return Stream.of(
                List.of("abc", "abd", "abde", "adel"),
                List.of("אבג", "אבגד", "אבגה", "אבפל")
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

    @Test
    void canEncodeAndDecode() throws IOException {

        var charset = StandardCharsets.ISO_8859_1;
        int maxPrefixes = 4;
        var os = new ByteArrayOutputStream();
        var encoder = new FrontCodingEncoder(maxPrefixes, charset, os);


        var res1 = encoder.encodeString("jezebel");

        assertEquals(0, res1.prefixLength);
        assertEquals("jezebel".length(), res1.suffixLength);

        var res2 = encoder.encodeString("jezer");
        assertEquals(4, res2.prefixLength);
        assertEquals(1, res2.suffixLength);

        var res3 = encoder.encodeString("jezerit");
        assertEquals(5, res3.prefixLength);
        assertEquals(2, res3.suffixLength);

        var res4 = encoder.encodeString("jeziah");
        assertEquals(3, res4.prefixLength);
        assertEquals(3, res4.suffixLength);


        var res5 = encoder.encodeString("jeziel");
        assertEquals(0, res5.prefixLength);
        assertEquals(6, res5.suffixLength);

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
}