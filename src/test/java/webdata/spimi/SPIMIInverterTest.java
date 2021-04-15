package webdata.spimi;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class SPIMIInverterTest {

    @Test
    void invert() throws IOException  {
        var os = new ByteArrayOutputStream();
        var spimi = new SPIMIInverter();

        var tokens = Stream.of(
                new Token("hello", 1, 3),
                new Token("shalom", 1 , 2),
                new Token("bye", 2, 1),
                new Token("bang", 2, 4),
                new Token("bang", 3, 2)
        );
        var tokensIt = tokens.iterator();
        spimi.invert(tokensIt, os);
        assertFalse(tokensIt.hasNext());

        var is = new ByteArrayInputStream(os.toByteArray());
        var it = new FileIndexIterator(is);

        assertEquals("bang", it.getTerm());
        assertEquals(2, it.getDocumentFrequency());

        assertIterableEquals(List.of(
                new DocAndFreq(2, 4), new DocAndFreq(3, 2)
        ), (Iterable<DocAndFreq>)(() -> it));

        assertTrue(it.moveToNextPostingList());
        assertEquals("bye", it.getTerm());
        assertEquals(1, it.getDocumentFrequency());
        assertIterableEquals(List.of(
                new DocAndFreq(2, 1)
        ), (Iterable<DocAndFreq>)(() -> it));

        assertTrue(it.moveToNextPostingList());
        assertEquals("hello", it.getTerm());
        assertEquals(1, it.getDocumentFrequency());
        assertIterableEquals(List.of(
                new DocAndFreq(1, 3)
        ), (Iterable<DocAndFreq>)(() -> it));


        assertTrue(it.moveToNextPostingList());
        assertEquals("shalom", it.getTerm());
        assertEquals(1, it.getDocumentFrequency());
        assertIterableEquals(List.of(
                new DocAndFreq(1, 2)
        ), (Iterable<DocAndFreq>)(() -> it));

        assertFalse(it.moveToNextPostingList());
    }
}