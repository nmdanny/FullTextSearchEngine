package webdata.spimi;

import org.junit.jupiter.api.Test;
import webdata.DocAndFreq;
import webdata.Token;
import webdata.Utils;
import webdata.dictionary.Dictionary;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.*;

class TemporaryIndexBuilderTest {

    @Test
    void invert() throws IOException  {
        var spimi = new TemporaryIndexBuilder();

        var tokens = Stream.of(
                new Token("hello", 1, 3),
                new Token("shalom", 1 , 2),
                new Token("bye", 2, 1),
                new Token("bang", 2, 4),
                new Token("bang", 3, 2)
        ).collect(Collectors.toList());

        var dictPath = Files.createTempDirectory("testSPIMIInvert");

        var tokensIt = tokens.iterator();
        spimi.invert(tokensIt, dictPath);
        assertFalse(tokensIt.hasNext());

        var dict = new Dictionary(dictPath.toString());

        tokens.sort(Comparator.comparing(Token::getTerm).thenComparing(Token::getDocID));
        var gottenTokens = StreamSupport.stream(dict.tokens(), false).collect(Collectors.toList());
        assertIterableEquals(tokens, gottenTokens);

        Utils.deleteDirectory(dictPath);
    }
}