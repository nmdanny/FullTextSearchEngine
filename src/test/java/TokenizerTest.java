package test;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import webdata.parsing.Tokenizer;

import static org.junit.jupiter.api.Assertions.*;

class TokenizerTest {

    @Test
    void tokenizationWorks() {

        String input = "It's time to party!   <br><\n>12-34-test";

        String[] expectedTokens = new String[] {
                "it",
                "s",
                "time",
                "to",
                "party",
                "br",
                "12",
                "34",
                "test"
        };

        assertArrayEquals(Tokenizer.tokenize(input), expectedTokens);
    }

}