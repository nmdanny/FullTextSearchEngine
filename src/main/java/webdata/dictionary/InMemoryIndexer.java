package webdata.dictionary;

import webdata.Token;
import webdata.parsing.Tokenizer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.stream.Stream;

/** Used to fill a dictionary from a list of documents within memory. Every seen token is kept in memory
 *  until we actually fill the dictionary. */
public class InMemoryIndexer {
    private final ArrayList<Token> occurrences;
    private final SequentialDictionaryBuilder dictionaryBuilder;

    public InMemoryIndexer(SequentialDictionaryBuilder dictionaryBuilder) {
        this.occurrences = new ArrayList<>();
        this.dictionaryBuilder = dictionaryBuilder;
    }

    public void processDocument(int docId, CharSequence document) {
        processDocument(docId, Tokenizer.tokensAsStream(document));
    }

    /**
     * Processes a document
     * @param docId Document ID
     * @param tokens Document tokens
     */
    public void processDocument(int docId, Stream<String> tokens) {
        var tokenToFreq = new HashMap<String, Integer>();
        tokens.forEach(token -> {
            assert !token.isEmpty();
            tokenToFreq.merge(token, 1, Integer::sum);
        });
        for (var entry: tokenToFreq.entrySet()) {
            occurrences.add(new Token(
                    entry.getKey(), docId, entry.getValue()
            ));
        }
    }

    public void finish() throws IOException {
        String curTerm = null;
        Comparator<Token> comparator = Comparator.comparing(Token::getTerm);
        comparator = comparator.thenComparingInt(Token::getDocID);
        occurrences.sort(comparator);
        for (var occurence: occurrences) {
            if (!occurence.getTerm().equals(curTerm)) {
                curTerm = occurence.getTerm();
                dictionaryBuilder.beginTerm(occurence.getTerm());
            }
            dictionaryBuilder.addTermOccurence(occurence.getDocID(), occurence.getDocFrequency());
        }
        dictionaryBuilder.endTerm();
        dictionaryBuilder.flush();
    }
}
