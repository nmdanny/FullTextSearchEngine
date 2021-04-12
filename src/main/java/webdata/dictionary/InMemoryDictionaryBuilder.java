package webdata.dictionary;

import webdata.parsing.Tokenizer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;

class TermOccurrence {
    String term;
    int docId;
    int freqInDoc;
}

/** Used to fill a dictionary from a list of documents within memory. Every seen token is kept in memory
 *  until we actually fill the dictionary. */
public class InMemoryDictionaryBuilder {
    private final ArrayList<TermOccurrence> occurrences;
    private final SequentialDictionaryBuilder dictionaryBuilder;

    public InMemoryDictionaryBuilder(SequentialDictionaryBuilder dictionaryBuilder) {
        this.occurrences = new ArrayList<>();
        this.dictionaryBuilder = dictionaryBuilder;
    }

    public void processDocument(int docId, CharSequence document) {
        var tokens = Tokenizer.tokenize(document);
        processDocument(docId, tokens);
    }

    public void processDocument(int docId, String[] tokens) {
        var tokenToOccurrence = new HashMap<String, TermOccurrence>();
        for (var token: tokens) {
            TermOccurrence occurrence = tokenToOccurrence.get(token);
            if (occurrence == null) {
                occurrence = new TermOccurrence();
                occurrence.docId = docId;
                occurrence.term = token;
                occurrence.freqInDoc = 0;
                tokenToOccurrence.put(token, occurrence);
            }
            ++occurrence.freqInDoc;
        }
        occurrences.addAll(tokenToOccurrence.values());
    }

    public void finish() throws IOException {
        String curTerm = null;
        // TODO: if I compare via CharBuffer, modify
        //       the first comparator accordingly
        Comparator<TermOccurrence> comparator = Comparator.comparing(occ -> occ.term);
        comparator = comparator.thenComparingInt(occ -> occ.docId);
        occurrences.sort(comparator);
        for (var occurence: occurrences) {
            if (!occurence.term.equals(curTerm)) {
                curTerm = occurence.term;
                dictionaryBuilder.beginTerm(occurence.term);
            }
            dictionaryBuilder.addTermOccurence(occurence.docId, occurence.freqInDoc);
        }
        dictionaryBuilder.endTerm();
        dictionaryBuilder.flush();
    }
}
