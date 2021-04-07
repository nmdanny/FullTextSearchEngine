package webdata.dictionary;

import webdata.parsing.Tokenizer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
    private final Dictionary dictionary;

    public InMemoryDictionaryBuilder(Dictionary dictionary) {
        this.occurrences = new ArrayList<>();
        this.dictionary = dictionary;
    }

    public void processDocument(int docId, CharSequence document) {
        var tokens = Tokenizer.tokenize(document);
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
        occurrences.sort(Comparator.comparing(occ -> occ.term));
        for (var occurence: occurrences) {
            if (!occurence.term.equals(curTerm)) {
                curTerm = occurence.term;
                dictionary.beginTerm(occurence.term);
            }
            dictionary.addTermOccurence(occurence.docId, occurence.freqInDoc);
        }
        dictionary.endTerm();
    }
}
