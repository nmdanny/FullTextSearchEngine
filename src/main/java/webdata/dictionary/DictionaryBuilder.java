package webdata.dictionary;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;

/** Can be used to create dictionaries in a sequential manner, by adding
 *  terms(lexicographically ordered) and tokens(ordered by terms).
 *
 *  (Interface exists for testing purposes, see {@link SequentialDictionaryBuilder}
 *   for the main implementation)
 *  */
public interface DictionaryBuilder extends Closeable, Flushable {

    void beginTerm(String term) throws IOException;

    void endTerm() throws IOException;

    void addTermOccurence(int docId, int freqInDoc) throws IOException;
}

