package webdata.dictionary;

import java.nio.CharBuffer;

/** Represents a dictionary element. All these getters should be O(1) and ideally avoid IO. */
public interface DictionaryElement extends Comparable<DictionaryElement> {

    /** Sets the dictionary which contains this element. Might be used to dereference the term */
    void setDictionary(Dictionary dictionary);

    /** Returns the term of this element. Should only be called after a dictionary was associated to it */
    CharBuffer getTerm();

    /** Returns the number of documents containing at least 1 occurrences of this term.
     *  Equivalently, this is the length of the posting list of this term. */
    int getTokenFrequency();

    /** Gets a pointer to the postings list of this term. */
    int getPostingsPointer();

    @Override
    default int compareTo(DictionaryElement other) {
        return getTerm().compareTo(other.getTerm());
    }
}


