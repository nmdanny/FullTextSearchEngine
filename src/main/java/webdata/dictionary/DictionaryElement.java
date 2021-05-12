package webdata.dictionary;


/** Represents a dictionary element. All these getters should be O(1) and avoid IO. */
interface DictionaryElement {
    /** Returns the number of documents containing at least 1 occurrences of this term.
     *  Equivalently, this is the length of the posting list of this term. */
    int getTokenFrequency();

    /** Returns the number of occurrences of this term within the corpus, including duplicates. */
    int getTokenCollectionFrequency();

    /** Gets a pointer to the postings list of this term. */
    long getPostingsPointer();
}
