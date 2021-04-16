package webdata.spimi;

import webdata.DocAndFreq;

import java.io.IOException;
import java.util.Iterator;

/** An iterator over a posting list, along with associated metadata(term and document frequency)
 *  and the ability to advance to another posting list.
 *
 * <p>
 *  Interface used for testing purposes, see {@link FileIndexIterator} for main
 *  implementation.
 * </p>
 */
public interface IndexIterator extends Iterator<DocAndFreq> {
    /**
     * Tries resetting the iterator for reading the next posting list, assuming it is positioned at the beginning of
     * a posting list's metadata
     *
     * @return True if there's more input and the iterator was reset, false otherwise.
     * @throws IOException In case reading the metadata(term and document frequency) fails
     */
    boolean moveToNextPostingList() throws IOException;

    /**
     * @return The document frequency of the current term/the length of the posting list/
     *         size of the iterator
     */
    int getDocumentFrequency();

    /**
     * @return The term of the current posting list
     */
    String getTerm();
}
