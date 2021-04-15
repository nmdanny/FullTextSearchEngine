package webdata.spimi;

import java.io.IOException;
import java.util.Iterator;

/** An iterator over a posting list, along with associated metadata and the ability
 *  to advance to another posting list.
 *
 *  Interface used for testing purposes, see {@link FileIndexIterator} for main
 *  implementation.
 */
public interface IndexIterator extends Iterator<DocAndFreq> {
    boolean moveToNextPostingList() throws IOException;

    int getDocumentFrequency();

    String getTerm();
}
