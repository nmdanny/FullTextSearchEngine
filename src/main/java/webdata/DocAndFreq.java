package webdata;

import java.util.Objects;

/** Tuple representing posting list element.
 *  Contains a document ID and frequency of term within that document.
 */
public class DocAndFreq {
    private final int docID;
    private final int docFrequency;

    public DocAndFreq(int docID, int docFrequency) {
        this.docID = docID;
        this.docFrequency = docFrequency;
    }

    public int getDocID() {
        return docID;
    }

    public int getDocFrequency() {
        return docFrequency;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DocAndFreq that = (DocAndFreq) o;
        return docID == that.docID && docFrequency == that.docFrequency;
    }

    @Override
    public int hashCode() {
        return Objects.hash(docID, docFrequency);
    }

    @Override
    public String toString() {
        return "DocAndFreq{" +
                "docID=" + docID +
                ", docFrequency=" + docFrequency +
                '}';
    }
}
