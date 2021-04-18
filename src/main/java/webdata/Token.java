package webdata;

import java.util.Objects;

/** Represents a token */
public class Token {
    private final String term;
    private final int docID;
    private final int docFrequency;

    public Token(String term, int docID, int docFrequency) {
        assert !term.isEmpty() : "Term cannot be empty";
        assert docID > 0 : "docID must be positive";
        assert docFrequency > 0 : "docFrequency must be positive";

        this.term = term;
        this.docID = docID;
        this.docFrequency = docFrequency;
    }

    public String getTerm() {
        return term;
    }

    public int getDocID() {
        return docID;
    }

    public int getDocFrequency() {
        return docFrequency;
    }

    public DocAndFreq toDocAndFreq() {
        return new DocAndFreq(docID, docFrequency);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Token token = (Token) o;
        return docID == token.docID && docFrequency == token.docFrequency && term.equals(token.term);
    }

    @Override
    public int hashCode() {
        return Objects.hash(term, docID, docFrequency);
    }

    @Override
    public String toString() {
        return "Token{" +
                "term='" + term + '\'' +
                ", docID=" + docID +
                ", docFrequency=" + docFrequency +
                '}';
    }
}
