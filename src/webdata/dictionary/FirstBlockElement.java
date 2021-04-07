package webdata.dictionary;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.CharBuffer;

/**
 * Represents the first element in a block, which contains a term pointer
 */
class FirstBlockElement implements DictionaryElement {
    Dictionary dictionary;
    int frequency;
    int postingPtr;
    int termLength;
    int termPointer;

    public FirstBlockElement(Dictionary dictionary, int frequency, int postingPtr, int termLength, int termPointer) {
        this.dictionary = dictionary;
        this.frequency = frequency;
        this.postingPtr = postingPtr;
        this.termLength = termLength;
        this.termPointer = termPointer;
    }

    public static FirstBlockElement NullElement(Dictionary dictionary) {
        return new FirstBlockElement(dictionary, -1, -1, -1, -1);
    }

    @Override
    public void setDictionary(Dictionary dictionary) {
        this.dictionary = dictionary;
    }

    @Override
    public CharBuffer getTerm() {
        return dictionary.derefTermPointer(termPointer, termLength);
    }

    public int getTermPointer() {
        return termPointer;
    }

    @Override
    public int getTokenFrequency() {
        return frequency;
    }

    @Override
    public int getPostingsPointer() {
        return postingPtr;
    }

    public void serialize(DataOutputStream out) throws IOException {
        out.writeInt(frequency);
        out.writeInt(postingPtr);
        out.writeInt(termLength);
        out.writeInt(termPointer);
    }

    public static FirstBlockElement deserialize(DataInputStream in, Dictionary dictionary) throws IOException {
        int frequency = in.readInt();
        int postingPtr = in.readInt();
        int termLength = in.readInt();
        int termPointer = in.readInt();
        return new FirstBlockElement(dictionary, frequency, postingPtr, termLength, termPointer);
    }

}
