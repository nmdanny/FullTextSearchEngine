package webdata.dictionary;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.CharBuffer;

/**
 * Represents a block element which isn't the first
 */
class OtherBlockElement implements DictionaryElement {


    Dictionary dictionary;
    FirstBlockElement firstBlockElement;
    int frequency;
    int postingPtr;
    int termLength;

    public OtherBlockElement(Dictionary dictionary, FirstBlockElement firstBlockElement, int frequency, int postingPtr, int termLength) {
        this.dictionary = dictionary;
        this.firstBlockElement = firstBlockElement;
        this.frequency = frequency;
        this.postingPtr = postingPtr;
        this.termLength = termLength;
    }

    public static OtherBlockElement NullElement(Dictionary dictionary, FirstBlockElement firstBlockElement) {
        return new OtherBlockElement(dictionary, firstBlockElement, -1, -1, -1);
    }

    @Override
    public void setDictionary(Dictionary dictionary) {
        this.dictionary = dictionary;
    }

    /**
     * Assigns the first element of the block which contains said element, needed to dereference term
     */
    public void setFirstBlockElement(FirstBlockElement element) {
        firstBlockElement = element;
    }

    @Override
    public CharBuffer getTerm() {
        return dictionary.derefTermPointer(firstBlockElement.getTermPointer(), termLength);
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
    }

    public static OtherBlockElement deserialize(DataInputStream in, Dictionary dictionary, FirstBlockElement firstBlockElement) throws IOException {
        int frequency = in.readInt();
        int postingPtr = in.readInt();
        int termLength= in.readInt();
        return new OtherBlockElement(dictionary, firstBlockElement, frequency, postingPtr, termLength);
    }
}
