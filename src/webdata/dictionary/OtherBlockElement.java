package webdata.dictionary;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.CharBuffer;

/**
 * Represents a block element which isn't the first
 */
class OtherBlockElement implements DictionaryElement {


    private Dictionary dictionary;
    private FirstBlockElement firstBlockElement;

    private int frequency;
    private int postingPtr;
    private int termLength;

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

    public static OtherBlockElement deserialize(DataInputStream in) throws IOException {
        var element = new OtherBlockElement();
        element.frequency = in.readInt();
        element.postingPtr = in.readInt();
        element.termLength = in.readInt();
        return element;
    }
}
