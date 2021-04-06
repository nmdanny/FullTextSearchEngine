package webdata.dictionary;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.CharBuffer;

/**
 * Represents the first element in a block, which contains a term pointer
 */
class FirstBlockElement implements DictionaryElement {
    private Dictionary dictionary;

    private int frequency;
    private int postingPtr;
    private int termLength;
    private int termPointer;

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

    public static FirstBlockElement deserialize(DataInputStream in) throws IOException {
        var element = new FirstBlockElement();
        element.frequency = in.readInt();
        element.postingPtr = in.readInt();
        element.termLength = in.readInt();
        element.termPointer = in.readInt();
        return element;
    }

}
