package webdata.dictionary;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.CharBuffer;

/**
 * Represents a block element which isn't the first
 */
class OtherBlockElement implements DictionaryElement {
    final int frequency;
    final int postingPtr;
    final int termLength;

    public OtherBlockElement(int frequency, int postingPtr, int termLength) {
        this.frequency = frequency;
        this.postingPtr = postingPtr;
        this.termLength = termLength;
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
        int frequency = in.readInt();
        int postingPtr = in.readInt();
        int termLength = in.readInt();
        return new OtherBlockElement(frequency, postingPtr, termLength);
    }
}
