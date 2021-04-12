package webdata.dictionary;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;

/**
 * Represents the first element in a block, which contains a term pointer
 */
class FirstBlockElement implements DictionaryElement {
    final int frequency;
    final int postingPtr;
    final int termLength;
    final int termPointer;

    static final int SIZE_BYTES = 4 * 4;

    public FirstBlockElement(int frequency, int postingPtr, int termLength, int termPointer) {
        this.frequency = frequency;
        this.postingPtr = postingPtr;
        this.termLength = termLength;
        this.termPointer = termPointer;
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
        int frequency = in.readInt();
        int postingPtr = in.readInt();
        int termLength = in.readInt();
        int termPointer = in.readInt();
        return new FirstBlockElement(frequency, postingPtr, termLength, termPointer);
    }

}
