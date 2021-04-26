package webdata.dictionary;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Represents a block element which isn't the first
 */
class OtherBlockElement implements DictionaryElement {
    final FirstBlockElement fbe;
    final int frequency;
    final int postingPtrOffset;
    final int prefixLength;
    final int suffixLength;

    static final int SIZE_BYTES = 4 * 4;

    public OtherBlockElement(
            FirstBlockElement fbe,
            int frequency, int postingPtrOffset, int prefixLength, int suffixLength) {
        assert frequency > 0 && postingPtrOffset >= 0 && prefixLength >= 0 && suffixLength > 0;
        assert fbe != null;
        this.fbe = fbe;
        this.frequency = frequency;
        this.postingPtrOffset = postingPtrOffset;
        this.prefixLength = prefixLength;
        this.suffixLength = suffixLength;
    }

    @Override
    public int getTokenFrequency() {
        return frequency;
    }

    @Override
    public long getPostingsPointer() {
        return fbe.getPostingsPointer() + postingPtrOffset;
    }

    public void serialize(DataOutputStream out) throws IOException {
        out.writeInt(frequency);
        out.writeInt(postingPtrOffset);
        out.writeInt(prefixLength);
        out.writeInt(suffixLength);
    }

    public static OtherBlockElement deserialize(FirstBlockElement fbe, DataInputStream in) throws IOException {
        int frequency = in.readInt();
        int postingPtrOffset = in.readInt();
        int prefixLength = in.readInt();
        int suffixLength = in.readInt();
        return new OtherBlockElement(fbe, frequency, postingPtrOffset, prefixLength, suffixLength);
    }

    public static OtherBlockElement deserialize(FirstBlockElement fbe, ByteBuffer buf) {
        int frequency = buf.getInt();
        int postingPtrOffset = buf.getInt();
        int prefixLength = buf.getInt();
        int suffixLength = buf.getInt();
        return new OtherBlockElement(fbe, frequency, postingPtrOffset, prefixLength, suffixLength);
    }
}
