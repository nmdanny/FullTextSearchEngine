package webdata.compression;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/** Allows encoding/decoding integers via varint length encoding.
 *
 *  While group varint encoding is probably more CPU efficient(due to less branching),
 *  (de)coding it requires a lot of extra state, while this one is stateless.
 *
 *  This allows us to use the varint encoding on in-memory posting lists(when building the temporary index files),
 *  without having to store a heavyweight encoder for each one of them, reducing memory usage
 *
 *
 *  faster to decode, this one is stateless, allowing us to use it while
 *  Group varint is more efficient, but this one is stateless */
public class Varint {
    public static void encode(OutputStream os, int value) throws IOException {
        assert value > 0 : "Can only encode positive values";
        boolean doContinue = true;
        while (doContinue) {
            int lowest7 = value & 0x7F;
            value >>>= 7;
            if (value == 0) {
                lowest7 |= 0x80;
                doContinue = false;
            }
            os.write(lowest7);
        }
    }

    public static int decode(InputStream is) throws IOException {
        int value = 0;
        boolean doContinue;
        int numIncrements = 0;
        do {
            int byteRead = is.read();
            if (byteRead == -1) {
                return -1;
            }
            int lowest7 = byteRead & 0x7F;
            doContinue = (byteRead & 0x80) == 0;
            value |= (lowest7 << numIncrements);
            numIncrements += 7;
        } while (doContinue);
        return value;
    }
}
