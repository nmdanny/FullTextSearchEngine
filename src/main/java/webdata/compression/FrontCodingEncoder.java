package webdata.compression;

import java.io.*;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;

/** Responsible for encoding strings using (k-1)-in-k front coding */
public class FrontCodingEncoder implements Closeable, Flushable {
    private final Charset charset;
    private final CharsetEncoder charsetEncoder;
    private final OutputStream os;
    private final int maxElements;

    private long numBytesWritten;
    private int numElements;
    private String curPrefix;


    /** Creates a (k-1)-in-k front coding encoder
     * @param maxElements 'k', the group size
     * @param charset Charset, used for encoding
     * @param os Output stream where bytes are emitted
     */
    public FrontCodingEncoder(int maxElements, Charset charset, OutputStream os) {
        this.charset = charset;
        this.charsetEncoder = charset.newEncoder()
                .onMalformedInput(CodingErrorAction.REPLACE)
                .onUnmappableCharacter(CodingErrorAction.REPLACE);
        this.os = os;
        this.maxElements = maxElements;

        this.numBytesWritten = 0;
        this.numElements = 0;
        this.curPrefix = "";
    }

    private FrontCodingResult startNewPrefix(String string) throws IOException {
        var bytes = charsetEncoder.encode(CharBuffer.wrap(string));

        long pos = numBytesWritten;
        int suffixLengthBytes = bytes.limit();

        os.write(bytes.array(), 0, suffixLengthBytes);
        numBytesWritten += suffixLengthBytes;

        return new FrontCodingResult(pos, 0, suffixLengthBytes);
    }

    private int greatestCommonPrefix(String a, String b) {
        int to = Integer.min(a.length(), b.length());
        int prefixLength = 0;
        for (int i=0; i < to; ++i) {
            if (a.charAt(i) == b.charAt(i)) {
                ++prefixLength;
            } else {
                break;
            }
        }
        return prefixLength;
    }

    private FrontCodingResult addWithPrefix(String string) throws IOException {
        int prefixLength = greatestCommonPrefix(string, curPrefix);
        var suffixBytes = charsetEncoder.encode(CharBuffer.wrap(string, prefixLength, string.length()));
        long pos = numBytesWritten;
        int suffixLengthBytes = suffixBytes.limit();

        os.write(suffixBytes.array(), 0, suffixLengthBytes);
        numBytesWritten += suffixLengthBytes;

        return new FrontCodingResult(pos, prefixLength, suffixLengthBytes);
    }

    /** Encodes a new string, returning information that can be used to decode it via front coding */
    public FrontCodingResult encodeString(String string) throws IOException {
        FrontCodingResult result;
        if (numElements == maxElements || numElements == 0) {
            result = startNewPrefix(string);
            curPrefix = string;
            numElements = 1;
        } else {
            assert (string.compareTo(curPrefix) >= 0) : "Strings must be encoded in lexicographically increasing order";
            result = addWithPrefix(string);
            curPrefix = string;
            ++numElements;
        }
        return result;
    }

    @Override
    public void flush() throws IOException {
        os.flush();
    }

    @Override
    public void close() throws IOException {
        flush();
        os.close();
    }
}
