package webdata.compression;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;

/** Responsible for decoding strings compressed via (k-1)-in-k front coding. */
public class FrontCodingDecoder implements Closeable {

    private final Charset charset;
    private final CharsetDecoder charsetDecoder;
    private InputStream is;

    private final int maxElements;
    private int numElements = 0;
    private String prevWord;


    // Initial size of buffer containing suffix
    // (Buffer will be re-allocated with a larger size if
    //  we happen to encounter a longer suffix)
    private final int INITIAL_BUF_SIZE = 1024;

    // Contains suffix
    private ByteBuffer readBuf;

    /** Creates a (k-1)-in-k front coding decoder
     * @param maxElements 'k', the group size
     * @param charset Charset, used for encoding
     * @param is Input stream aligned to the first expected group member
     */
    public FrontCodingDecoder(int maxElements, Charset charset, InputStream is) {
        this.charset = charset;
        this.charsetDecoder = charset.newDecoder()
                .onMalformedInput(CodingErrorAction.REPLACE)
                .onUnmappableCharacter(CodingErrorAction.REPLACE);
        this.is = is;
        this.maxElements = maxElements;
        this.numElements = 0;
        this.prevWord = null;

        this.readBuf = ByteBuffer.allocate(INITIAL_BUF_SIZE);
    }

    private String decodeFirstElement(FrontCodingResult result) throws IOException {
        assert result.prefixLengthChars == 0;

        if (result.suffixLengthBytes > readBuf.capacity()) {
            readBuf = ByteBuffer.allocate(result.suffixLengthBytes * 2);
        }
        int read = is.readNBytes(readBuf.array(), 0, result.suffixLengthBytes);
        assert read == result.suffixLengthBytes;

        readBuf.position(0).limit(result.suffixLengthBytes);
        String suffix = charsetDecoder.decode(readBuf).toString();

        this.prevWord = suffix;
        return suffix;
    }

    private String decodeAnyOtherElement(FrontCodingResult result) throws IOException {
        readBuf.clear();
        var arr = readBuf.array();
        for (int i=0; i < readBuf.capacity(); ++i) {
            arr[i] = 0;
        }

        if (result.suffixLengthBytes > readBuf.capacity()) {
            readBuf = ByteBuffer.allocate(result.suffixLengthBytes * 2);
        }
        int read = is.readNBytes(readBuf.array(), 0, result.suffixLengthBytes);
        assert read == result.suffixLengthBytes;


        String prefix = prevWord.substring(0, result.prefixLengthChars);
        readBuf.position(0).limit(result.suffixLengthBytes);
        String suffix = charsetDecoder.decode(readBuf).toString();

        this.prevWord = prefix + suffix;
        return this.prevWord;
    }

    /** Resets the current group, starting a new group assumed to begin at given input stream,
     *  and closes the previous input stream. */
    public void reset(InputStream is) throws IOException {
        this.is.close();
        this.numElements = 0;
        this.prevWord = null;
        this.is = is;
    }

    /** Decodes the next word iwthin the current group(or the first one if this
     *  is the group start) using the given front coding result.
     */
    public String decodeElement(FrontCodingResult result) throws IOException {
        String ret;
        if (numElements % maxElements == 0) {
            ret = decodeFirstElement(result);
        } else {
            ret = decodeAnyOtherElement(result);
        }
        ++numElements;
        return ret;
    }

    @Override
    public void close() throws IOException {
        this.is.close();
    }
}
