package webdata.compression;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;

/** Responsible for decoding strings compressed via (k-1)-in-k front coding.
* */
public class FrontCodingDecoder {

    private final String string;

    private final int maxElements;
    private String prevWord;


    /** Creates a (k-1)-in-k front coding decoder
     * @param maxElements 'k', the group size
     * @param charset Charset, used for encoding
     * @param path Path to terms
     */
    public FrontCodingDecoder(int maxElements, Charset charset, Path path) throws IOException {
        this(maxElements, Files.readString(path, charset));
    }

    public FrontCodingDecoder(int maxElements, Charset charset, InputStream is) throws IOException {
        var bytes = is.readAllBytes();
        this.maxElements = maxElements;
        this.string = charset.decode(ByteBuffer.wrap(bytes)).toString();
        this.prevWord = null;
    }

    public FrontCodingDecoder(int maxElements, String string) {
        this.maxElements = maxElements;
        this.string = string;
        this.prevWord = null;
    }


    private String decodeFirstElement(FrontCodingResult result) {
        assert result.prefixLength == 0;

        String suffix = string.substring(result.suffixPos, result.suffixPos + result.suffixLength);
        this.prevWord = suffix;
        return suffix;
    }

    private String decodeAnyOtherElement(FrontCodingResult result) {
        String prefix = prevWord.substring(0, result.prefixLength);
        String suffix = string.substring(result.suffixPos, result.suffixPos + result.suffixLength);

        this.prevWord = prefix + suffix;
        return this.prevWord;
    }

    /** Tries decoding an element within a group
     * @param result Contains information for decoding
     * @param posInGroup Index within group
     * @return Decoded element
     */
    public String decodeElement(FrontCodingResult result, int posInGroup) {
        String ret;
        if (posInGroup % maxElements == 0) {
            ret = decodeFirstElement(result);
        } else {
            ret = decodeAnyOtherElement(result);
        }
        return ret;
    }

    /** Returns the string backing this decoder */
    public String getString() {
        return string;
    }
}
