package webdata.compression;

/** The result of encoding a string via (k-1)-in-k front coding, containing
 *  information that allows decoding it.
 */
public class FrontCodingResult {
    public final long suffixPos;
    public final int prefixLengthChars;
    public final int suffixLengthBytes;

    /**
     * @param suffixPos    Byte position where suffix starts
     * @param prefixLengthChars Length of the prefix (in chars!)
     * @param suffixLengthBytes Length of the suffix (in bytes)
     */
    public FrontCodingResult(long suffixPos, int prefixLengthChars, int suffixLengthBytes) {
        this.suffixPos = suffixPos;
        this.prefixLengthChars = prefixLengthChars;
        this.suffixLengthBytes = suffixLengthBytes;
    }
}
