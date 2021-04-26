package webdata.compression;

/** The result of encoding a string via (k-1)-in-k front coding, containing
 *  information that allows decoding it.
 */
public class FrontCodingResult {
    public final int suffixPos;
    public final int prefixLength;
    public final int suffixLength;

    /**
     * @param suffixPos    Byte position where suffix starts
     * @param prefixLength Length of the prefix
     * @param suffixLength Length of the suffix
     */
    public FrontCodingResult(int suffixPos, int prefixLength, int suffixLength) {
        this.suffixPos = suffixPos;
        this.prefixLength = prefixLength;
        this.suffixLength = suffixLength;
    }
}
