package webdata.dictionary;


import java.io.*;


/** A constant sized block containing BLOCK_SIZE elements */
public class DictionaryBlock {
    private static final int BLOCK_SIZE = 4;

    private byte numFilledElements;
    private FirstBlockElement firstBlockElement;
    private OtherBlockElement[] otherBlockElements;

    public DictionaryBlock() {
        this.numFilledElements = 0;
        this.otherBlockElements = new OtherBlockElement[BLOCK_SIZE - 1];
    }

    public boolean full() {
        return numFilledElements != BLOCK_SIZE;
    }

    /** Gets the next free dictionary element, and increments the filled element counter.
     *
     * @return
     */
    public DictionaryElement fillNewDictionaryElement(
            String term, int documentFrequency, int postingPtr
    ) {
        if (numFilledElements == BLOCK_SIZE) {
            throw new IllegalStateException("Cannot get a dictionary element as this block is full");
        }
        if (numFilledElements == 0) {
            return firstBlockElement;
        }
        return otherBlockElements[numFilledElements - 1];
    }


    public void serialize(DataOutputStream out) throws IOException {
        out.writeByte(numFilledElements);
        firstBlockElement.serialize(out);
        assert otherBlockElements.length == BLOCK_SIZE - 1;
        for (var block: otherBlockElements) {
            block.serialize(out);
        }
    }

    public static DictionaryBlock deserialize(DataInputStream in) throws IOException {
        var block = new DictionaryBlock();
        block.numFilledElements = in.readByte();
        block.firstBlockElement = FirstBlockElement.deserialize(in);
        for (int i=0; i < BLOCK_SIZE - 1; ++i) {
            block.otherBlockElements[i] = OtherBlockElement.deserialize(in);
        }
        return block;
    }

}
