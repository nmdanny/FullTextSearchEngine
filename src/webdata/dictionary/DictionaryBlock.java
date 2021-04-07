package webdata.dictionary;


import java.io.*;
import java.util.Arrays;
import java.util.stream.Stream;


/** A constant sized block containing BLOCK_SIZE elements */
public class DictionaryBlock {
    private static final int BLOCK_SIZE = 4;

    private final Dictionary dictionary;
    private byte numFilledElements;

    FirstBlockElement firstBlockElement;
    final OtherBlockElement[] otherBlockElements;

    public DictionaryBlock(Dictionary dictionary) {
        this.dictionary = dictionary;
        this.numFilledElements = 0;
        this.firstBlockElement = FirstBlockElement.NullElement(dictionary);
        this.firstBlockElement.dictionary = dictionary;
        this.otherBlockElements = new OtherBlockElement[BLOCK_SIZE - 1];
        for (int i=0; i < this.otherBlockElements.length; ++i) {
            this.otherBlockElements[i] = OtherBlockElement.NullElement(dictionary, this, i+1);
        }
    }

    public boolean full() {
        return numFilledElements == BLOCK_SIZE;
    }

    /** Gets the next free dictionary element, and increments the filled element counter.
     *
     * @return The element which was just filled
     */
    public DictionaryElement fillNewDictionaryElement(
            int termPointer, int termLength, int documentFrequency, int postingPtr
    ) {
        if (numFilledElements == BLOCK_SIZE) {
            throw new IllegalStateException("Cannot get a dictionary element as this block is full");
        }
        if (numFilledElements == 0) {
            firstBlockElement.dictionary = dictionary;
            firstBlockElement.termPointer = termPointer;
            firstBlockElement.termLength = termLength;
            firstBlockElement.frequency = documentFrequency;
            firstBlockElement.postingPtr = postingPtr;
            ++numFilledElements;
            return firstBlockElement;
        }
        var block = otherBlockElements[numFilledElements - 1];
        block.dictionary = dictionary;
        block.termLength = termLength;
        block.frequency = documentFrequency;
        block.postingPtr = postingPtr;
        ++numFilledElements;
        return block;
    }

    /** Returns a stream over all filled dictionary elements */
    public Stream<DictionaryElement> stream() {
        if (numFilledElements == 0) {
            return Stream.empty();
        }
        return Stream.concat(Stream.of(firstBlockElement), Arrays.stream(otherBlockElements).limit(numFilledElements - 1));
    }

    public void serialize(DataOutputStream out) throws IOException {
        out.writeByte(numFilledElements);
        firstBlockElement.serialize(out);
        assert otherBlockElements.length == BLOCK_SIZE - 1;
        for (var block: otherBlockElements) {
            block.serialize(out);
        }
    }

    public static DictionaryBlock deserialize(DataInputStream in, Dictionary dictionary) throws IOException {
        var block = new DictionaryBlock(dictionary);
        block.numFilledElements = in.readByte();
        block.firstBlockElement = FirstBlockElement.deserialize(in, dictionary);
        for (int i=0; i < BLOCK_SIZE - 1; ++i) {
            block.otherBlockElements[i] = OtherBlockElement.deserialize(in, dictionary, block, i + 1);
        }
        return block;
    }

}
