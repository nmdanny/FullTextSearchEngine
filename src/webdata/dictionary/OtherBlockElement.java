package webdata.dictionary;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.CharBuffer;

/**
 * Represents a block element which isn't the first
 */
class OtherBlockElement implements DictionaryElement {
    Dictionary dictionary;
    DictionaryBlock dictionaryBlock;
    int indexWithinBlock;

    int frequency;
    int postingPtr;
    int termLength;

    public OtherBlockElement(Dictionary dictionary, DictionaryBlock dictionaryBlock, int indexWithinBlock,
                             int frequency, int postingPtr, int termLength) {
        this.dictionary = dictionary;
        this.dictionaryBlock = dictionaryBlock;
        this.indexWithinBlock = indexWithinBlock;
        this.frequency = frequency;
        this.postingPtr = postingPtr;
        this.termLength = termLength;
    }

    public static OtherBlockElement NullElement(Dictionary dictionary, DictionaryBlock dictionaryBlock, int indexWithinBlock) {
        return new OtherBlockElement(dictionary, dictionaryBlock, indexWithinBlock, -1, -1, -1);
    }

    @Override
    public void setDictionary(Dictionary dictionary) {
        this.dictionary = dictionary;
    }


    @Override
    public CharBuffer getTerm() {
        var firstBlockElement = dictionaryBlock.firstBlockElement;
        int termPointer = firstBlockElement.getTermPointer() + firstBlockElement.termLength;
        for (int i=1; i < indexWithinBlock; ++i) {
            termPointer += dictionaryBlock.otherBlockElements[i-1].termLength;
        }
        return dictionary.derefTermPointer(termPointer, termLength);
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
    }

    public static OtherBlockElement deserialize(DataInputStream in, Dictionary dictionary,
                                                DictionaryBlock dictionaryBlock, int indexWithinBlock) throws IOException {
        int frequency = in.readInt();
        int postingPtr = in.readInt();
        int termLength= in.readInt();
        return new OtherBlockElement(dictionary, dictionaryBlock, indexWithinBlock, frequency, postingPtr, termLength);
    }
}
