package webdata.dictionary;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.AbstractList;
import java.util.ArrayList;

/** An array of dictionary elements, very wasteful in terms of memory. Only
 *  for debugging/measuring purposes */
class UnpackedDictionaryElements extends AbstractList<DictionaryElement> {

    private final ArrayList<DictionaryElement> elements;

    UnpackedDictionaryElements(DataInputStream dis, int numElements) throws IOException {
        elements = new ArrayList<>(numElements);
        for (int elementNum = 0; elementNum < numElements; ++elementNum) {
            if (elementNum % Dictionary.BLOCK_SIZE == 0) {
                elements.add(FirstBlockElement.deserialize(dis));
            } else {
                elements.add(OtherBlockElement.deserialize(dis));
            }
        }
    }

    @Override
    public DictionaryElement get(int index) {
        return elements.get(index);
    }

    @Override
    public int size() {
        return elements.size();
    }
}
