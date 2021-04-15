package webdata.spimi;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import webdata.dictionary.DictionaryBuilder;

import java.io.IOException;
import java.util.*;

class InMemoryDictionaryBuilder implements DictionaryBuilder {
    private final Map<String, List<DocAndFreq>> dict;

    private int lastDocId;
    private String lastTerm;

    InMemoryDictionaryBuilder() {
        this.dict = new HashMap<>();
        this.lastDocId = 0;
        this.lastTerm = null;
    }

    @Override
    public void beginTerm(String term) {
        endTerm();
        assert lastTerm == null || term.compareTo(lastTerm) > 0;
        this.lastTerm = term;
        dict.put(term, new ArrayList<>());
    }

    @Override
    public void endTerm() {
        this.lastDocId = 0;
    }

    @Override
    public void addTermOccurence(int docId, int freqInDoc) {
        assert lastTerm != null;
        assert docId > lastDocId;
        assert freqInDoc > 0;
        dict.get(lastTerm).add(new DocAndFreq(docId, freqInDoc));
        this.lastDocId = docId;
    }



    @Override
    public void close() {
    }

    @Override
    public void flush() {

    }

    public Map<String, List<DocAndFreq>> getIndex() {
        return dict;
    }
}

class InMemoryIndexIterator implements IndexIterator {
    private final TreeMap<String, List<DocAndFreq>> index;


    InMemoryIndexIterator(TreeMap<String, List<DocAndFreq>> index) {
        this.index = index;
        var res = moveToNextPostingList();
        assert res;
    }

    String curDoc;
    int curDocFrequency;
    Iterator<DocAndFreq> curDocAndFreq;

    @Override
    public boolean moveToNextPostingList() {
        assert curDocAndFreq == null || !curDocAndFreq.hasNext();

        if (index.isEmpty()) {
            return false;
        }
        var entry = index.pollFirstEntry();
        curDoc = entry.getKey();
        var list = entry.getValue();
        assert !list.isEmpty();
        curDocFrequency = list.size();
        curDocAndFreq = list.iterator();
        return true;
    }

    @Override
    public int getDocumentFrequency() {
        return curDocFrequency;
    }

    @Override
    public String getTerm() {
        return curDoc;
    }

    @Override
    public boolean hasNext() {
        return curDocAndFreq.hasNext();
    }

    @Override
    public DocAndFreq next() {
        return curDocAndFreq.next();
    }
}

class MergerTest {

    @Test
    void mergePostings() throws IOException {
        var dictBuilder = new InMemoryDictionaryBuilder();
        var queue = new PriorityQueue<IndexIterator>(Comparator.comparing(IndexIterator::getTerm));

        var map1 = new TreeMap<String, List<DocAndFreq>>();
        map1.put("abc", List.of(
                new DocAndFreq(1, 3), new DocAndFreq(2, 5), new DocAndFreq(7, 9)
        ));

        map1.put("bb", List.of(
                new DocAndFreq(2, 4), new DocAndFreq(10, 15)
        ));

        queue.add(new InMemoryIndexIterator(map1));



        var map2 = new TreeMap<String, List<DocAndFreq>>();
        map2.put("abc", List.of(
                new DocAndFreq(2, 1), new DocAndFreq(8, 1)
        ));
        map2.put("abd", List.of(
                new DocAndFreq(3, 2)
        ));
        queue.add(new InMemoryIndexIterator(map2));



        var map3 = new TreeMap<String, List<DocAndFreq>>();
        map3.put("bb", List.of(
                new DocAndFreq(10, 5)
        ));
        queue.add(new InMemoryIndexIterator(map3));



        Merger.merge(dictBuilder, queue);


        var index = dictBuilder.getIndex();
        var abc = index.get("abc");
        assertIterableEquals(
                List.of(new DocAndFreq(1, 3), new DocAndFreq(2, 6), new DocAndFreq(7, 9), new DocAndFreq(8, 1)),
                abc
        );

        var abd =  index.get("abd");
        assertIterableEquals(
                List.of(new DocAndFreq(3, 2)),
                abd
        );

        var bb = index.get("bb");
        assertIterableEquals(
                List.of(new DocAndFreq(2, 4), new DocAndFreq(10, 20)),
                bb
        );
    }

    @Test
    void merge() {
    }
}