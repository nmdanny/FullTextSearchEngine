package webdata.spimi;

import webdata.Utils;
import webdata.dictionary.DictionaryBuilder;
import webdata.dictionary.SequentialDictionaryBuilder;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;

public class Merger {
    public static void merge(String dirPath, List<String> filePaths) throws IOException {
        List<InputStream> fileStreams = new ArrayList<>(filePaths.size());
        var indexIts = new PriorityQueue<IndexIterator>(
                filePaths.size(),
                Comparator.comparing(IndexIterator::getTerm)
        );
        try (var builder = new SequentialDictionaryBuilder(dirPath)){
            for (var filePath: filePaths) {
                var is = new BufferedInputStream(new FileInputStream(filePath));
                var iterator = new FileIndexIterator(is);
                fileStreams.add(is);
                indexIts.add(iterator);
            }
            merge(builder, indexIts);
        } finally {
            for (var stream: fileStreams) {
                stream.close();
            }
        }
    }


    /** Given a priority queue sorted by index terms, obtains a list of all index iterators with
     *  the minimal term - these iterators are pulled out of the queue, and should be re-added
     *  if we reset them for the next term)
     * @param queue Priority queue sorted by index term. Will be mutated.
     * @return List of iterators for posting lists with the minimal term
     */
    private static LinkedList<IndexIterator> getMinimalIndices(PriorityQueue<IndexIterator> queue) {
        return Utils.getMinElements(queue).collect(
                Collectors.toCollection(LinkedList::new)
        );

    }

    /** Given iterators to postings lists of different indices for the same term,
     *  merges them into the final dictionary's posting list.
     *  Drained iterators will be put back into the 'origQueue',
     */
    static void mergePostings(DictionaryBuilder builder,
                                      LinkedList<IndexIterator> iterators,
                                      PriorityQueue<IndexIterator> origQueue
                                      ) throws IOException {
        /* treating the iterators as lists of docID & freq pairs,
           we'll perform K-way merge on them

           It *is* possible for a term to appear in the same docID within different index files

           Since we probably don't have enough memory to load all `k` posting lists into memory,
           we will work as follows:

           every k iterations, add k elements to the heap from each of the
           different blocks,
         */

        var queue = new PriorityQueue<DocAndFreq>(iterators.size(), Comparator.comparing(DocAndFreq::getDocID));



        while (!iterators.isEmpty()) {

            var linkedListIt = iterators.listIterator();

            // Drain k iterators
            while (linkedListIt.hasNext()) {
                var postingIt = linkedListIt.next();
                if (!postingIt.hasNext()) {
                    // delete index-iterators which are drained for the current term
                    linkedListIt.previous();
                    linkedListIt.remove();
                    if (postingIt.moveToNextPostingList()) {
                        // if they have more terms(more posting lists), re-add them to the original queue
                        origQueue.add(postingIt);
                    }
                } else {
                    // Add the minimal docId & freq pair from each of the 'k' iterators
                    queue.add(postingIt.next());
                }
            }
            // get the minimal pair, combine the frequencies
            var optMinimal = Utils.getMinElements(queue)
                    .reduce((elm1, elm2) -> new DocAndFreq(elm1.getDocID(), elm1.getDocFrequency() + elm2.getDocFrequency()));
            if (optMinimal.isPresent()) {
                var minimal = optMinimal.get();
                builder.addTermOccurence(minimal.getDocID(), minimal.getDocFrequency());
            }
        }

        // after getting the minimal element, there might be more elements in the queue
        // which aren't minimal, but now that all iterators are drained, they are guaranteed to be minimal
        if (queue.isEmpty()) {
            return;
        }
        var minimal = Utils.getMinElements(queue)
                .reduce((elm1, elm2) -> new DocAndFreq(elm1.getDocID(), elm1.getDocFrequency() + elm2.getDocFrequency()))
                .get();
        builder.addTermOccurence(minimal.getDocID(), minimal.getDocFrequency());



    }

    /** Performs the merge step, creating the final dictionary */
    static void merge(DictionaryBuilder builder, PriorityQueue<IndexIterator> iterators) throws IOException {
        LinkedList<IndexIterator> minIndices;
        while (!(minIndices =  getMinimalIndices(iterators)).isEmpty()) {
            String term = minIndices.get(0).getTerm();
            builder.beginTerm(term);
            mergePostings(builder, minIndices, iterators);
        }
    }

}
