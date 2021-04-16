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

/** Contains methods for merging temporary index files.
 *
 *   <p>
 *      In essence, we perform 2 levels of an algorithm similar to K-way-merge, but unlike
 *      the traditional K-way merge, we have a combining behavior when there are multiple minimal
 *      elements(rather than just choosing a minimal element arbitrarily and pushing it to the output)
 *   </p>
 *   <ol>
 *       <li>
 *           In the first level, we merge {@link IndexIterator}s ordered by the term of the posting list they're
 *           currently pointing at.
 *
 *           If multiple index iterators are pointing to the same term, all those iterators are combined
 *           together in the second level via {@link #mergePostings(DictionaryBuilder, LinkedList, PriorityQueue)}
 *       </li>
 *       <li>
 *           In the second level, we merge IndexIterators(now viewing them as an iterator of {@link DocAndFreq})
 *           ordered by the docID, each time taking the element(s) with the minimal docID. If there are multiple
 *           pairs with the same docID, their frequency within document will be summed together.
 *       </li>
 *   </ol>
 *
 * */
public class Merger {
    /** Merges given index files into a dictionary given path
     * @param dirPath Directory containing files of final dictionary
     * @param filePaths Paths of temporary index files, in no particular order
     */
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
     *  if we reset them for the next term.
     * @param queue Priority queue sorted by index term. Will be mutated.
     * @return List of iterators for posting lists with the minimal term
     */
    private static LinkedList<IndexIterator> getMinimalIndexIterators(PriorityQueue<IndexIterator> queue) {
        return Utils.getMinElements(queue).collect(
                Collectors.toCollection(LinkedList::new)
        );

    }

    /**
     * Given a min heap containing posting list elements ordered by docIDs, which contains
     * all minimal docIDs and possibly more, merges the minimal ones and adds them as a single
     * posting-list element to the final dictionary.
     *
     * @param builder Used to build final dictionary
     * @param queue Contains posting list elements ordered by docIDs, AND
     *              contains all elements with the minimal remaining docID.
     */
    private static void mergeMinimalPostingListElement(DictionaryBuilder builder,
                                                       PriorityQueue<DocAndFreq> queue) throws IOException {
        var minimal = Utils.getMinElements(queue)
                .reduce((elm1, elm2) -> new DocAndFreq(elm1.getDocID(), elm1.getDocFrequency() + elm2.getDocFrequency()))
                .get();
        builder.addTermOccurence(minimal.getDocID(), minimal.getDocFrequency());
    }

    /** Given iterators to posting lists of different temporary indices for the same term,
     *  merges them into the final dictionary. Drained iterators are put back into the queue.
     * @param builder Builder for the final dictionary
     * @param iterators IndexIterators pointing to the same term
     * @param origQueue Contains IndexIterators pointing to later terms.
     */
    static void mergePostings(DictionaryBuilder builder,
                                      LinkedList<IndexIterator> iterators,
                                      PriorityQueue<IndexIterator> origQueue
                                      ) throws IOException {
        /* Treating the iterators as lists of docID & freq pairs,
           we'll perform a variation of K-way merge on them, where if we have multiple
           minimal elements, we'll combine them into a single posting-list element and
           add up their frequencies.

           Since we probably don't have enough memory to load all `k` posting lists into memory,
           we will work as follows:

           1. Load 1 posting-list element from each of the available index iterators, and add them to a
             min heap ordered by docID.

             If an index-iterator has no more elements in its current posting list, delete it from
             the 'iterators' linked list, try advancing it to the next posting list, and if it indeed
             has another posting list, re-add it to 'origQueue' (the one ordered by terms)

           2. Now the queue has up to 'k' elements, take all minimal elements(there might be between 1 to 'k' of them)
             and combine them, summing their frequency within the document.

           3. Repeat 1-2 so long we still have index iterators for the same term

           4. Repeat 2 up to 'k-1' time to deal with leftover elements
         */

        var queue = new PriorityQueue<DocAndFreq>(iterators.size(), Comparator.comparing(DocAndFreq::getDocID));


        String term = iterators.getFirst().getTerm();

        while (!iterators.isEmpty()) {
            var linkedListIt = iterators.listIterator();

            while (linkedListIt.hasNext()) {
                // Drain 1 docID-freq element from each of the index iterators
                var postingIt = linkedListIt.next();
                assert postingIt.getTerm().equals(term);

                if (!postingIt.hasNext()) {
                    // In case there are no more elements(for the current term) in that iterator,
                    // remove it from the linked list.
                    linkedListIt.previous();
                    linkedListIt.remove();
                    if (postingIt.moveToNextPostingList()) {
                        // if they have more terms(more posting lists),
                        // re-add them to the original queue
                        origQueue.add(postingIt);
                    }
                } else {
                    // Add the minimal docId & freq pair from each of the 'k' iterators
                    queue.add(postingIt.next());
                }
            }
            if (!queue.isEmpty()) {
                mergeMinimalPostingListElement(builder, queue);
            }
        }

        // After having drained all iterators for the current term and added up to 1 element, we might
        // have up to 'k-1' leftover docID-freq pairs(corresponding to up to 'k-1' elements in
        // the final dictionary), so merge them as well.
        while (!queue.isEmpty()) {
            mergeMinimalPostingListElement(builder, queue);
        }
    }

    /** Performs the merge step, creating the final dictionary
     * @param builder Used for building the final dictionary
     * @param iterators A heap of index iterators ordered by their current term
     * */
    static void merge(DictionaryBuilder builder, PriorityQueue<IndexIterator> iterators) throws IOException {
        LinkedList<IndexIterator> minIterators;
        while (!(minIterators =  getMinimalIndexIterators(iterators)).isEmpty()) {
            // minIterators contains all iterators
            // to indices with the same minimal term
            String term = minIterators.get(0).getTerm();
            builder.beginTerm(term);
            mergePostings(builder, minIterators, iterators);
        }
    }

}
