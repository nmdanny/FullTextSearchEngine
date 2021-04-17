package webdata;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.PriorityQueue;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.*;

class UtilsTest {
    @Test
    void canGetMinimalIndices() {

        var heap = Stream.of(1, 1, 1, 2, 2, 3)
                .collect(Collectors.toCollection(PriorityQueue::new));

        var minimals = Utils.getMinElements(heap).collect(Collectors.toList());
        assertIterableEquals(List.of(1, 1, 1), minimals);

        minimals = Utils.getMinElements(heap).collect(Collectors.toList());
        assertIterableEquals(List.of(2, 2), minimals);

        minimals = Utils.getMinElements(heap).collect(Collectors.toList());
        assertIterableEquals(List.of(3), minimals);


        minimals = Utils.getMinElements(heap).collect(Collectors.toList());
        assertTrue(minimals.isEmpty());

    }

    @Test
    void interleave() {
        var streams = List.of(
                List.of(0, 4, 8).spliterator(),
                List.of(1, 5, 9).spliterator(),
                List.of(2, 6, 10).spliterator(),
                List.of(3, 7).spliterator()
        );
        var interleavedSplit  = Utils.interleave(streams);
        var interleaved = StreamSupport.stream(interleavedSplit, false)
                .collect(Collectors.toList());

        assertIterableEquals(List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10), interleaved);
    }
}