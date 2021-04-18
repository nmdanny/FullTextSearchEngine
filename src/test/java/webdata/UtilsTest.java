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
}