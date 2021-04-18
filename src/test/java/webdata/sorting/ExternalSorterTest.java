package webdata.sorting;

import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import webdata.Utils;
import webdata.storage.IntSerializableFactory;
import webdata.storage.SerializableFactory;

import java.io.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.*;

@RunWith(JUnitQuickcheck.class)
public class ExternalSorterTest {

    @Property
    public void externalSort(ArrayList<Integer> integers) throws IOException {

        for (int blockSize = 1; blockSize <= integers.size() * 2; blockSize *= 2) {
            var dir = Files.createTempDirectory("externalSort-blsize-" + blockSize + "-");
            var fac = new IntSerializableFactory();
            Comparator<Integer> cmp = Comparator.naturalOrder();
            int finalBlockSize = blockSize;
            var sorter = new ExternalSorter<Integer>(fac, cmp, dir) {
                @Override
                protected boolean hasMemory(ArrayList<Integer> curBlock) {
                    return curBlock.size() < finalBlockSize;
                }
            };

            var expected = integers.stream().sorted().collect(Collectors.toList());
            var dis = elementsToDis(integers, fac);

            var baos = new ByteArrayOutputStream();
            var dos = new DataOutputStream(baos);

            sorter.externalSort(dis, dos);

            var gottenSplit = fac.deserializeStream(new DataInputStream(new ByteArrayInputStream(baos.toByteArray())));
            var gotten = StreamSupport.stream(gottenSplit, false).collect(Collectors.toList());

            assertIterableEquals(expected, gotten);
            Utils.deleteDirectory(dir);
        }

    }

    private static <T> DataInputStream elementsToDis(Collection<T> elements, SerializableFactory<T> fac) throws IOException {
        var baos = new ByteArrayOutputStream();
        var dos = new DataOutputStream(baos);
        for (var elem: elements) {
            fac.serialize(elem, dos);
        }
        return new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    }
}