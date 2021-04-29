package webdata.compression;

import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(JUnitQuickcheck.class)
public class VarintQuickcheck {

    @Property
    public void canEncodeAndDecodeQuickcheckNumbers(ArrayList<Integer> numbers) throws IOException {

        for (int i=0; i < numbers.size(); ++i) {
            int num = numbers.get(i);
            if (num == 0) {
                num++;
            } else if (num < 0) {
                num = -num;
            }
            numbers.set(i, num);
        }
        testNumbers(numbers);
    }

    static void testNumbers(List<Integer> numbers) throws IOException {
        var os = new ByteArrayOutputStream();
        for (var num: numbers) {
            Varint.encode(os, num);
        }
        var is = new ByteArrayInputStream(os.toByteArray());
        for (var expectedNum: numbers) {
            var res = Varint.decode(is);
            assertEquals((int)expectedNum, res);
        }
    }

    @Test
    public void simpleTest() throws IOException {
        testNumbers(List.of(128));
    }
}
