import org.junit.runner.RunWith;

import java.util.ArrayList;

import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import com.pholser.junit.quickcheck.Property;

import java.io.IOException;
import java.util.ArrayList;


import static org.hamcrest.Matchers.*;
import static org.junit.Assume.assumeThat;

@RunWith(JUnitQuickcheck.class)
public class GroupVarintQuickcheck {

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
        GroupVarintEncoderTest.ensureCanEncodeAndDecodeIntegers(numbers);
    }
}
