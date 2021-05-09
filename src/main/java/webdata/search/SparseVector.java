package webdata.search;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;


/** A sparse vector keyed by strings(terms), all other tokens in the universe(corpus+query)
 *  that aren't in this vector are considered to have a value of 0.
 */
public class SparseVector {
    private final Map<String, Double> elements;

    public SparseVector(Map<String, Double> elements) {
        this.elements = Collections.unmodifiableMap(elements);
    }

    public SparseVector cosNormalized() {
        double norm = cosNorm();
        return new SparseVector(
                elements.entrySet()
                        .stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey, entry -> entry.getValue() / norm
                        ))
        );
    }

    public double cosNorm() {
        double sum = 0;
        for (var elm: elements.values()) {
            sum += elm * elm;
        }
        return Math.sqrt(sum);
    }

    public SparseVector multiply(SparseVector other) {
        HashMap<String, Double> result = new HashMap<>();
        for (var entry: elements.entrySet()) {
            String key = entry.getKey();
            if (other.elements().containsKey(key)) {
                result.put(key, entry.getValue() * other.elements().get(key));
            }
        }
        return new SparseVector(result);
    }

    public double dot(SparseVector other) {
        double sum = 0.0;
        for (var entry: elements.entrySet()) {
            String key = entry.getKey();
            if (other.elements().containsKey(key)) {
                sum += entry.getValue() * other.elements.get(key);
            }
        }
        return sum;
    }

    public Map<String, Double> elements() {
        return elements;
    }

}
