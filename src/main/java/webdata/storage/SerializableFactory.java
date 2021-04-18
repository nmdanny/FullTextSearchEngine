package webdata.storage;


import java.io.*;
import java.nio.ByteBuffer;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;

/**
 * Allows (de)serialization of some generic type
 */
public interface SerializableFactory<T> {
    /** If the serialized size of an element is fixed, returns it(and should not
     *  change between calls), otherwise, returns -1.
     */
    default int sizeBytes() {
        return -1;
    }

    /** Tries deserializing an element from given stream */
    T deserialize(DataInputStream dis) throws IOException;

    default T deserialize(ByteBuffer buf) throws IOException {
        if (!buf.hasArray()) {
            throw new UnsupportedOperationException("Default deserialize implementation requires ByteBuffer backed by array");
        }
        var dis = new DataInputStream(new ByteArrayInputStream(buf.array()));
        return deserialize(dis);
    }

    void serialize(T element, DataOutputStream dos) throws IOException;

    /**
     * Given a stream of serialized elements(with nothing else in-between), returns a spliterator
     * which deserializes them
     * @param dis Stream of serialized elements. NOT closed by this method.
     * @return A spliterator of deserialized elements
     */
    default Spliterator<T> deserializeStream(DataInputStream dis) {
        int cs = Spliterator.ORDERED;
        return new Spliterators.AbstractSpliterator<T>(Long.MAX_VALUE, cs) {
            @Override
            public boolean tryAdvance(Consumer<? super T> action) {
                try {
                    var res = deserialize(dis);
                    if (res != null) {
                        action.accept(res);
                        return true;
                    }
                    return false;
                } catch (EOFException e) {
                    return false;
                }
                catch (IOException e) {
                    throw new RuntimeException("IO error while deserializing stream", e);
                }
            }
        };
    }
}
