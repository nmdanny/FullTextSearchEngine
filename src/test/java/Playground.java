import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Scanner;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.atomic.AtomicLong;

public class Playground {
    @Test
    void mmapTest() throws IOException, InterruptedException {
        String path = "E:\\junk";

        int numMaps = 30;
        int mapSize = 1024 * 1024 * 1024;

        var pool = ForkJoinPool.commonPool();
        var tasks = new ConcurrentLinkedQueue<ForkJoinTask<?>>();

        var curPos = new AtomicLong(0);

        try (var raf = new RandomAccessFile(path, "rw"); var chan = raf.getChannel()) {
            for (int mapIx = 0; mapIx < numMaps; ++mapIx)
            {
                var task =  pool.submit(() -> {
                    var pos = curPos.getAndAdd(mapSize);
                    try {
                        var map = chan.map(FileChannel.MapMode.READ_WRITE, pos, Integer.MAX_VALUE);
                        for (int i=0; i < map.capacity(); ++i)
                        {
                            map.put((byte)i);
                        }
                    } catch (IOException ex) {
                        System.err.format("Couldn't create map index: %s ", ex);
                    }
                });
                tasks.add(task);
            }

            for (var task : tasks) {
                task.join();
            }

            var scanner = new Scanner(System.in);
            while (true) {
                System.out.println("Done, type 'exit' to quit");
                if (scanner.nextLine().equalsIgnoreCase("exit")) {
                    break;
                }
            }
        }

    }
}
