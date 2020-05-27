package org.apache.rocketmq.example.quickstart;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author youyu
 * @date 2020/5/18 2:42 PM
 */
public class MappedFileTest {
    public static void main(String[] args) throws IOException {
        File file = new File("/Users/jam/company/code/rocketmq/conf/store/consumequeue/TopicTest/0/00000000000000000000");
        FileChannel rw = new RandomAccessFile(file, "rw").getChannel();
        MappedByteBuffer map = rw.map(FileChannel.MapMode.READ_WRITE, 0, 1024 * 64);
        ByteBuffer slice = map.slice();
    }
}
