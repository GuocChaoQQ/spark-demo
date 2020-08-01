package com.demo.sparkstreaming.rdd;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class AppendTextOutputFormat extends TextOutputFormat<Text, Text> {

    protected static class MyLineRecordWriter<K, V> implements RecordWriter<K, V> {
        private static final byte[] NEWLINE;
        protected DataOutputStream out;
        private final byte[] keyValueSeparator;

        public MyLineRecordWriter(DataOutputStream out, String keyValueSeparator) {
            this.out = out;
            this.keyValueSeparator = keyValueSeparator.getBytes(StandardCharsets.UTF_8);
        }

        public MyLineRecordWriter(DataOutputStream out) {
            this(out, "\t");
        }

        private void writeObject(Object o) throws IOException {
            if (o instanceof Text) {
                Text to = (Text)o;
                this.out.write(to.getBytes(), 0, to.getLength());
            } else {
                this.out.write(o.toString().getBytes(StandardCharsets.UTF_8));
            }

        }

        public synchronized void write(K key, V value) throws IOException {
            boolean nullKey = key == null || key instanceof NullWritable;
            boolean nullValue = value == null || value instanceof NullWritable;
            if (!nullKey || !nullValue) {
                /*if (!nullKey) {
                    this.writeObject(key);
                }

                if (!nullKey && !nullValue) {
                    this.out.write(this.keyValueSeparator);
                }*/
                if (!nullValue) {
                    this.writeObject(value);
                }

                this.out.write(NEWLINE);
            }
        }

        public synchronized void close(Reporter reporter) throws IOException {
            this.out.close();
        }

        static {
            NEWLINE = "\n".getBytes(StandardCharsets.UTF_8);
        }
    }

    @Override
    public RecordWriter getRecordWriter(FileSystem ignored, JobConf job, String name, Progressable progress) throws IOException {
        boolean isCompressed = getCompressOutput(job);
        String keyValueSeparator = job.get("mapreduce.output.textoutputformat.separator", "\t");
        if (!isCompressed) {
            Path file = FileOutputFormat.getTaskOutputPath(job, name);
            FileSystem fs = file.getFileSystem(job);
            Path newFile = new Path(FileOutputFormat.getOutputPath(job), name);
            FSDataOutputStream fileOut = null;
            if (fs.exists(newFile)) {
                //存在，追加写
                fileOut = fs.append(newFile);
            } else {
                fileOut = fs.create(file, progress);
            }
            return new AppendTextOutputFormat.MyLineRecordWriter(fileOut, keyValueSeparator);
        } else {
            Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(job, GzipCodec.class);
            CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, job);
            Path file = FileOutputFormat.getTaskOutputPath(job, name + codec.getDefaultExtension());
            FileSystem fs = file.getFileSystem(job);
            Path newFile = new Path(FileOutputFormat.getOutputPath(job), name);
            FSDataOutputStream fileOut = null;
            if (fs.exists(newFile)) {
                //存在，追加写
                fileOut = fs.append(newFile);
            } else {
                fileOut = fs.create(file, progress);
            }
            return new AppendTextOutputFormat.MyLineRecordWriter(new DataOutputStream(codec.createOutputStream(fileOut)), keyValueSeparator);
        }
    }
}