package com.deppon.hadoop.sqoopx.core.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Created by meepai on 2017/6/29.
 */
public class RawKeyTextOuputFormat<K, V> extends FileOutputFormat<K, V> {

    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        boolean isCompressed = getCompressOutput(job);
        Configuration conf = job.getConfiguration();
        String ext = "";
        CompressionCodec codec = null;
        if(isCompressed){
            Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(job, GzipCodec.class);
            codec = ReflectionUtils.newInstance(codecClass, conf);
            ext = codec.getDefaultExtension();
        }
        Path file = getDefaultWorkFile(job, ext);
        FileSystem fs = file.getFileSystem(conf);
        FSDataOutputStream fout = fs.create(file, false);
        DataOutputStream ostream = fout;
        if(isCompressed){
            ostream = new DataOutputStream(codec.createOutputStream(fout));
        }
        return new RawKeyRecordWriter<K, V>(ostream);
    }

    public static class RawKeyRecordWriter<K, V> extends RecordWriter<K, V> {

        protected DataOutputStream out;

        public RawKeyRecordWriter(DataOutputStream out){
            this.out = out;
        }

        @Override
        public void write(K key, V value) throws IOException, InterruptedException {
            writeObject(key);
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            out.close();
        }

        private void writeObject(Object o) throws IOException {
            if(o instanceof Text){
                out.write(((Text)o).getBytes(), 0, ((Text)o).getLength());
            } else {
                out.write(o.toString().getBytes("UTF-8"));
            }
        }
    }
}
