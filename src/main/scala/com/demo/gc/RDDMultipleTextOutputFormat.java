package com.demo.gc;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;

public class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat<String, String> {
    private AppendTextOutputFormat theTextOutputFormat = null;
    public String generateFileNameForKeyValue(String key, String value, String name) {
        //输出格式 /ouput/key/key.csv
        //key  2020-02-06   value:data  name:part-00000
//      System.out.println(key + "/"+name);
        return key + "/"+name;
    }
    @Override
    protected RecordWriter getBaseRecordWriter(FileSystem fs, JobConf job, String name, Progressable progressable) throws IOException {
        if (this.theTextOutputFormat == null) {
            this.theTextOutputFormat = new AppendTextOutputFormat();
        }
        return this.theTextOutputFormat.getRecordWriter(fs, job, name, progressable);
    }
}