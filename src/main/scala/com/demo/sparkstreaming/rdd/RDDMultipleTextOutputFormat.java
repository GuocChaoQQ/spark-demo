package com.demo.sparkstreaming.rdd;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.util.Progressable;

import java.io.File;
import java.io.IOException;

public class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat<String, String> {
    private AppendTextOutputFormat theTextOutputFormat = null;
    public String generateFileNameForKeyValue(String key, String value, String name) {

        String[] s = key.split("_"); // database_tablename
        if(s.length>=2){
            return s[0]+ File.separator+s[1] +File.separator+name; // database/tablename/
        }else{
            return  s[0]+ File.separator+s[1] +name; // database/
        }

    }
    @Override
    protected RecordWriter getBaseRecordWriter(FileSystem fs, JobConf job, String name, Progressable progressable) throws IOException {
        if (this.theTextOutputFormat == null) {
            this.theTextOutputFormat = new AppendTextOutputFormat();
        }
        return this.theTextOutputFormat.getRecordWriter(fs, job, name, progressable);
    }
}