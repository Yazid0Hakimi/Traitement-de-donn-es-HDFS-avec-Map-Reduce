package org.example.webServer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LogMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(" ");
        if (tokens.length >= 8) {
            String ipAddress = tokens[0];
            String httpStatus = tokens[8];
            if (httpStatus.equals("200")) {
                context.write(new Text(ipAddress), new IntWritable(1));
            }
        }
    }
}
