package data;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

// mapper进行分组
public class WCMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    Text k = new Text();
    IntWritable v = new IntWritable(1); //value的值出现一个赋一个1
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();  //1.将文本转化成字符串
        String[] words =  line.split("\\s+");  //2.将字符串切割
        for (String word : words) {  //3.将每一个单词写出
            k.set(word);
            context.write(k,v);
        }
    }
}


