package data;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * KEYIN:reduce端输入的Key类型，即map端输出的key类型
 * VALEINP:reduce端输入的value类型，即map端输出的value类型
 * KEYOUT：reduce端输出的key类型
 * VALUEOUT：reduce端输出的value类型
 */
// reduce进行聚合
public class WCReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
    int sum ;
    IntWritable v =  new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //reduce端接收到的类型大概是这样的  (wish,1)
        sum = 0;
        //对迭代器进行累加求和
        for (IntWritable count : values) {
            sum += count.get();
        }
        //将key和value进行写出
        v.set(sum);
        context.write(key,v);
    }
}
