import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		/**
		 * LongWritable, IntWritable, Text 均是 Hadoop 中实现的用于封装 Java 数据类型的类，
		 * 这些类实现了WritableComparable接口， 都能够被串行化从而便于在分布式环境中进行数据交换，
		 * 你可以将它们分别视为long,int,String 的替代品。
		 */
		// IntWritable one 相当于 java 原生类型 int 1
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// 每行记录都会调用 map 方法处理，此处是每行都被分词
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				// 输出每个词及其出现的次数 1，类似 <word1,1><word2,1><word1,1>
				context.write(word, one);
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
				InterruptedException {
			// key 相同的键值对会被分发到同一个 reduce中处理
			// 例如 <word1,<1,1>>在 reduce1 中处理，而<word2,<1>> 会在 reduce2 中处理
			int sum = 0;
			// 相同的key（单词）的出现次数会被 sum 累加
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			// 1个 reduce 处理完1 个键值对后，会输出其 key（单词）对应的结果（出现次数）
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		// 多队列hadoop集群中，设置使用的队列
		conf.set("mapred.job.queue.name", "regular");
		// 之所以此处不直接用 argv[1] 这样的，是为了排除掉运行时的集群属性参数，例如队列参数，
		// 得到用户输入的纯参数，如路径信息等
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		for (String argsStr : otherArgs) {
			System.out.println("-->> " + argsStr);
		}
		if (otherArgs.length < 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "word count");
		job.setJarByClass(WordCount.class);
		// map、reduce 输入输出类
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		// 输入输出路径
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		// 多子job的类中，可以保证各个子job串行执行
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}