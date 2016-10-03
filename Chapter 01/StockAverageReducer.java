package hbp.chapt1;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StockAverageReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		double quoteAverage = 0;
		double quoteTotal = 0;
		int quoteCount = 0;
		for (DoubleWritable value : values) {
			quoteTotal += value.get();
			System.out.println("Reducer: " + key + " "+ quoteTotal);
			quoteCount++;
		}
		quoteAverage = quoteTotal/quoteCount;
		context.write(key, new DoubleWritable(quoteAverage));
	}

}
