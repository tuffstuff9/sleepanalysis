import java.io.IOException;
import java.io.StringReader;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvMalformedLineException;
import com.opencsv.exceptions.CsvValidationException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountLinesMapper
    extends Mapper<Object, Text, Text, IntWritable> {

  private final static IntWritable one = new IntWritable(1);

  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

    try {
      CSVReader reader = new CSVReader(new StringReader(value.toString()));
      String[] nextLine;

      while ((nextLine = reader.readNext()) != null) {
        if (!nextLine.toString().isEmpty()) {
          context.write(new Text("Total number of records:"), one);
        }
      }

    } catch (Exception e2) {
      context.write(new Text("Total number of records:"), one);
      // System.out.println(e2);
    }

  }
}