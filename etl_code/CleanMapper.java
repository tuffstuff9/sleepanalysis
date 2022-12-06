import java.io.IOException;
import java.io.StringReader;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvMalformedLineException;
import com.opencsv.exceptions.CsvValidationException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanMapper extends Mapper<Object, Text, Text, Text> {

  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

    try {
      CSVReader reader = new CSVReader(new StringReader(value.toString()));
      String[] line = reader.readNext();
      // value.toString().split(",");
      System.out.println(line[8]);

      if (line[8].equals("1")) {
        System.out.println("inside");
        String date = line[0];
        date = date.substring(0, 10);

        String bedtime = line[3];
        String waketime = line[4];
        String inBed = line[5];
        String fellAsleepIn = line[7];
        String asleep = line[9];
        String sleepAvg7 = line[10];
        String efficiency = line[11];
        String efficiencyAvg7 = line[12];
        String deep = line[15];
        String deepAvg7 = line[16];
        String sleepBMP = line[17];

        context.write(new Text(""), new Text(bedtime + "," + waketime + "," + inBed + "," + fellAsleepIn + "," +
            asleep + "," + sleepAvg7 + "," + efficiency + "," + efficiencyAvg7 + "," + deep + "," + deepAvg7 + ","
            + sleepBMP));

      }

    } catch (Exception e2) {
      // System.out.println(e2);
    }
  }
}