package kafkaProducer;

import com.opencsv.CSVWriter;
import utils.Config;

import java.io.FileWriter;
import java.io.IOException;

public class GenerateCsv {


    public static void main(String[] args) {
        CSVWriter writer = null;
        //DateTimeFormatter dtf = DateTimeFormat.forPattern("MM/dd/yyyy HH:mm:ss");
        try {
            writer = new CSVWriter(new FileWriter(Config.filename));

            // genera ogni 5 secondi un tweet
            for (int i = 0 ; i<20;i++){
                String[] line = new String[3];
                line[0] = String.valueOf(System.currentTimeMillis());
                //System.out.println(date);
                String comment= "prova"+i;
                line[1] = String.valueOf(i);
                line[2] = comment;
                writer.writeNext(line);
                Thread.sleep(5000);
            }
            writer.close();

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }


    }
}
