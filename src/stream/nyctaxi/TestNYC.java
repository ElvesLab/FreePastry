package stream.nyctaxi;

import java.io.IOException;
import java.util.HashMap;

import stream.etl.CSVReader;

public class TestNYC {
    public static void main(String[] args) {
        String filePath = "/home/parallels/Desktop/taxi_csvs/test.csv";
        CSVReader reader = new CSVReader()
                        .filePath(filePath)
                        .rowKeyIdx(0)
                        .versionIdx(3, true)
                        .buildReader();
        

        if (reader == null) {
            System.err.println("Error in intilizaling reader");
        }

        HashMap<String, String> data;
        try {
            data = reader.readOneLine();
            while (data != null) {
                System.out.println(data);
                break;
                // get node in pastry, and insert the data
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
