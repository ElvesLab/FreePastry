package stream.etl;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class CSVReader {

    public static String COMMA_DELIMITER = ",";

    public String filePath;
    public ExtractAndTransform eat;
    private BufferedReader reader;

    public CSVReader() {
        this.eat = new ExtractAndTransform();
    }
    
    public CSVReader filePath(String filePath) {
        this.filePath = filePath;
        return this;
    }

    public CSVReader rowKeyIdx(int rowKeyIdx) {
        eat.rowKeyIdx = rowKeyIdx;
        return this;
    }

    public CSVReader versionIdx(int versionIdx, boolean needTimestamp) {
        eat.versionIdx = versionIdx;
        eat.needTimestamp = needTimestamp;
        return this;
    }

    public CSVReader buildReader() {
        if (eat == null || filePath == null) {
            return null;
        }

        eat.build();

        try {
            reader = new BufferedReader(new FileReader(filePath));
            reader.readLine(); // skip the column header line
        } catch (IOException e) {
            System.err.println("Error in Reading " + filePath);
            e.printStackTrace();
        }

        return this;
    }

    public HashMap<String, String> readOneLine() throws IOException {
        String line = reader.readLine();
        if (line != null) {
            return eat.constructMap(line.split(COMMA_DELIMITER));
        }
        return null;
    }
}