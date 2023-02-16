package stream.etl;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
public class ExtractAndTransform {
    int rowKeyIdx;
    int versionIdx;
    boolean needTimestamp;
    DateFormat formatter;

    public ExtractAndTransform() {
    }

    public void build() {
        formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
    }

    public HashMap<String, String> constructMap(String[] values) {
        HashMap<String, String> data = new HashMap<String, String>();
        data.put("key", values[rowKeyIdx]);

        if (needTimestamp) {
            try {
                Date date = formatter.parse(values[versionIdx]);
                data.put("version", String.valueOf(new Timestamp(date.getTime()).getTime()));
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        else {
            data.put("version", values[versionIdx]);
        }

        String other_data_combination = "";
        for (int i = 0; i < values.length; i++) {
            if (i == rowKeyIdx || i == versionIdx) {
                continue;
            }
            other_data_combination += values[i] + CSVReader.COMMA_DELIMITER;
        }
        data.put("other", other_data_combination);
        return data;
    }
}
