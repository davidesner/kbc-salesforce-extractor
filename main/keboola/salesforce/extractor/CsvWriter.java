package keboola.salesforce.extractor;

import java.io.PrintWriter;
import java.io.Writer;

/**
 * CSV writer
 *
 * <p/>
 * User: mcheenath
 * Date: Nov 1, 2010
 */

public class CsvWriter {
    private PrintWriter writer;

    public CsvWriter( Writer w) {
        writer = new PrintWriter(w, true);
    }

    public void writeRecord(String[] values) {
        assert values != null;

        writeFirstField(values[0]);

        for (int i=1; i<values.length; i++) {
            writeField(values[i]);
        }

        endRecord();
    }

    public void endDocument() {
        writer.close();
    }

    public void endRecord() {
        writer.println();
    }

    public void writeField(String value) {
        writer.print(",");
        writeFirstField(value);
    }

    public void writeFirstField(String value) {
        if (value == null) {
            return;
        }

        writer.print(value);
    }
}