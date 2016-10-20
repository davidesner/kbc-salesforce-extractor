package keboola.salesforce.writer.config;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;

/**
 *
 * @author David Esner <esnerda at gmail.com>
 * @created 2015
 */
public class JsonConfigParser {

    public static KBCConfig parseFile(File file) throws IOException {
   		System.out.println( "KBC Config parseFile start");

        final ObjectMapper mapper = new ObjectMapper(new JsonFactory());
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.findAndRegisterModules();
   		System.out.println( "KBC Config parseFile end");
        return mapper.readValue(file, KBCConfig.class);
    }

    public static Object parseFile(File file, Class type) throws IOException {
   		System.out.println( "KBC Config parseFile object start");
        final ObjectMapper mapper = new ObjectMapper(new JsonFactory());
        mapper.findAndRegisterModules();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return mapper.readValue(file, type);
    }
}
