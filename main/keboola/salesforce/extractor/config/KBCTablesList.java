/*
 */
package keboola.salesforce.extractor.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 *
 * @author David Esner <esnerda at gmail.com>
 * @created 2015
 */
public class KBCTablesList {

    @JsonProperty("tables")
    private List<KBCOutputMapping> tables;

    public KBCTablesList() {
    }

    public KBCTablesList(List<KBCOutputMapping> tables) {
        this.tables = tables;
    }

    public List<KBCOutputMapping> getTables() {
        return tables;
    }

    public void setTables(List<KBCOutputMapping> tables) {
        this.tables = tables;
    }
}
