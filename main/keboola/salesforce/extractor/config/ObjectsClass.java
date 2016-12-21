package keboola.salesforce.extractor.config;
import java.util.List;

public class ObjectsClass {
	private String name;
	private String soql;
	
    public String getName() {
        return name;
    }

    public void setName(String objectname) {
        this.name = objectname;
    }
    public String getSoql() {
        return soql;
    }

    public void setSoql(String objectsoql) {
        this.soql = objectsoql;
    }
	
}