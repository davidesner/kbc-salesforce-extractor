/*
 */
package keboola.salesforce.extractor.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author David Esner <esnerda at gmail.com>
 * @updated Martin Humpolec <kbc at htns.cz>
 * @created 2015
 */
public class KBCParameters {

    private final static String[] REQUIRED_FIELDS = {"loginname", "password", "securitytoken", "object", "soql"};
    private final Map<String, Object> parametersMap;

    @JsonProperty("loginname")
    private String loginname;
    @JsonProperty("#password")
    private String password;
    @JsonProperty("#securitytoken")
    private String securitytoken;
    @JsonProperty("sandbox")
    private Boolean sandbox;
    @JsonProperty("object")
    private List<String> object;
    @JsonProperty("objects")
    private List<List<String>> objects;
    @JsonProperty("soql")
    private List<String> soql;
    
    public KBCParameters() {
        parametersMap = new HashMap();

    }

    @JsonCreator
    public KBCParameters(@JsonProperty("loginname") String loginname, @JsonProperty("#password") String password,
            @JsonProperty("#securitytoken") String securitytoken, @JsonProperty("object") List<String> object, @JsonProperty("objects")
            private List<List<String>> objects; @JsonProperty( "soql") List<String> soql
    ) throws ParseException {
        parametersMap = new HashMap();
        this.loginname = loginname;
        this.password = password;
        this.securitytoken = securitytoken;
        this.sandbox = sandbox;
        this.object = object;
        this.objects = objects;
        this.soql = soql;

        //set param map
        parametersMap.put("loginname", loginname);
        parametersMap.put("password", password);
        parametersMap.put("securitytoken", securitytoken);
        parametersMap.put("sandbox", sandbox);
        parametersMap.put("object", object);
        parametersMap.put("objects", objects)
        parametersMap.put("soql", soql);

    }

    /**
     * Returns list of required fields missing in config
     *
     * @return
     */
    private List<String> getMissingFields() {
        List<String> missing = new ArrayList<String>();
        for (int i = 0; i < REQUIRED_FIELDS.length; i++) {
            Object value = parametersMap.get(REQUIRED_FIELDS[i]);
            if (value == null) {
                missing.add(REQUIRED_FIELDS[i]);
            }
        }
        
        if (missing.isEmpty()) {
            return null;
        }
        return missing;
    }

    private String missingFieldsMessage() {
        List<String> missingFields = getMissingFields();
        String msg = "";
        if (missingFields != null && missingFields.size() > 0) {
            msg = "Required config fields are missing: ";
            int i = 0;
            for (String fld : missingFields) {
                if (i < missingFields.size()) {
                    msg += fld + ", ";
                } else {
                    msg += fld;
                }
            }
        }
        return msg;
    }

    public boolean validateParametres() throws ValidationException {
        //validate date format
        String error = "";

        error += missingFieldsMessage();

        if (error.equals("")) {
            return true;
        } else {

            throw new ValidationException("Validation error: " + error);
        }
    }

    public String getLoginname() {
        return loginname;
    }

    public void setLoginname(String loginname) {
        this.loginname = loginname;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getSecuritytoken() {
        return securitytoken;
    }

    public void setSecuritytoken(String securitytoken) {
        this.securitytoken = securitytoken;
    }

    public boolean getSandbox() {
        return sandbox;
    }

    public void setSandbox(boolean sandbox) {
        this.sandbox = sandbox;
    }

    public List<String> getObject() {
        return object;
    }

    public void setObject(List<String> object) {
        this.object = object;
    }

    public List<List<String>> getObjects() {
    	return objects;
    }
    
    public void setObjects(List<List<String>> objects) {
    	this.objects = objects;
    }
    
    public List<String> getSOQL() {
        return soql;
    }

    public void setSOQL(List<String> soql) {
        this.soql = soql;
    }
}
