/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.custom;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.json.JSONArray;
import org.json.JSONObject;

import org.apache.nifi.processor.io.StreamCallback;
import java.nio.charset.Charset;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.io.IOException;
import org.apache.commons.io.IOUtils;
import java.io.ByteArrayOutputStream;


import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.fasterxml.jackson.core.JsonProcessingException;

@Tags({"example"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class GetAttendance extends AbstractProcessor {

    public static final PropertyDescriptor MY_PROPERTY = new PropertyDescriptor
            .Builder().name("MY_PROPERTY")
            .displayName("My property")
            .description("Example Property")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship Success = new Relationship.Builder()
            .name("Succcess")
            .description("Succcess")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(Success);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }
    @Override
	  public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {  
    	FlowFile flowFile = session.get(); 
    	//flow file to string
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        session.exportTo(flowFile, bytes);
        final String alldata = bytes.toString();
        //
    	CassandraConnector client = new CassandraConnector();
    	JSONArray returnData = new JSONArray();
    	client.connect("HOST", 9042);
    	Session Session;
   	    Session = client.getSession();
   	    JSONObject obj = new JSONObject(alldata);
   	    try {
			    	JSONObject obj2 = null;
			    	JSONArray data =  obj.getJSONArray("data");
			    	System.out.println(data.toString());
			    	for(int i = 0;i<=data.length()-1;i++) {
		//	    		System.out.println(data.getJSONObject(i).toString());
			    		String Query = "SELECT json  companydetails FROM attendance.company where companyid ="+data.getJSONObject(i).getString("companyid")+" allow filtering;";
			    		System.out.println("==> "+Query);
			    		ResultSet result = 
					    	      Session.execute(Query);
				    	Iterator<Row> test =  result.iterator();
				    	while(test.hasNext()) {
				    		obj2 = new JSONObject(result.one().getString(0));
		//		 	    	System.out.println(obj2.getJSONObject("employeedetails").getString("username"));
				    		data.getJSONObject(i).put("companynameDyn", obj2.getJSONObject("companydetails").getString("companyname"));
				    	 }
			    		
			    	}
	    	
	   		    client.close();
	   	    	}catch(Exception e) {
	   	    	client.close();
   	    	}
	        
	        String Returndata =  obj.toString();
	        // Creates the flowfile  
	        flowFile = session.write(flowFile, new StreamCallback() {  
			  @Override  
			  public void process(InputStream inputStream, OutputStream outputStream) throws IOException {  
				IOUtils.write(Returndata , outputStream); // writes the result to the flowfile.  
			  }  
	        }); 
		  //session.write(flowFile, osc)  
		  if (flowFile == null) {  
			  return;  
		  }  
		  session.transfer(flowFile,Success); // Transfer the output flowfile to success relationship.  
	  }
    
	public static class CassandraConnector {

	    private Cluster cluster;

	    private Session Session;

	    public void connect(String node, Integer port) {
	        Builder b = Cluster.builder().addContactPoint(node);
	        if (port != null) {
	            b.withPort(port);
	        }
	        cluster = b.build();

	        Session = cluster.connect();
	    }

	    public Session getSession() {
	        return this.Session;
	    }

	    public void close() {
	        Session.close();
	        cluster.close();
	        System.out.println("Cassandra client closed !!");
	    }
	}
	
}
