package com.sohan.student.listener;

import io.debezium.config.Configuration;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;

import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.stereotype.Component;

import com.sohan.student.PubDemo;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * This class creates, starts and stops the EmbeddedEngine, which starts the Debezium engine. The engine also
 * loads and launches the connectors setup in the configuration.
 * <p>
 * The class uses @PostConstruct and @PreDestroy functions to perform needed operations.
 *
 * @author Sohan
 */

@Component
public class CDCListener {

    /**
     * Single thread pool which will run the Debezium engine asynchronously.
     */
    private final Executor executor = Executors.newSingleThreadExecutor();

    /**
     * The Debezium engine which needs to be loaded with the configurations, Started and Stopped - for the
     * CDC to work.
     */
    // private final EmbeddedEngine engine;
    private final DebeziumEngine<ChangeEvent<String, String>> engine;

   
    

    /**
     * Constructor which loads the configurations and sets a callback method 'handleEvent', which is invoked when
     * a DataBase transactional operation is performed.
     *
     * @param studentConnector
     *
     */
    private CDCListener(Configuration studentConnector) {
        PubDemo pb = new PubDemo();
        // List<String> messagesList;
        Map<String, String> messages = new LinkedHashMap<String, String>();
        
        Properties props = studentConnector.asProperties();
        this.engine = DebeziumEngine.create(Json.class)
        .using(props)
        .notifying((record,committer) -> {
            // System.out.println("-----清空messages-------");
            // 清空messages
            messages.clear();
            // System.out.println(messages);

            for (ChangeEvent<String, String> r : record) {               
                
                // System.out.println("-----紀錄中-----");                
                // System.out.println(r.key());
                
                //解析捕獲到資訊的key schema, 取得 Primary key or Unique key
                // System.out.println("-----key_jsonObject-----");
                JSONObject key_jsonObject =  new JSONObject(r.key());
                // System.out.println(key_jsonObject.toString());
                String key_schema = key_jsonObject.get("schema").toString();
                // System.out.println(key_schema);
                JSONObject key_schema_field = new JSONObject(key_schema);
                String key_schema_field2 = key_schema_field.get("fields").toString();
                // System.out.println(key_schema_field2);

                //取得每個Primary key or Unique key欄位, 並加入jsonArray
                JSONArray new_field_jsonArray = new JSONArray();
                JSONArray field_jsonArray = new JSONArray(key_schema_field2);                
                for(int i =0; i<field_jsonArray.length(); i++){
                    String field_value = field_jsonArray.get(i).toString();
                    JSONObject field_value2 = new JSONObject(field_value);
                    String field_value3 = field_value2.get("field").toString();
                    // System.out.println(field_value3);
                    
                    new_field_jsonArray.put(i, field_value3);                   
                }
                // System.out.println(new_field_jsonArray);
			    


                //捕獲到資訊的value
                JSONObject jsonObject = new JSONObject(r.value());
                String payload_info = jsonObject.get("payload").toString();
                String cdc_info = jsonObject.put("keyField",new_field_jsonArray).toString();
                // System.out.println(cdc_info);
                

                if (payload_info != "null"){
                    System.out.println("-----CDC後-----");
                    System.out.println(payload_info);
                    JSONObject payload_info_obj = new JSONObject(payload_info);
                    String payload_op = payload_info_obj.get("op").toString();
                    // System.out.println(payload_op);
                    if (!payload_op.equals("r")){
                        System.out.println("-----放進message後-------");
                        messages.put(cdc_info, "key1");
                        // jsonObject.toString()

                    }                    
                       
                    
                }
                committer.markProcessed(r);
                
            }

            // System.out.println("-----印出messages-------");
            // System.out.println(messages);

            try {
                System.out.println("-----印出messages-------");
                System.out.println(messages);
                pb.sendMessage(messages); //cloud pubsub             

            } catch (IOException e) {                
                e.printStackTrace();
            }
            
        }).build();
        
    }

    /**
     * The method is called after the Debezium engine is initialized and started asynchronously using the Executor.
     */
    @PostConstruct
    private void start() {
        this.executor.execute(engine);

    }

    /**
     * This method is called when the container is being destroyed. This stops the debezium, merging the Executor.
     */
    @PreDestroy
    private void stop() {
        if (this.engine != null) {
            ((CDCListener) this.engine).stop();
        }
    }
}
