package com.cdc.student.listener;

import io.debezium.config.Configuration;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;

import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.stereotype.Component;

import com.cdc.student.PubDemo;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.ArrayList;
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
    private final DebeziumEngine<ChangeEvent<String, String>> engine;


    /**
     * PostgreSQL database.
     */   
    private String url = "jdbc:postgresql://localhost:5432/studentdb";
    private String user = "user";
    private String password = "password";

    //設置一個開關,不直接使用isLastLsm,是因比對到table lsn後是要他的下一筆才繼續執行上傳
    private Boolean findLast = false;
    /**
     * Connect to the PostgreSQL database
     *
     * @return a Connection object
     */
    public Connection connect() throws SQLException {
        return DriverManager.getConnection(url, user, password);        
    }

   
    

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
        
        ArrayList<String> lsnArrayList = new ArrayList<String>();

        
        
        // table 儲存需排除的已記錄過的 lsn
        ResultSet rs = null;
		String SQL ="Select * From recorded_lsn";
        try (Connection conn = connect();
                PreparedStatement pstmt = conn.prepareStatement(SQL)){
            System.out.println("******************Connection******************");
            // pstmt.setInt(1,1234);
            // pstmt.setString(2,"1234");            
            // System.out.println(pstmt);
            
            rs = pstmt.executeQuery();
            while(rs.next()) {
                lsnArrayList.add(rs.getString("lsn"));
                // System.out.println(rs.getString("lsn"));    
            } 
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
        
        System.out.println("------lsnArrayList-------");
        System.out.println(lsnArrayList);
        

        

        Properties props = studentConnector.asProperties();
        this.engine = DebeziumEngine.create(Json.class)
        .using(props)
        .notifying((record,committer) -> {
            // 清空messages
            messages.clear();
            Boolean isLastLsm = false;
            
            String lsn_num_r ="";

            for (ChangeEvent<String, String> r : record) {          
                
                // System.out.println("-----紀錄中-----");                
                // System.out.println(r.key());
                
                //解析捕獲到資訊的key schema, 取得 Primary key or Unique key
                // System.out.println("-----key_jsonObject-----");
                JSONObject key_jsonObject =  new JSONObject(r.key());
                
                //捕獲到資訊的value
                // System.out.println("-----jsonObject-----");
                JSONObject jsonObject = new JSONObject(r.value());
                // System.out.println(jsonObject);
                // System.out.println("-----jsonObject-----");
                String payload_info = jsonObject.get("payload").toString();

                // 判別是否lsn存在
                if (payload_info != "null"){
                    JSONObject payload_info_obj2 = new JSONObject(payload_info);
                    String lsn_num = payload_info_obj2.getJSONObject("source").get("lsn").toString();
                    System.out.println(lsn_num);
                    
                    // lsn table 未有資料 or 在lsn table中與最後一筆lsn符合 方法2 
                    if(lsnArrayList.size()!=0){
                            //判斷是否為table中的最後一筆
                            // System.out.println("lsnArrayList的table有值");                        
                            if(lsnArrayList.get(lsnArrayList.size()-1).equals(lsn_num)){
                                System.out.println("是table最後一筆");
                                isLastLsm = true;
                                findLast = true;
                                lsn_num_r = lsn_num;
                                continue;
                            }else if(findLast){
                                //程式執行中,捕獲到CDC
                                isLastLsm = true;                                                                
                            }else{                                
                                //包含在table,但不是最後一筆
                                isLastLsm = false;
                                continue;                                
                            }


                    }else{
                        System.out.println("lsnArrayList的table沒有值"); 
                        //table是空的,第一次run
                        isLastLsm = true;
                    }

                    // // lsn table 未有資料 or 在lsn table中與最後一筆lsn符合 方法2(修改中)
                    // if (lsnArrayList.size()!=0 && lsnArrayList.get(lsnArrayList.size()-1).equals(lsn_num) ){
                    //     System.out.println("table有");
                    //     isLastLsm = true;
                    //     lsn_num_r = lsn_num;
                    //     continue;
                        
                    // }
                    // else if(lsnArrayList.size()==0){
                    //     isLastLsm = true;
                    // }
                    // else if(lsnArrayList.size()!=0 && notFirstCdc == true){ //程式執行中CDC的條件
                    //     isLastLsm = true; 
                    // }
                    
                    lsn_num_r = lsn_num;
                    
                    
                    if (isLastLsm){
                        // System.out.println("isLastLsm 是 true");
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
                        String cdc_info = jsonObject.put("keyField",new_field_jsonArray).toString();
                        // System.out.println(cdc_info);
                        
                        if (payload_info != "null"){
                            // System.out.println("-----After CDC-----");
                            // System.out.println(payload_info);
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
                        System.out.println("****************加進table******************");
                        System.out.println(lsn_num_r);
                    }
                    
                    //last recorded lsn 存放在 recorded_lsn(table) 的 lsn(field)
                    if(!lsnArrayList.contains(lsn_num_r)){
                        String sqlLsn ="INSERT INTO recorded_lsn (lsn) VALUES (?);";
                        try (Connection conn = connect();
                                PreparedStatement pstmt = conn.prepareStatement(sqlLsn)){
                            System.out.println("******************record last recorded lsn******************");
                            pstmt.setString(1,lsn_num_r);            
                            System.out.println(pstmt);
                            
                            pstmt.executeUpdate();
                            // lsnArrayList.clear(); 

                        } catch (SQLException ex) {
                            System.out.println(ex.getMessage());
                        } 

                    }
                    
                    
                    
                }
                
            }
            
        
            // //紀錄處理到的最後一筆lsn (測試用)            
            // System.out.println("-----print messages-------");
            // System.out.println(messages);
            // // System.out.println(lsn_num_r);
            // isLastLsm = false;
           

            // cloud pubsub + 紀錄處理到的最後一筆lsn (正式)
            try {
                System.out.println("-----print messages-------");
                System.out.println(messages);
                isLastLsm = false;
             
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
