package com.mycompany.flink;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import java.nio.file.Files;
import java.nio.file.Paths;

public class SqlRunner {
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            throw new RuntimeException("Not found sql file path");
        }
        
        String sqlFilePath = args[0];
        System.out.println("Read SQL file: " + sqlFilePath);
        
        String sql = new String(Files.readAllBytes(Paths.get(sqlFilePath)));
        
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);
        
        String[] statements = sql.split(";");
        
        System.out.println("Staring...");
        for (String stmt : statements) {
            String trimmedStmt = stmt.trim();
            if (!trimmedStmt.isEmpty()) {
                System.out.println("Running: \n" + trimmedStmt);
                
                // Hứng kết quả trả về
                TableResult result = tEnv.executeSql(trimmedStmt);
                
                // Nếu là lệnh INSERT, ép chương trình Java đứng chờ Job chạy
                if (trimmedStmt.toUpperCase().startsWith("INSERT")) {
                    System.out.println("Đang chờ luồng INSERT chạy vô hạn...");
                    result.await(); 
                }
            }
        }
        System.out.println("Ok!!!!!!!!");
    }
}