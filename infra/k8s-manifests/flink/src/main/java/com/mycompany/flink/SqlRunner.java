package com.mycompany.flink;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class SqlRunner {

    public static void main(String[] args) throws Exception {
        // Kiểm tra xem đã truyền đường dẫn file SQL vào chưa
        if (args.length < 1) {
            System.err.println("Lỗi: Vui lòng cung cấp đường dẫn tới file Flink SQL.");
            System.err.println("Sử dụng: SqlRunner <path_to_sql_file>");
            System.exit(1);
        }

        String sqlFilePath = args[0];
        System.out.println("Đang đọc file SQL từ: " + sqlFilePath);

        // 1. Khởi tạo Flink Table Environment ở chế độ Streaming
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // 2. Đọc toàn bộ nội dung file
        String sqlContent = new String(Files.readAllBytes(Paths.get(sqlFilePath)));

        // 3. Logic tách câu lệnh (Parsing)
        List<String> statements = new ArrayList<>();
        StringBuilder currentStmt = new StringBuilder();
        boolean inStatementSet = false;

        // Tách file thành từng dòng để duyệt
        String[] lines = sqlContent.split("\n");
        for (String line : lines) {
            String trimmed = line.trim();

            // Bỏ qua các dòng chú thích (comment) hoặc dòng trống
            if (trimmed.startsWith("--") || trimmed.isEmpty()) {
                continue;
            }

            // Nối dòng hiện tại vào câu lệnh đang build
            currentStmt.append(line).append("\n");

            // Kiểm tra xem có bắt đầu vào khối STATEMENT SET không
            if (trimmed.toUpperCase().startsWith("EXECUTE STATEMENT SET BEGIN")) {
                inStatementSet = true;
            }

            // Nếu đang ở trong khối STATEMENT SET
            if (inStatementSet) {
                // Chỉ cắt câu lệnh khi gặp từ khóa END;
                if (trimmed.toUpperCase().endsWith("END;")) {
                    statements.add(currentStmt.toString());
                    currentStmt.setLength(0); // Reset để đọc lệnh tiếp theo
                    inStatementSet = false;
                }
            } 
            // Nếu là câu lệnh SQL bình thường (CREATE TABLE, DROP, ...)
            else {
                // Cắt câu lệnh khi gặp dấu chấm phẩy ;
                if (trimmed.endsWith(";")) {
                    statements.add(currentStmt.toString());
                    currentStmt.setLength(0); // Reset
                }
            }
        }

        // Đề phòng trường hợp câu lệnh cuối cùng quên viết dấu ;
        if (currentStmt.toString().trim().length() > 0) {
            statements.add(currentStmt.toString());
        }

        // 4. Thực thi từng câu lệnh đã được cắt chuẩn xác
        for (String stmt : statements) {
            String cleanStmt = stmt.trim();
            if (cleanStmt.isEmpty()) continue;

            System.out.println("==================================================");
            System.out.println("Đang thực thi lệnh:\n" + cleanStmt);
            System.out.println("==================================================");
            
            // Gửi lệnh vào Flink
            tableEnv.executeSql(cleanStmt);
        }
    }
}