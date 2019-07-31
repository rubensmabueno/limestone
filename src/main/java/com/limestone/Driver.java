package com.limestone;

import de.vandermeer.asciitable.AsciiTable;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

class Driver {

    public static void main(String...args) throws Exception {
        String url = "jdbc:calcite:model=build/resources/test/limestone.json";

        Properties properties = new Properties();
        properties.put("lex", "JAVA");
        Connection conn = DriverManager.getConnection(url, properties);

//        String sql = "SELECT ID, FIRST_NAME, LAST_NAME, EMAIL, GENDER, IP_ADDRESS, CC, COUNTRY, BIRTHDATE, SALARY, TITLE, COMMENTS FROM USERS LIMIT 12";
        String sql = "SELECT COUNTRY, AVG(SALARY) FROM USERS GROUP BY COUNTRY";

        AsciiTable at = new AsciiTable();

        try(ResultSet rs = conn.createStatement().executeQuery(sql)) {
            at.addRule();
            int columnCount = rs.getMetaData().getColumnCount();
            List<String> columns = new ArrayList<>();
            for (int i=1; i<columnCount+1;i++) {
                String columnLabel = rs.getMetaData().getColumnLabel(i);
                columns.add(columnLabel);
            }
            at.addRow(columns);
            while (rs.next()) {
                at.addRule();
                List<String> resultRow = new ArrayList<>();
                for (int i=1;i<columnCount+1;i++) {
                    Object object = rs.getObject(i);

                    if(object == null) {
                        resultRow.add("NULL");
                    } else {
                        resultRow.add(object.toString());
                    }
                }
                at.addRow(resultRow);

            }
            at.addRule();
        }

        System.out.println(at.render());
    }
}
