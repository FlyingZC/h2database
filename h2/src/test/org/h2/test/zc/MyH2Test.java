package org.h2.test.zc;

import org.checkerframework.checker.units.qual.A;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static java.sql.Connection.TRANSACTION_READ_COMMITTED;
import static java.sql.Connection.TRANSACTION_REPEATABLE_READ;

public class MyH2Test {

    public static void main(String[] args) throws Exception {
//        testQuery();
        doTestTransactionIsolationRC();
    }
    
    @Test
    public void testTransactionRollback() throws Exception {
        Connection conn = getConnection();
        conn.setAutoCommit(false);
        executeWithLog(conn, "insert into Test(id, name) values(1, 'name1'), (2, 'name2')");
        conn.rollback();
    }

    @Test
    public void testTransactionIsolationRC() throws Exception {
        doTestTransactionIsolationRC();
    }

    private static void doTestTransactionIsolationRC() throws ClassNotFoundException, SQLException {
        Connection conn = getConnection();
        conn.createStatement().execute("delete from test");
        conn.close();

        Connection conn1 = getConnection();
        Connection conn2 = getConnection();
        int isolation = 
                // TRANSACTION_READ_COMMITTED
                TRANSACTION_REPEATABLE_READ
                ;
        conn1.setTransactionIsolation(isolation);
        conn2.setTransactionIsolation(isolation);
        conn1.setAutoCommit(false);
        conn2.setAutoCommit(false);
        System.out.println("query-----");
        // 是在第一次执行sql时创建快照
        printQueryResultSet(executeQueryWithLog(conn2, "select * from test"));
        executeWithLog(conn1, "insert into test(id, name) values(1, 'name1'), (2, 'name2')");
        conn1.commit();
        System.out.println("query-----");
        printQueryResultSet(executeQueryWithLog(conn2, "select * from test"));
        
        // 自己插入数据
        executeWithLog(conn2, "insert into test(id, name) values(3, 'name3')");
        
        printQueryResultSet(executeQueryWithLog(conn2, "select * from test"));
        // 读到 conn1 已经提交的数据
        conn2.commit();
    }

    public static void executeWithLog(final Connection connection, final String sql) throws SQLException {
        System.out.println("Connection execute: " + sql);
        connection.createStatement().execute(sql);
    }

    public static ResultSet executeQueryWithLog(final Connection connection, final String sql) throws SQLException {
        System.out.println("Connection execute query: " + sql);
        return connection.createStatement().executeQuery(sql);
    }

    public static void printQueryResultSet(ResultSet resultSet) throws SQLException {
        String[] columnNames = getResultMetadata(resultSet);
        System.out.println("Result: ");
        System.out.println(String.join("|", columnNames));
        while (resultSet.next()) {
            List<String> row = new ArrayList<>();
            for (int i = 0; i < columnNames.length; i++) {
                row.add(resultSet.getString(i + 1));
            }
            if (row.isEmpty()) {
                System.out.println("");
            } else {
                System.out.println(String.join("|", row));
            }
        }
    }

    private static String[] getResultMetadata(final ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int count = metaData.getColumnCount();
        String[] name = new String[count];
        for (int i = 0; i < count; i++) {
            name[i] = metaData.getColumnName(i + 1);
        }
        return name;
    }

    private static void testQuery() throws ClassNotFoundException, SQLException {
        Connection conn = getConnection();
        try {
            // 创建表
            Statement stmt = conn.createStatement();
//            stmt.executeUpdate("drop TABLE IF EXISTS Test");
//            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS Test(id INT, NAME VARCHAR(255))");

            // 查询数据
            String sql = 
                    // "SELECT * FROM test where id > 1";
                    "SELECT * FROM test where id > 1 and name='name3'";
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                System.out.println("ID: " + rs.getInt("id") + ", Name: " + rs.getString("name"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (conn != null) conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private static Connection getConnection() throws ClassNotFoundException, SQLException {
        Connection conn;
        // 注册H2 JDBC驱动
        Class.forName("org.h2.Driver");

        // 连接到H2数据库（内存模式）
        conn = DriverManager.getConnection("jdbc:h2:~/test", "sa", "");
        return conn;
    }
}
