package com.bobocode;

import javax.sql.DataSource;

public class Main {

    public static void main(String[] args) {
        DataSource dataSource = initPostgresPooledDatasource();
        var start = System.nanoTime();
        for (int i = 0; i < 10_000; i++) {
            try (var connection = dataSource.getConnection();
                 var statement = connection.prepareStatement("select now() as time")) {
                statement.executeQuery();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        var end = System.nanoTime();
        System.out.println((end - start) / 1_000_000 + "ms");

    }

    private static DataSource initPostgresPooledDatasource() {
        return new PooledDataSource("jdbc:postgresql://localhost:5432/postgres", "postgres", "admin", 10);
    }

    private static DataSource initMysqlPooledDataSource() {
        return new PooledDataSource("jdbc:mysql://localhost:3306", "root", "admin", 10);
    }
}
