import lombok.SneakyThrows;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;

public class PooledConnection implements Connection {

    private final BlockingQueue<Connection> connectionPool;
    private final List<Statement> statements = new ArrayList<>();
    private final Connection physicalConnection;
    private final int initTransactionIsolationLevel;

    @SneakyThrows
    public PooledConnection(BlockingQueue<Connection> connectionPool, Connection physicalConnection) {
        this.connectionPool = connectionPool;
        this.physicalConnection = physicalConnection;
        this.initTransactionIsolationLevel = physicalConnection.getTransactionIsolation();
    }


    @Override
    public void close() throws SQLException {
        clearStatements();
        resetConnection();
        connectionPool.add(this);
    }

    private void resetConnection() throws SQLException {
        if (!this.isClosed() && !this.getAutoCommit()) {
            this.rollback();
        }
        this.setReadOnly(false);
        this.setAutoCommit(true);
        this.setTransactionIsolation(initTransactionIsolationLevel);
    }

    private void clearStatements() throws SQLException {
        for (Statement statement : statements) {
            statement.close();
        }
        statements.clear();
    }


    @Override
    public Statement createStatement() throws SQLException {
        Statement statement = physicalConnection.createStatement();
        statements.add(statement);
        return statement;
    }

    @Override
    public PreparedStatement prepareStatement(String s) throws SQLException {
        PreparedStatement statement = physicalConnection.prepareStatement(s);
        statements.add(statement);
        return statement;
    }

    @Override
    public CallableStatement prepareCall(String s) throws SQLException {
        CallableStatement statement = physicalConnection.prepareCall(s);
        statements.add(statement);
        return statement;
    }

    @Override
    public String nativeSQL(String s) throws SQLException {
        return physicalConnection.nativeSQL(s);
    }

    @Override
    public void setAutoCommit(boolean b) throws SQLException {
        physicalConnection.setAutoCommit(b);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return physicalConnection.getAutoCommit();
    }

    @Override
    public void commit() throws SQLException {
        physicalConnection.commit();
    }

    @Override
    public void rollback() throws SQLException {
        physicalConnection.rollback();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return physicalConnection.isClosed();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return physicalConnection.getMetaData();
    }

    @Override
    public void setReadOnly(boolean b) throws SQLException {
        physicalConnection.setReadOnly(b);
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return physicalConnection.isReadOnly();
    }

    @Override
    public void setCatalog(String s) throws SQLException {
        physicalConnection.setCatalog(s);
    }

    @Override
    public String getCatalog() throws SQLException {
        return physicalConnection.getCatalog();
    }

    @Override
    public void setTransactionIsolation(int i) throws SQLException {
        physicalConnection.setTransactionIsolation(i);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return physicalConnection.getTransactionIsolation();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return physicalConnection.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        physicalConnection.clearWarnings();
    }

    @Override
    public Statement createStatement(int i, int i1) throws SQLException {
        return physicalConnection.createStatement(i, i1);
    }

    @Override
    public PreparedStatement prepareStatement(String s, int i, int i1) throws SQLException {
        return physicalConnection.prepareStatement(s, i, i1);
    }

    @Override
    public CallableStatement prepareCall(String s, int i, int i1) throws SQLException {
        return physicalConnection.prepareCall(s, i, i1);
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return physicalConnection.getTypeMap();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        physicalConnection.setTypeMap(map);
    }

    @Override
    public void setHoldability(int i) throws SQLException {
        physicalConnection.setHoldability(i);
    }

    @Override
    public int getHoldability() throws SQLException {
        return physicalConnection.getHoldability();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return physicalConnection.setSavepoint();
    }

    @Override
    public Savepoint setSavepoint(String s) throws SQLException {
        return physicalConnection.setSavepoint(s);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        physicalConnection.rollback(savepoint);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        physicalConnection.releaseSavepoint(savepoint);
    }

    @Override
    public Statement createStatement(int i, int i1, int i2) throws SQLException {
        return physicalConnection.createStatement(i, i1, i2);
    }

    @Override
    public PreparedStatement prepareStatement(String s, int i, int i1, int i2) throws SQLException {
        return physicalConnection.prepareStatement(s, i, i1, i2);
    }

    @Override
    public CallableStatement prepareCall(String s, int i, int i1, int i2) throws SQLException {
        return physicalConnection.prepareCall(s, i, i1, i2);
    }

    @Override
    public PreparedStatement prepareStatement(String s, int i) throws SQLException {
        return physicalConnection.prepareStatement(s, i);
    }

    @Override
    public PreparedStatement prepareStatement(String s, int[] ints) throws SQLException {
        return physicalConnection.prepareStatement(s, ints);
    }

    @Override
    public PreparedStatement prepareStatement(String s, String[] strings) throws SQLException {
        return physicalConnection.prepareStatement(s, strings);
    }

    @Override
    public Clob createClob() throws SQLException {
        return physicalConnection.createClob();
    }

    @Override
    public Blob createBlob() throws SQLException {
        return physicalConnection.createBlob();
    }

    @Override
    public NClob createNClob() throws SQLException {
        return physicalConnection.createNClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return physicalConnection.createSQLXML();
    }

    @Override
    public boolean isValid(int i) throws SQLException {
        return physicalConnection.isValid(i);
    }

    @Override
    public void setClientInfo(String s, String s1) throws SQLClientInfoException {
        setClientInfo(s, s1);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        physicalConnection.setClientInfo(properties);
    }

    @Override
    public String getClientInfo(String s) throws SQLException {
        return physicalConnection.getClientInfo(s);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return physicalConnection.getClientInfo();
    }

    @Override
    public Array createArrayOf(String s, Object[] objects) throws SQLException {
        return physicalConnection.createArrayOf(s, objects);
    }

    @Override
    public Struct createStruct(String s, Object[] objects) throws SQLException {
        return physicalConnection.createStruct(s, objects);
    }

    @Override
    public void setSchema(String s) throws SQLException {
        physicalConnection.setSchema(s);
    }

    @Override
    public String getSchema() throws SQLException {
        return physicalConnection.getSchema();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        physicalConnection.abort(executor);
    }

    @Override
    public void setNetworkTimeout(Executor executor, int i) throws SQLException {
        physicalConnection.setNetworkTimeout(executor, i);
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return physicalConnection.getNetworkTimeout();
    }

    @Override
    public <T> T unwrap(Class<T> aClass) throws SQLException {
        return physicalConnection.unwrap(aClass);
    }

    @Override
    public boolean isWrapperFor(Class<?> aClass) throws SQLException {
        return physicalConnection.isWrapperFor(aClass);
    }


}
