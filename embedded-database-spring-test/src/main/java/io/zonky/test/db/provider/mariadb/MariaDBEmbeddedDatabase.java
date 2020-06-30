/*
 * Copyright 2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.zonky.test.db.provider.mariadb;

import io.zonky.test.db.provider.EmbeddedDatabase;
import org.apache.commons.lang3.StringUtils;
import org.mariadb.jdbc.MariaDbDataSource;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Logger;

import static io.zonky.test.db.util.ReflectionUtils.getField;

public class MariaDBEmbeddedDatabase implements EmbeddedDatabase {

    private final MariaDbDataSource dataSource;
    private final CopyOnWriteArrayList<CloseCallback> closeCallbacks;

    public MariaDBEmbeddedDatabase(MariaDbDataSource dataSource, CloseCallback closeCallback) {
        this.dataSource = dataSource;
        this.closeCallbacks = new CopyOnWriteArrayList<>(new CloseCallback[] { closeCallback });
    }

    @Override
    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return dataSource.getConnection(username, password);
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return dataSource.getLogWriter();
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        dataSource.setLogWriter(out);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return dataSource.getLoginTimeout();
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        dataSource.setLoginTimeout(seconds);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        if (iface.isAssignableFrom(dataSource.getClass())) {
            return iface.cast(dataSource);
        }
        return dataSource.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return true;
        }
        if (iface.isAssignableFrom(dataSource.getClass())) {
            return true;
        }
        return dataSource.isWrapperFor(iface);
    }
    
    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return dataSource.getParentLogger();
    }

    @Override
    public String getUrl() {
        String url = String.format("jdbc:mariadb://%s:%s/%s?user=%s",
                dataSource.getServerName(), dataSource.getPort(), dataSource.getDatabaseName(), dataSource.getUser());
        if (StringUtils.isNotBlank(getPassword())) {
            url += String.format("&password=%s", getPassword());
        }
        return url;
    }

    private String getPassword() {
        return getField(dataSource, "password");
    }

    @Override
    public Map<String, String> getAliases() {
        return Collections.emptyMap();
    }

    @Override
    public synchronized void close() {
        closeCallbacks.forEach(closeCallback -> {
            try {
                closeCallback.call();
            } catch (SQLException e) {
                // TODO: investigate the issue and consider adding a configuration property for enabling/disabling the exception
//            throw new ProviderException("Unexpected error when releasing the database", e);
            }
        });
    }

    void registerCloseCallback(CloseCallback closeCallback) {
        closeCallbacks.add(closeCallback);
    }

    @FunctionalInterface
    public interface CloseCallback {

        void call() throws SQLException;

    }
}
