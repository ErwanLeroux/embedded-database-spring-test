package io.zonky.test.db.provider;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import io.zonky.test.db.flyway.BlockingDataSourceWrapper;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import io.zonky.test.db.provider.MavenPostgresDatabaseProvider.DatabaseInstance.DatabaseTemplate;
import org.apache.commons.lang3.RandomStringUtils;
import org.springframework.core.env.Environment;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Semaphore;

public class MavenPostgresDatabaseProvider implements DatabaseProvider {

    private static final LoadingCache<DatabaseConfig, DatabaseInstance> databases = CacheBuilder.newBuilder()
            .build(new CacheLoader<DatabaseConfig, DatabaseInstance>() {
                public DatabaseInstance load(DatabaseConfig config) throws IOException {
                    return new DatabaseInstance(config);
                }
            });

    private final DatabaseConfig databaseConfig;
    private final ClientConfig clientConfig;

//    @Autowired(required = false)
//    protected List<Consumer<Builder>> databaseCustomizers = new ArrayList<>(); // TODO:

    public MavenPostgresDatabaseProvider(Environment environment) {
        Map<String, String> initdbProperties = PropertyUtils.extractAll(environment, "embedded-database.postgres.initdb.properties");
        Map<String, String> configProperties = PropertyUtils.extractAll(environment, "embedded-database.postgres.server.properties");
        Map<String, String> connectProperties = PropertyUtils.extractAll(environment, "embedded-database.postgres.client.properties");

        this.databaseConfig = new DatabaseConfig(initdbProperties, configProperties);
        this.clientConfig = new ClientConfig(connectProperties);
    }

    @Override
    public DatabaseType getDatabaseType() {
        return DatabaseType.POSTGRES;
    }

    @Override
    public ProviderType getProviderType() {
        return ProviderType.MAVEN;
    }

    @Override
    public DataSource getDatabase(DatabasePreparer preparer) throws SQLException {
        DatabaseInstance instance = databases.getUnchecked(databaseConfig);
        DatabaseTemplate template = instance.getTemplate(clientConfig, preparer);
        return template.createDatabase();
    }

    protected static class DatabaseInstance {

        private static final int MAX_DATABASE_CONNECTIONS = 300;

        private final EmbeddedPostgres postgres;
        private final Semaphore semaphore;

        private final LoadingCache<TemplateKey, DatabaseTemplate> templates = CacheBuilder.newBuilder()
                .build(new CacheLoader<TemplateKey, DatabaseTemplate>() {
                    public DatabaseTemplate load(TemplateKey key) throws Exception {
                        return new DatabaseTemplate(key.config, key.preparer);
                    }
                });

        private DatabaseInstance(DatabaseConfig config) throws IOException {
            EmbeddedPostgres.Builder builder = EmbeddedPostgres.builder();

            builder.setPGStartupWait(Duration.ofSeconds(30L));
            config.initdbProperties.forEach(builder::setLocaleConfig);
            config.configProperties.forEach(builder::setServerConfig);
            builder.setServerConfig("max_connections", String.valueOf(MAX_DATABASE_CONNECTIONS));

            postgres = builder.start();
            semaphore = new Semaphore(MAX_DATABASE_CONNECTIONS);

        }

        public DatabaseTemplate getTemplate(ClientConfig config, DatabasePreparer preparer) {
            return templates.getUnchecked(new TemplateKey(config, preparer));
        }

        protected class DatabaseTemplate {

            private final ClientConfig config;
            private final String templateName;

            private DatabaseTemplate(ClientConfig config, DatabasePreparer preparer) throws SQLException {
                this.config = config;
                this.templateName = RandomStringUtils.randomAlphabetic(12).toLowerCase(Locale.ENGLISH);

                executeStatement(String.format("CREATE DATABASE %s OWNER %s ENCODING 'utf8'", templateName, "postgres"));
                DataSource dataSource = getDatabase(templateName);
                preparer.prepare(dataSource);
            }

            public DataSource createDatabase() throws SQLException {
                String databaseName = RandomStringUtils.randomAlphabetic(12).toLowerCase(Locale.ENGLISH);
                executeStatement(String.format("CREATE DATABASE %s TEMPLATE %s OWNER %s ENCODING 'utf8'", databaseName, templateName, "postgres"));
                return getDatabase(databaseName);
            }

            private void executeStatement(String ddlStatement) throws SQLException {
                DataSource dataSource = getDatabase("postgres");
                try (Connection connection = dataSource.getConnection(); PreparedStatement stmt = connection.prepareStatement(ddlStatement)) {
                    stmt.execute();
                }
            }

            private DataSource getDatabase(String dbName) {
                DataSource dataSource = postgres.getDatabase("postgres", dbName, config.connectProperties);
                return new BlockingDataSourceWrapper(dataSource, semaphore);
            }
        }

        protected static class TemplateKey {

            private final ClientConfig config;
            private final DatabasePreparer preparer;

            private TemplateKey(ClientConfig config, DatabasePreparer preparer) {
                this.config = config;
                this.preparer = preparer;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                TemplateKey that = (TemplateKey) o;
                return Objects.equals(config, that.config) &&
                        Objects.equals(preparer, that.preparer);
            }

            @Override
            public int hashCode() {
                return Objects.hash(config, preparer);
            }
        }
    }

    private static class DatabaseConfig {

        private final Map<String, String> initdbProperties;
        private final Map<String, String> configProperties;

        private DatabaseConfig(Map<String, String> initdbProperties, Map<String, String> configProperties) {
            this.initdbProperties = ImmutableMap.copyOf(initdbProperties);
            this.configProperties = ImmutableMap.copyOf(configProperties);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DatabaseConfig that = (DatabaseConfig) o;
            return Objects.equals(initdbProperties, that.initdbProperties) &&
                    Objects.equals(configProperties, that.configProperties);
        }

        @Override
        public int hashCode() {
            return Objects.hash(initdbProperties, configProperties);
        }
    }

    private static class ClientConfig {

        private final Map<String, String> connectProperties;

        private ClientConfig(Map<String, String> connectProperties) {
            this.connectProperties = ImmutableMap.copyOf(connectProperties);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ClientConfig that = (ClientConfig) o;
            return Objects.equals(connectProperties, that.connectProperties);
        }

        @Override
        public int hashCode() {
            return Objects.hash(connectProperties);
        }
    }
}
