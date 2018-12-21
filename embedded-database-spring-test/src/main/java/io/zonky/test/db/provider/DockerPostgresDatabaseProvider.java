package io.zonky.test.db.provider;

import com.github.dockerjava.api.command.CreateContainerCmd;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import io.zonky.test.db.flyway.BlockingDataSourceWrapper;
import io.zonky.test.db.provider.DockerPostgresDatabaseProvider.DatabaseInstance.DatabaseTemplate;
import org.apache.commons.lang3.RandomStringUtils;
import org.postgresql.ds.PGSimpleDataSource;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.Environment;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT;

public class DockerPostgresDatabaseProvider implements DatabaseProvider {

    private static final LoadingCache<DatabaseConfig, DatabaseInstance> databases = CacheBuilder.newBuilder()
            .build(new CacheLoader<DatabaseConfig, DatabaseInstance>() {
                public DatabaseInstance load(DatabaseConfig config) {
                    return new DatabaseInstance(config);
                }
            });

    private final DatabaseConfig databaseConfig;
    private final ClientConfig clientConfig;

    public DockerPostgresDatabaseProvider(Environment environment) {
        String dockerImage = environment.getProperty("embedded-database.postgres.docker.image", "postgres:10.6-alpine");
        String tmpfsOptions = environment.getProperty("embedded-database.postgres.docker.tmpfs.options", "rw,noexec,nosuid");
        boolean tmpfsEnabled = environment.getProperty("embedded-database.postgres.docker.tmpfs.enabled", boolean.class, false);

        Map<String, String> initdbProperties = PropertyUtils.extractAll(environment, "embedded-database.postgres.initdb.properties");
        Map<String, String> configProperties = PropertyUtils.extractAll(environment, "embedded-database.postgres.server.properties");
        Map<String, String> connectProperties = PropertyUtils.extractAll(environment, "embedded-database.postgres.client.properties");

        this.databaseConfig = new DatabaseConfig(dockerImage, tmpfsOptions, tmpfsEnabled, initdbProperties, configProperties);
        this.clientConfig = new ClientConfig(connectProperties);
    }

    @Override
    public DatabaseType getDatabaseType() {
        return DatabaseType.POSTGRES;
    }

    @Override
    public ProviderType getProviderType() {
        return ProviderType.DOCKER;
    }

    @Override
    public DataSource getDatabase(DatabasePreparer preparer) throws SQLException {
        DatabaseInstance instance = databases.getUnchecked(databaseConfig);
        DatabaseTemplate template = instance.getTemplate(clientConfig, preparer);
        return template.createDatabase();
    }

    protected static class DatabaseInstance {

        private final PostgreSQLContainer container;
        private final Semaphore semaphore;

        private final LoadingCache<TemplateKey, DatabaseTemplate> templates = CacheBuilder.newBuilder()
                .build(new CacheLoader<TemplateKey, DatabaseTemplate>() {
                    public DatabaseTemplate load(TemplateKey key) throws Exception {
                        return new DatabaseTemplate(key.config, key.preparer);
                    }
                });

        private DatabaseInstance(DatabaseConfig config) {
            String initdbArgs = config.initdbProperties.entrySet().stream()
                    .map(e -> String.format("--%s=%s", e.getKey(), e.getValue()))
                    .collect(Collectors.joining(" "));

            Map<String, String> serverProperties = new HashMap<>(config.configProperties);

            serverProperties.put("fsync", "off");
            serverProperties.put("full_page_writes", "off");
            serverProperties.putIfAbsent("max_connections", "300");

            String postgresArgs = serverProperties.entrySet().stream()
                    .map(e -> String.format("-c %s=%s", e.getKey(), e.getValue()))
                    .collect(Collectors.joining(" "));

            container = new PostgreSQLContainer(config.dockerImage) {
                @Override
                protected void configure() {
                    super.configure();
                    addEnv("POSTGRES_INITDB_ARGS", "--nosync " + initdbArgs);
                    setCommand("postgres -c fsync=off -c full_page_writes=off " + postgresArgs);
                }
            };

            if (config.tmpfsEnabled) {
                Consumer<CreateContainerCmd> consumer = cmd -> cmd.getHostConfig()
                        .withTmpFs(ImmutableMap.of("/var/lib/postgresql/data", config.tmpfsOptions));
                container.withCreateContainerCmdModifier(consumer);
            }

            container.withUsername("postgres");
            container.withPassword("test");
            container.start();
            container.followOutput(new Slf4jLogConsumer(LoggerFactory.getLogger(DockerPostgresDatabaseProvider.class)));

            semaphore = new Semaphore(Integer.parseInt(serverProperties.get("max_connections")));

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

            private DataSource getDatabase(String dbName) throws SQLException {
                PGSimpleDataSource dataSource = new PGSimpleDataSource();

                dataSource.setServerName(container.getContainerIpAddress());
                dataSource.setPortNumber(container.getMappedPort(POSTGRESQL_PORT));
                dataSource.setDatabaseName(dbName);

                dataSource.setUser("postgres");
                dataSource.setPassword("test");

                for (Map.Entry<String, String> entry : config.connectProperties.entrySet()) {
                    dataSource.setProperty(entry.getKey(), entry.getValue());
                }

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

        private final String dockerImage;
        private final String tmpfsOptions;
        private final boolean tmpfsEnabled;
        private final Map<String, String> initdbProperties;
        private final Map<String, String> configProperties;

        private DatabaseConfig(String dockerImage, String tmpfsOptions, boolean tmpfsEnabled, Map<String, String> initdbProperties, Map<String, String> configProperties) {
            this.dockerImage = dockerImage;
            this.tmpfsOptions = tmpfsOptions;
            this.tmpfsEnabled = tmpfsEnabled;
            this.initdbProperties = ImmutableMap.copyOf(initdbProperties);
            this.configProperties = ImmutableMap.copyOf(configProperties);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DatabaseConfig that = (DatabaseConfig) o;
            return tmpfsEnabled == that.tmpfsEnabled &&
                    Objects.equals(dockerImage, that.dockerImage) &&
                    Objects.equals(tmpfsOptions, that.tmpfsOptions) &&
                    Objects.equals(initdbProperties, that.initdbProperties) &&
                    Objects.equals(configProperties, that.configProperties);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dockerImage, tmpfsOptions, tmpfsEnabled, initdbProperties, configProperties);
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
