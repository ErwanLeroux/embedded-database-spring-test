package io.zonky.test.db.provider;

import org.springframework.beans.factory.ObjectProvider;

import javax.sql.DataSource;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.function.Function.identity;

public class DefaultDatabaseProvider implements GenericDatabaseProvider {

    private final Map<DatabaseDescriptor, DatabaseProvider> databaseProviders;

    public DefaultDatabaseProvider(ObjectProvider<Collection<DatabaseProvider>> databaseProviders) {
        this.databaseProviders = databaseProviders.getIfAvailable(Collections::emptyList).stream()
                .collect(Collectors.toMap(p -> DatabaseDescriptor.of(p.getDatabaseType(), p.getProviderType()), identity()));
    }

    @Override
    public DataSource getDatabase(DatabasePreparer preparer, DatabaseDescriptor descriptor) throws Exception {
        DatabaseProvider provider = databaseProviders.get(descriptor);

        if (provider != null) {
            return provider.getDatabase(preparer);
        }

        throw new IllegalStateException(String.format("Missing database provider for descriptor: %s", descriptor)); // TODO: use a specific exception type
    }
}
