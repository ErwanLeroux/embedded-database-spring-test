package io.zonky.test.db.provider;

import javax.sql.DataSource;

public interface DatabaseProvider {

    DatabaseType getDatabaseType();

    ProviderType getProviderType();

    DataSource getDatabase(DatabasePreparer preparer) throws Exception;

}
