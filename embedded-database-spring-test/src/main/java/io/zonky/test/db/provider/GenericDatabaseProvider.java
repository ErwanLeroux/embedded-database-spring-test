package io.zonky.test.db.provider;

import javax.sql.DataSource;

public interface GenericDatabaseProvider {

    DataSource getDatabase(DatabasePreparer preparer, DatabaseDescriptor descriptor) throws Exception;

}
