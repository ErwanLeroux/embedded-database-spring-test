package io.zonky.test.db.provider;

import javax.sql.DataSource;
import java.sql.SQLException;

public interface DatabasePreparer {

    void prepare(DataSource dataSource) throws SQLException;

}
