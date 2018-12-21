package io.zonky.test.db.provider;

import com.google.common.base.MoreObjects;
import org.springframework.util.Assert;

import java.util.Objects;

public final class DatabaseType {

    public static final DatabaseType POSTGRES = DatabaseType.of("postgres");

    private final String name;

    public static DatabaseType of(String name) {
        Assert.notNull(name, "Database name must not be null");
        return new DatabaseType(name.toLowerCase());
    }

    private DatabaseType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DatabaseType that = (DatabaseType) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("name", name)
                .toString();
    }
}
