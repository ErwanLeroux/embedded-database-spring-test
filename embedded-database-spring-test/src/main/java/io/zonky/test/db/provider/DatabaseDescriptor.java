package io.zonky.test.db.provider;

import com.google.common.base.MoreObjects;
import org.springframework.util.Assert;

import java.util.Objects;

public final class DatabaseDescriptor {

    private final DatabaseType databaseType;
    private final ProviderType providerType;

    public static DatabaseDescriptor of(DatabaseType databaseType, ProviderType providerType) {
        Assert.notNull(databaseType, "Database type must not be null");
        Assert.notNull(providerType, "Provider type must not be null");
        return new DatabaseDescriptor(databaseType, providerType);
    }

    private DatabaseDescriptor(DatabaseType databaseType, ProviderType providerType) {
        this.databaseType = databaseType;
        this.providerType = providerType;
    }

    public DatabaseType getDatabaseType() {
        return databaseType;
    }

    public ProviderType getProviderType() {
        return providerType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DatabaseDescriptor that = (DatabaseDescriptor) o;
        return Objects.equals(databaseType, that.databaseType) &&
                Objects.equals(providerType, that.providerType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(databaseType, providerType);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("databaseType", databaseType)
                .add("providerType", providerType)
                .toString();
    }
}
