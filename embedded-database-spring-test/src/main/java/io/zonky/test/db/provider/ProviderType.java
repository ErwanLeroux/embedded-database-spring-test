package io.zonky.test.db.provider;

import com.google.common.base.MoreObjects;
import org.springframework.util.Assert;

import java.util.Objects;

public final class ProviderType {

    public static final ProviderType DOCKER = ProviderType.of("docker");
    public static final ProviderType MAVEN = ProviderType.of("maven");

    private final String name;

    public static ProviderType of(String name) {
        Assert.notNull(name, "Provider name must not be null");
        return new ProviderType(name.toLowerCase());
    }

    private ProviderType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProviderType that = (ProviderType) o;
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
