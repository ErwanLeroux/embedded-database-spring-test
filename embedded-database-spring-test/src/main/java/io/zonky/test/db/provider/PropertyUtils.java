package io.zonky.test.db.provider;

import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.Environment;
import org.springframework.core.env.PropertySource;

import java.util.HashMap;
import java.util.Map;

public class PropertyUtils {

    private PropertyUtils() {}

    public static Map<String, String> extractAll(Environment environment, String prefix) {
        Map<String, String> properties = new HashMap<>();
        if (environment instanceof ConfigurableEnvironment) {
            for (PropertySource<?> propertySource : ((ConfigurableEnvironment) environment).getPropertySources()) {
                if (propertySource instanceof EnumerablePropertySource) {
                    for (String key : ((EnumerablePropertySource) propertySource).getPropertyNames()) {
                        if (key.startsWith(prefix)) {
                            properties.put(key, String.valueOf(propertySource.getProperty(key)));
                        }
                    }
                }
            }
        }
        return properties;
    }
}
