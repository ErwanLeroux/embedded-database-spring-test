/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.zonky.test.db.postgres;

import io.zonky.test.db.AutoConfigureEmbeddedDatabase;
import io.zonky.test.db.AutoConfigureEmbeddedDatabase.DatabaseProvider;
import io.zonky.test.db.AutoConfigureEmbeddedDatabase.Replace;
import io.zonky.test.db.flyway.DefaultFlywayDataSourceContext;
import io.zonky.test.db.flyway.FlywayClassUtils;
import io.zonky.test.db.flyway.FlywayDataSourceContext;
import io.zonky.test.db.provider.DefaultDatabaseProvider;
import io.zonky.test.db.provider.DockerPostgresDatabaseProvider;
import io.zonky.test.db.provider.MavenPostgresDatabaseProvider;
import io.zonky.test.db.provider.PrefetchingDatabaseProvider;
import org.apache.commons.lang3.StringUtils;
import org.flywaydb.core.Flyway;
import org.flywaydb.test.annotation.FlywayTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanDefinitionHolder;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.test.context.ContextConfigurationAttributes;
import org.springframework.test.context.ContextCustomizer;
import org.springframework.test.context.ContextCustomizerFactory;
import org.springframework.test.context.MergedContextConfiguration;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import javax.sql.DataSource;
import java.lang.reflect.AnnotatedElement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.zonky.test.db.AutoConfigureEmbeddedDatabase.DatabaseType;

/**
 * Implementation of the {@link org.springframework.test.context.ContextCustomizerFactory} interface,
 * which is responsible for initialization of the embedded postgres database and its registration to the application context.
 * The applied initialization strategy is driven by the {@link AutoConfigureEmbeddedDatabase} annotation.
 *
 * @see AutoConfigureEmbeddedDatabase
 */
public class EmbeddedPostgresContextCustomizerFactory implements ContextCustomizerFactory {

    private static final Logger logger = LoggerFactory.getLogger(EmbeddedPostgresContextCustomizerFactory.class);

    private static final boolean flywayNameAttributePresent = FlywayClassUtils.isFlywayNameAttributePresent();
    private static final boolean repeatableAnnotationPresent = FlywayClassUtils.isRepeatableFlywayTestAnnotationPresent();

    @Override
    public ContextCustomizer createContextCustomizer(Class<?> testClass, List<ContextConfigurationAttributes> configAttributes) {
        AutoConfigureEmbeddedDatabase databaseAnnotation = AnnotatedElementUtils.findMergedAnnotation(testClass, AutoConfigureEmbeddedDatabase.class);

        if (databaseAnnotation != null
                && databaseAnnotation.type() == DatabaseType.POSTGRES
                && databaseAnnotation.replace() != Replace.NONE) {
            return new PreloadableEmbeddedPostgresContextCustomizer(databaseAnnotation);
        }

        return null;
    }

    protected static class PreloadableEmbeddedPostgresContextCustomizer implements ContextCustomizer {

        private final AutoConfigureEmbeddedDatabase databaseAnnotation;

        public PreloadableEmbeddedPostgresContextCustomizer(AutoConfigureEmbeddedDatabase databaseAnnotation) {
            this.databaseAnnotation = databaseAnnotation;
        }

        @Override
        public void customizeContext(ConfigurableApplicationContext context, MergedContextConfiguration mergedConfig) {
            context.addBeanFactoryPostProcessor(new EnvironmentPostProcessor(context.getEnvironment(), databaseAnnotation.provider()));

            Class<?> testClass = mergedConfig.getTestClass();
            FlywayTest[] flywayAnnotations = findFlywayTestAnnotations(testClass);

            BeanDefinitionRegistry registry = getBeanDefinitionRegistry(context);
            RootBeanDefinition registrarDefinition = new RootBeanDefinition();

            registrarDefinition.setBeanClass(PreloadableEmbeddedPostgresRegistrar.class);
            registrarDefinition.getConstructorArgumentValues()
                    .addIndexedArgumentValue(0, databaseAnnotation);
            registrarDefinition.getConstructorArgumentValues()
                    .addIndexedArgumentValue(1, flywayAnnotations);

            registry.registerBeanDefinition("preloadableEmbeddedPostgresRegistrar", registrarDefinition);
        }

        // TODO
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            PreloadableEmbeddedPostgresContextCustomizer that =
                    (PreloadableEmbeddedPostgresContextCustomizer) o;

            return databaseAnnotation.equals(that.databaseAnnotation);
        }

        // TODO
        @Override
        public int hashCode() {
            return databaseAnnotation.hashCode();
        }
    }

    protected static class EnvironmentPostProcessor implements BeanDefinitionRegistryPostProcessor {

        private final ConfigurableEnvironment environment;
        private final DatabaseProvider databaseProvider;

        public EnvironmentPostProcessor(ConfigurableEnvironment environment, DatabaseProvider databaseProvider) {
            this.environment = environment;
            this.databaseProvider = databaseProvider;
        }

        @Override
        public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) throws BeansException {
            Map<String, Object> map = new HashMap<>();
            map.put("spring.test.database.replace", "NONE");

            if (databaseProvider != DatabaseProvider.DEFAULT) {
                map.put("embedded-database.provider", databaseProvider.toString());
            }

            environment.getPropertySources().addFirst(new MapPropertySource(
                    PreloadableEmbeddedPostgresContextCustomizer.class.getSimpleName(), map));
        }

        @Override
        public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {
            // nothing to do
        }
    }

    protected static class PreloadableEmbeddedPostgresRegistrar implements BeanDefinitionRegistryPostProcessor {

        private final AutoConfigureEmbeddedDatabase databaseAnnotation;
        private final FlywayTest[] flywayAnnotations;

        public PreloadableEmbeddedPostgresRegistrar(AutoConfigureEmbeddedDatabase databaseAnnotation, FlywayTest[] flywayAnnotations) {
            this.databaseAnnotation = databaseAnnotation;
            this.flywayAnnotations = flywayAnnotations;
        }

        @Override
        public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) throws BeansException {
            Assert.isInstanceOf(ConfigurableListableBeanFactory.class, registry,
                    "Embedded Database Auto-configuration can only be used with a ConfigurableListableBeanFactory");
            ConfigurableListableBeanFactory beanFactory = (ConfigurableListableBeanFactory) registry;

            // ---- TODO ----
            // MAKE THE FOLLOWING BEANS CONDITIONAL BY NAME
            RootBeanDefinition dockerPostgresProviderDefinition = new RootBeanDefinition();
            dockerPostgresProviderDefinition.setBeanClass(DockerPostgresDatabaseProvider.class);
            dockerPostgresProviderDefinition.setAutowireMode(AbstractBeanDefinition.AUTOWIRE_CONSTRUCTOR);
            registry.registerBeanDefinition("dockerPostgresProvider", dockerPostgresProviderDefinition);

            RootBeanDefinition mavenPostgresProviderDefinition = new RootBeanDefinition();
            mavenPostgresProviderDefinition.setBeanClass(MavenPostgresDatabaseProvider.class);
            mavenPostgresProviderDefinition.setAutowireMode(AbstractBeanDefinition.AUTOWIRE_CONSTRUCTOR);
            registry.registerBeanDefinition("mavenPostgresProvider", mavenPostgresProviderDefinition);

            RootBeanDefinition defaultDatabaseServiceDefinition = new RootBeanDefinition();
            defaultDatabaseServiceDefinition.setBeanClass(DefaultDatabaseProvider.class);
            defaultDatabaseServiceDefinition.setAutowireMode(AbstractBeanDefinition.AUTOWIRE_CONSTRUCTOR);
            registry.registerBeanDefinition("defaultDatabaseService", defaultDatabaseServiceDefinition);

            RootBeanDefinition prefetchingDatabaseServiceDefinition = new RootBeanDefinition();
            prefetchingDatabaseServiceDefinition.setBeanClass(PrefetchingDatabaseProvider.class);
            defaultDatabaseServiceDefinition.setAutowireMode(AbstractBeanDefinition.AUTOWIRE_CONSTRUCTOR);
            prefetchingDatabaseServiceDefinition.getConstructorArgumentValues().addIndexedArgumentValue(0, new RuntimeBeanReference("defaultDatabaseService"));
            prefetchingDatabaseServiceDefinition.setPrimary(true);
            registry.registerBeanDefinition("prefetchingDatabaseService", prefetchingDatabaseServiceDefinition);
            // ---- TODO ----

            BeanDefinitionHolder dataSourceInfo = getDataSourceBeanDefinition(beanFactory, databaseAnnotation);

            RootBeanDefinition dataSourceDefinition = new RootBeanDefinition();
            dataSourceDefinition.setPrimary(dataSourceInfo.getBeanDefinition().isPrimary());

            BeanDefinitionHolder flywayInfo = getFlywayBeanDefinition(beanFactory, flywayAnnotations);
            if (flywayInfo == null) {
                dataSourceDefinition.setBeanClass(EmptyEmbeddedPostgresDataSourceFactoryBean.class);
            } else {
                BeanDefinitionHolder contextInfo = getDataSourceContextBeanDefinition(beanFactory, flywayAnnotations);

                if (contextInfo == null) {
                    RootBeanDefinition dataSourceContextDefinition = new RootBeanDefinition();
                    dataSourceContextDefinition.setBeanClass(DefaultFlywayDataSourceContext.class);
                    registry.registerBeanDefinition("defaultDataSourceContext", dataSourceContextDefinition);
                    contextInfo = new BeanDefinitionHolder(dataSourceContextDefinition, "defaultDataSourceContext");
                }

                dataSourceDefinition.setBeanClass(FlywayEmbeddedPostgresDataSourceFactoryBean.class);

                dataSourceDefinition.getConstructorArgumentValues()
                        .addIndexedArgumentValue(0, flywayInfo.getBeanName());
                dataSourceDefinition.getConstructorArgumentValues()
                        .addIndexedArgumentValue(1, new RuntimeBeanReference(contextInfo.getBeanName()));
            }

            String dataSourceBeanName = dataSourceInfo.getBeanName();
            if (registry.containsBeanDefinition(dataSourceBeanName)) {
                logger.info("Replacing '{}' DataSource bean with embedded version", dataSourceBeanName);
                registry.removeBeanDefinition(dataSourceBeanName);
            }
            registry.registerBeanDefinition(dataSourceBeanName, dataSourceDefinition);
        }

        @Override
        public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {
            // nothing to do
        }
    }

    protected static BeanDefinitionRegistry getBeanDefinitionRegistry(ApplicationContext context) {
        if (context instanceof BeanDefinitionRegistry) {
            return (BeanDefinitionRegistry) context;
        }
        if (context instanceof AbstractApplicationContext) {
            return (BeanDefinitionRegistry) ((AbstractApplicationContext) context).getBeanFactory();
        }
        throw new IllegalStateException("Could not locate BeanDefinitionRegistry");
    }

    protected static BeanDefinitionHolder getDataSourceBeanDefinition(ConfigurableListableBeanFactory beanFactory, AutoConfigureEmbeddedDatabase annotation) {
        if (StringUtils.isNotBlank(annotation.beanName())) {
            if (beanFactory.containsBean(annotation.beanName())) {
                BeanDefinition beanDefinition = beanFactory.getBeanDefinition(annotation.beanName());
                return new BeanDefinitionHolder(beanDefinition, annotation.beanName());
            } else {
                return new BeanDefinitionHolder(new RootBeanDefinition(), annotation.beanName());
            }
        }

        String[] beanNames = beanFactory.getBeanNamesForType(DataSource.class);

        if (ObjectUtils.isEmpty(beanNames)) {
            throw new IllegalStateException("No DataSource beans found, embedded version will not be used, " +
                    "you must specify data source name - use @AutoConfigureEmbeddedDatabase(beanName = \"dataSource\") annotation");
        }

        if (beanNames.length == 1) {
            String beanName = beanNames[0];
            BeanDefinition beanDefinition = beanFactory.getBeanDefinition(beanName);
            return new BeanDefinitionHolder(beanDefinition, beanName);
        }

        for (String beanName : beanNames) {
            BeanDefinition beanDefinition = beanFactory.getBeanDefinition(beanName);
            if (beanDefinition.isPrimary()) {
                return new BeanDefinitionHolder(beanDefinition, beanName);
            }
        }

        throw new IllegalStateException("No primary DataSource found, embedded version will not be used");
    }

    protected static BeanDefinitionHolder getDataSourceContextBeanDefinition(ConfigurableListableBeanFactory beanFactory, FlywayTest[] annotations) {
        String flywayBeanName = resolveFlywayBeanName(annotations);

        if (StringUtils.isNotBlank(flywayBeanName)) {
            String contextBeanName = flywayBeanName + "DataSourceContext";
            if (beanFactory.containsBean(contextBeanName)) {
                BeanDefinition beanDefinition = beanFactory.getBeanDefinition(contextBeanName);
                return new BeanDefinitionHolder(beanDefinition, contextBeanName);
            } else {
                return null;
            }
        }

        String[] beanNames = beanFactory.getBeanNamesForType(FlywayDataSourceContext.class);

        if (ObjectUtils.isEmpty(beanNames)) {
            return null;
        }

        if (beanNames.length == 1) {
            String beanName = beanNames[0];
            BeanDefinition beanDefinition = beanFactory.getBeanDefinition(beanName);
            return new BeanDefinitionHolder(beanDefinition, beanName);
        }

        for (String beanName : beanNames) {
            BeanDefinition beanDefinition = beanFactory.getBeanDefinition(beanName);
            if (beanDefinition.isPrimary()) {
                return new BeanDefinitionHolder(beanDefinition, beanName);
            }
        }

        return null;
    }

    protected static BeanDefinitionHolder getFlywayBeanDefinition(ConfigurableListableBeanFactory beanFactory, FlywayTest[] annotations) {
        if (annotations.length > 1) {
            return null; // optimized loading is not supported yet when using multiple flyway test annotations
        }

        String flywayBeanName = resolveFlywayBeanName(annotations);

        if (StringUtils.isNotBlank(flywayBeanName)) {
            BeanDefinition beanDefinition = beanFactory.getBeanDefinition(flywayBeanName);
            return new BeanDefinitionHolder(beanDefinition, flywayBeanName);
        }

        String[] beanNames = beanFactory.getBeanNamesForType(Flyway.class);

        if (ObjectUtils.isEmpty(beanNames)) {
            return null;
        }

        if (beanNames.length == 1) {
            String beanName = beanNames[0];
            BeanDefinition beanDefinition = beanFactory.getBeanDefinition(beanName);
            return new BeanDefinitionHolder(beanDefinition, beanName);
        }

        for (String beanName : beanNames) {
            BeanDefinition beanDefinition = beanFactory.getBeanDefinition(beanName);
            if (beanDefinition.isPrimary()) {
                return new BeanDefinitionHolder(beanDefinition, beanName);
            }
        }

        return null;
    }

    protected static FlywayTest[] findFlywayTestAnnotations(AnnotatedElement element) {
        if (repeatableAnnotationPresent) {
            org.flywaydb.test.annotation.FlywayTests flywayContainerAnnotation =
                    AnnotatedElementUtils.findMergedAnnotation(element, org.flywaydb.test.annotation.FlywayTests.class);
            if (flywayContainerAnnotation != null) {
                return flywayContainerAnnotation.value();
            }
        }

        FlywayTest flywayAnnotation = AnnotatedElementUtils.findMergedAnnotation(element, FlywayTest.class);
        if (flywayAnnotation != null) {
            return new FlywayTest[] { flywayAnnotation };
        }

        return new FlywayTest[0];
    }

    protected static String resolveFlywayBeanName(FlywayTest[] annotations) {
        FlywayTest annotation = annotations.length == 1 ? annotations[0] : null;
        if (annotation != null && flywayNameAttributePresent) {
            return annotation.flywayName();
        } else {
            return null;
        }
    }
}
