package org.springframework.session.data.jdbc.config.annotation.web.http;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.connection.RedisConnectionFactory;



/**
 * Add this annotation to an {@code @Configuration} class to expose the
 * SessionRepositoryFilter as a bean named "springSessionRepositoryFilter" and
 * backed by Jdbc. In order to leverage the annotation, a single {@link RedisConnectionFactory}
 * must be provided. For example:
 *
 * <pre>
 * {@literal @Configuration}
 * {@literal @EnableJdbcHttpSession}
 * public class JdbcHttpSessionConfig {
 *
 *     	{@literal @Bean}
 *		public DataSource dataSource() {
 *			BasicDataSource dataSource = new BasicDataSource();
 *			dataSource.setDriverClassName("com.mysql.jdbc.Driver");
 *			dataSource.setUrl("jdbc:mysql://mysql.example.com/spring_session");
 *			dataSource.setUsername("spring_session");
 *			dataSource.setPassword("spring_session");
 *			return dataSource;
 *		}
 *
 * }
 * </pre>
 *
 * @author Tobias Montagna-Hay
 * @since 1.0.2
 */
@Retention(value=java.lang.annotation.RetentionPolicy.RUNTIME)
@Target(value={java.lang.annotation.ElementType.TYPE})
@Documented
@Import(JdbcHttpSessionConfiguration.class)
@Configuration
public @interface EnableJdbcHttpSession {
	int maxInactiveIntervalInSeconds() default 1800;
}
