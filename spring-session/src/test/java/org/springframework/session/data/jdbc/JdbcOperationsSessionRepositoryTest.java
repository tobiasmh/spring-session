package org.springframework.session.data.jdbc;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.*;
import static org.springframework.session.data.jdbc.JdbcOperationsSessionRepository.CREATION_TIME_ATTR;
import static org.springframework.session.data.jdbc.JdbcOperationsSessionRepository.LAST_ACCESSED_ATTR;
import static org.springframework.session.data.jdbc.JdbcOperationsSessionRepository.MAX_INACTIVE_ATTR;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.session.ExpiringSession;
import org.springframework.session.MapSession;
import org.springframework.session.data.jdbc.JdbcOperationsSessionRepository.JdbcSession;
import org.springframework.util.SerializationUtils;

@RunWith(MockitoJUnitRunner.class)
public class JdbcOperationsSessionRepositoryTest {

	private JdbcOperationsSessionRepository jdbcOperationsSessionRepository;
	
	private NamedParameterJdbcTemplate jdbcTemplate;
	
	@Before
	public void setup() throws Exception {				
//		EmbeddedDatabase embeddedDatabase = new EmbeddedDatabaseBuilder().setType(EmbeddedDatabaseType.H2).build();
		DriverManagerDataSource embeddedDatabase = new DriverManagerDataSource("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false;MODE=MySQL;");
		this.jdbcTemplate = new NamedParameterJdbcTemplate(embeddedDatabase);
		this.jdbcOperationsSessionRepository = new JdbcOperationsSessionRepository(this.jdbcTemplate);
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void constructorNullConnectionFactory() {
		new JdbcOperationsSessionRepository((NamedParameterJdbcTemplate)(null));
	}
	
	@Test
	public void createSessionDefaultMaxInactiveInterval() throws Exception {
		ExpiringSession session = jdbcOperationsSessionRepository.createSession();
		assertThat(session.getMaxInactiveIntervalInSeconds()).isEqualTo(new MapSession().getMaxInactiveIntervalInSeconds());
	}
	
	@Test
	public void createSessionCustomMaxInactiveInterval() throws Exception {
		int interval = 1;
		jdbcOperationsSessionRepository.setDefaultMaxInactiveInterval(interval);
		ExpiringSession session = jdbcOperationsSessionRepository.createSession();
		assertThat(session.getMaxInactiveIntervalInSeconds()).isEqualTo(interval);
	}
	
	@Test
	public void saveNewSession() {
		JdbcSession session = jdbcOperationsSessionRepository.createSession();

		jdbcOperationsSessionRepository.save(session);
		
		List<Map<String, Object>> result = this.jdbcTemplate.queryForList("SELECT * FROM spring_sessions WHERE session_id = :session_id"
				, new MapSqlParameterSource("session_id", session.getId()));

		Map<String,Object> delta = result.get(0);
		assertThat(delta.size()).isEqualTo(4);
		Object creationTime = delta.get(CREATION_TIME_ATTR);
		assertThat(creationTime).isInstanceOf(Long.class);
		assertThat(delta.get(MAX_INACTIVE_ATTR)).isEqualTo(MapSession.DEFAULT_MAX_INACTIVE_INTERVAL_SECONDS);
		assertThat(delta.get(LAST_ACCESSED_ATTR)).isEqualTo(creationTime);
	}
	
	@Test
	public void saveLastAccessChanged() {
		JdbcSession session = jdbcOperationsSessionRepository.new JdbcSession(new MapSession());
		session.setLastAccessedTime(12345678L);

		jdbcOperationsSessionRepository.save(session);
		
		List<Map<String, Object>> result = this.jdbcTemplate.queryForList("SELECT * FROM spring_sessions WHERE session_id = :session_id"
				, new MapSqlParameterSource("session_id", session.getId()));

		Map<String,Object> delta = result.get(0);

		assertThat(delta.get(LAST_ACCESSED_ATTR)).isEqualTo(session.getLastAccessedTime());
	}

	@Test
	public void saveSetAttribute() {
		String attrName = "attrName";
		JdbcSession session = jdbcOperationsSessionRepository.new JdbcSession(new MapSession());
		session.setAttribute(attrName, "attrValue");
		
		jdbcOperationsSessionRepository.save(session);
		
		byte[] attributeBytes = this.jdbcTemplate.queryForObject("SELECT attributeValue FROM spring_sessions_attributes WHERE session_id = :session_id AND attributeName = 'attrName'"
				, new MapSqlParameterSource("session_id", session.getId()), byte[].class);

		String attributeValue = (String) SerializationUtils.deserialize(attributeBytes);		
		assertThat(attributeValue).isEqualTo("attrValue");
	}
	
	@Test
	public void saveSetAttributesAreSerializedCorrectly() {
		Map<String, String> testMap = new HashMap<String, String>();
		testMap.put("testKey", "testValue");		
		JdbcSession session = jdbcOperationsSessionRepository.createSession();
		session.setAttribute("testMap", testMap);
		jdbcOperationsSessionRepository.save(session);
		JdbcSession retrievedSession = jdbcOperationsSessionRepository.getSession(session.getId());
		Map<String, String> retrievedMap = (Map<String, String>)retrievedSession.getAttribute("testMap");
		assertThat(retrievedMap.get("testKey")).isEqualTo("testValue");
		
	}

	@Test
	public void saveRemoveAttribute() {
		String attrName = "attrName";
		JdbcSession session = jdbcOperationsSessionRepository.new JdbcSession(new MapSession());
		session.removeAttribute(attrName);
		
		jdbcOperationsSessionRepository.save(session);
		
		List<Map<String, Object>> result = this.jdbcTemplate.queryForList("SELECT * FROM spring_sessions WHERE session_id = :session_id"
				, new MapSqlParameterSource("session_id", session.getId()));

		Map<String,Object> delta = result.get(0);
		
		assertThat(delta.get(attrName)).isNull();
	}
	
	@Test
	public void jdbcSessionGetAttributes() {
		String attrName = "attrName";
		JdbcSession session = jdbcOperationsSessionRepository.new JdbcSession(new MapSession());
		assertThat(session.getAttributeNames()).isEmpty();
		session.setAttribute(attrName, "attrValue");
		assertThat(session.getAttributeNames()).containsOnly(attrName);
		session.removeAttribute(attrName);
		assertThat(session.getAttributeNames()).isEmpty();
	}

	@Test
	public void delete() {
		JdbcSession session = jdbcOperationsSessionRepository.createSession();
		jdbcOperationsSessionRepository.save(session);
		List<Map<String, Object>> result = this.jdbcTemplate.queryForList("SELECT * FROM spring_sessions "
				+ "WHERE session_id = :session_id"
				, new MapSqlParameterSource("session_id", session.getId()));
		assertThat(result.size()).isEqualTo(1);
		jdbcOperationsSessionRepository.delete(session.getId());
		List<Map<String, Object>> afterDeleteResult = this.jdbcTemplate.queryForList("SELECT * FROM spring_sessions "
				+ "WHERE session_id = :session_id"
				, new MapSqlParameterSource("session_id", session.getId()));
		assertThat(afterDeleteResult.size()).isEqualTo(0);
	}

	@Test
	public void deleteNullSession() {		
		// Ensure can run without throwing an exception
		jdbcOperationsSessionRepository.delete(null); 		
	}
	
	@Test
	public void getSessionNotFound() {
		String id = UUID.randomUUID().toString();
		assertThat(jdbcOperationsSessionRepository.getSession(id)).isNull();
	}

	@Test
	public void getSessionFound() {
		String attrName = "attrName";
		JdbcSession expected = jdbcOperationsSessionRepository.new JdbcSession();
		expected.setLastAccessedTime(System.currentTimeMillis() - 60000);
		expected.setAttribute(attrName, "attrValue");
		jdbcOperationsSessionRepository.save(expected);

		long now = System.currentTimeMillis();
		JdbcSession session = jdbcOperationsSessionRepository.getSession(expected.getId());
		assertThat(session.getId()).isEqualTo(expected.getId());
		assertThat(session.getAttributeNames()).isEqualTo(expected.getAttributeNames());
		assertThat(session.getAttribute(attrName)).isEqualTo(expected.getAttribute(attrName));
		assertThat(session.getCreationTime()).isEqualTo(expected.getCreationTime());
		assertThat(session.getMaxInactiveIntervalInSeconds()).isEqualTo(expected.getMaxInactiveIntervalInSeconds());
		assertThat(session.getLastAccessedTime()).isGreaterThanOrEqualTo(now);

	}

	@Test
	public void getSessionExpired() {
		JdbcSession expected = jdbcOperationsSessionRepository.new JdbcSession();
		expected.setLastAccessedTime(System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(35));
		jdbcOperationsSessionRepository.save(expected);				
		assertThat(jdbcOperationsSessionRepository.getSession(expected.getId())).isNull();
	}

	@Test
	public void cleanupExpiredSessions() {
		JdbcSession expected = jdbcOperationsSessionRepository.new JdbcSession();
		expected.setLastAccessedTime(System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(35));
		jdbcOperationsSessionRepository.save(expected);				
		
		jdbcOperationsSessionRepository.cleanupExpiredSessions();

		Long expiredSessionCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM spring_sessions WHERE session_id = :session_id"
				, new MapSqlParameterSource("session_id", expected.getId()), Long.class);
		
		assertTrue(expiredSessionCount == 0);
		
	}
		
}
