package org.springframework.session.data.jdbc;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.session.ExpiringSession;
import org.springframework.session.MapSession;
import org.springframework.session.Session;
import org.springframework.session.SessionRepository;
import org.springframework.session.data.redis.SessionMessageListener;
import org.springframework.session.events.SessionDestroyedEvent;
import org.springframework.session.web.http.SessionRepositoryFilter;
import org.springframework.util.Assert;
import org.springframework.util.DigestUtils;
import org.springframework.util.SerializationUtils;

/**
 * <p>
 * A {@link org.springframework.session.SessionRepository} that is implemented
 * using Spring Data's
 * {@link org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate}. In a web
 * environment, this is typically used in combination with
 * {@link SessionRepositoryFilter}. This implementation supports
 * {@link SessionDestroyedEvent} through {@link SessionMessageListener}.
 * </p>
 *
 * <h2>Creating a new instance</h2>
 *
 * A typical example of how to create a new instance can be seen below:
 *
 * <pre>
 * NamedParameterJdbcTemplate jdbcTemplate = new NamedParameterJdbcTemplate();
 *
 * JdbcOperationsSessionRepository jdbcSessionRepository = new JdbcOperationsSessionRepository(
 * 		jdbcTemplate);
 * </pre>
 *
 * <p>
 * For additional information on how to create a JdbcTemplate, refer to the <a
 * href =
 * "http://docs.spring.io/spring/docs/current/spring-framework-reference/html/jdbc.html"
 * >Spring Jdbc Reference</a>.
 * </p>
 *
 * <h2>Storage Details</h2>
 *
 * <p>
 * Each session is stored in SQL Row. An example of how each session is stored can be seen below.
 * </p>
 * 
 * <pre>session-id|creationTime|maxInactiveInterval|lastAccessedTime</pre>
 * <pre>session-id|attributeName|attributeValue</pre>
 *  *
 *
 * <p>
 * The {@link JdbcSession} keeps track of the properties that have changed and
 * only updates those. This means if an attribute is written once and read many
 * times we only need to write that attribute once.
 *
 * </p>
 *
 *
 * @since 1.0.2
 *
 * @author Tobias Montagna-Hay
 */
public class JdbcOperationsSessionRepository implements SessionRepository<JdbcOperationsSessionRepository.JdbcSession> {

	private NamedParameterJdbcTemplate jdbcTemplate;
	
	private Integer defaultMaxInactiveInterval;
	
	/**
	 * The key in the Hash representing {@link org.springframework.session.ExpiringSession#getCreationTime()}
	 */
	static final String CREATION_TIME_ATTR = "creationTime";

	/**
	 * The key in the Hash representing {@link org.springframework.session.ExpiringSession#getMaxInactiveIntervalInSeconds()}
	 */
	static final String MAX_INACTIVE_ATTR = "maxInactiveInterval";

	/**
	 * The key in the Hash representing {@link org.springframework.session.ExpiringSession#getLastAccessedTime()}
	 */
	static final String LAST_ACCESSED_ATTR = "lastAccessedTime";

	/**
	 * The prefix of the key for used for session attributes. The suffix is the name of the session attribute. For
	 * example, if the session contained an attribute named attributeName, then there would be an entry in the hash named
	 * sessionAttr:attributeName that mapped to its value. TODO Update
	 */
	
	static final String SESSION_TABLE_SCRIPT = 
	"CREATE TABLE IF NOT EXISTS spring_sessions ("+
	"session_id varchar(45) NOT NULL PRIMARY KEY, "+
	"creationTime BIGINT DEFAULT NULL, "+
	"maxInactiveInterval INT DEFAULT NULL, "+
	"lastAccessedTime BIGINT DEFAULT NULL "+
	"); ";
	
	static final String SESSION_ATTRIBUTES_TABLE_SCRIPT =
	"CREATE TABLE IF NOT EXISTS spring_sessions_attributes ("+
	"uniqueKey varchar(100) NOT NULL PRIMARY KEY, "+
	"session_id varchar(45) NOT NULL, "+
	"attributeName TEXT DEFAULT NULL, "+
	"attributeValue BLOB DEFAULT NULL"+
	"); ";
	
	public JdbcOperationsSessionRepository(NamedParameterJdbcTemplate jdbcTemplate) {
		Assert.notNull(jdbcTemplate, "jdbcTemplate cannot be null");
		this.jdbcTemplate = jdbcTemplate;
		this.jdbcTemplate.getJdbcOperations().execute(SESSION_TABLE_SCRIPT);
		this.jdbcTemplate.getJdbcOperations().execute(SESSION_ATTRIBUTES_TABLE_SCRIPT);
	}
	
	public JdbcSession createSession() {
		JdbcSession jdbcSession = new JdbcSession();
		if(defaultMaxInactiveInterval != null) {
			jdbcSession.setMaxInactiveIntervalInSeconds(defaultMaxInactiveInterval);
		}
		return jdbcSession;
	}

	public void save(JdbcSession session) {

		this.jdbcTemplate.update("INSERT INTO spring_sessions (session_id) VALUES(:session_id) ON DUPLICATE KEY UPDATE session_id=session_id"
				, new MapSqlParameterSource("session_id", session.getId()));		
		
		session.saveDelta();
		
	}
	
	@Scheduled(cron="0 * * * * *")
	public void cleanupExpiredSessions() {
		String query = "SELECT session_id, maxInactiveInterval, lastAccessedTime FROM spring_sessions WHERE (maxInactiveInterval + lastAccessedTime) < :currentTime"; 
		List<Map<String, Object>> entries = this.jdbcTemplate.queryForList(query, new MapSqlParameterSource("currentTime", System.currentTimeMillis()));		
		for (Map<String, Object> entry : entries) {
			String sessionId = (String)entry.get("session_id");
			delete(sessionId);
		}
	}

	public JdbcSession getSession(String id) {
		return getSession(id, false);
	}
	
	/**
	 *
	 * @param id the session id
	 * @param allowExpired
	 *            if true, will also include expired sessions that have not been
	 *            deleted. If false, will ensure expired sessions are not
	 *            returned.
	 * @return
	 */
	private JdbcSession getSession(String id, boolean allowExpired) {
		List<Map<String, Object>> entries = this.jdbcTemplate.queryForList("SELECT * FROM spring_sessions WHERE session_id = :session_id", new MapSqlParameterSource("session_id", id));
		if(entries.isEmpty()) {
			return null;
		}
		
		Map<String, Object> sourceMap = entries.get(0);
		
		MapSession loaded = new MapSession();
		loaded.setId(id);		
		
		loaded.setCreationTime((Long) sourceMap.get(CREATION_TIME_ATTR));
		loaded.setMaxInactiveIntervalInSeconds((Integer) sourceMap.get(MAX_INACTIVE_ATTR));
		loaded.setLastAccessedTime((Long) sourceMap.get(LAST_ACCESSED_ATTR));
		
		List<Map<String, Object>> attributeEntries = jdbcTemplate.queryForList("SELECT * FROM spring_sessions_attributes "
				+ "WHERE session_id = :session_id"
				, new MapSqlParameterSource("session_id", id));
		
		for (Map<String, Object> entry : attributeEntries) {			
			
			byte[] attributeBytes = (byte[]) entry.get("attributeValue");			
			loaded.setAttribute((String)entry.get("attributeName"), SerializationUtils.deserialize(attributeBytes));
		}
		
		if(!allowExpired && loaded.isExpired()) {
			return null;
		}
		JdbcSession result = new JdbcSession(loaded);
		result.setLastAccessedTime(System.currentTimeMillis());
		return result;
	}

	public void delete(String sessionId) {
		ExpiringSession session = getSession(sessionId, true);
		if(session == null) {
			return;
		}
		jdbcTemplate.update("DELETE FROM spring_sessions WHERE "
				+ "session_id = :session_id"
				, new MapSqlParameterSource("session_id", sessionId));

		jdbcTemplate.update("DELETE FROM spring_sessions_attributes WHERE "
				+ "session_id = :session_id"
				, new MapSqlParameterSource("session_id", sessionId));
		
	}
	
	/**
	 * Sets the maximum inactive interval in seconds between requests before newly created sessions will be
	 * invalidated. A negative time indicates that the session will never timeout. The default is 1800 (30 minutes).
	 *
	 *  @param defaultMaxInactiveInterval the number of seconds that the {@link Session} should be kept alive between
	 *                                    client requests.
	 */
	public void setDefaultMaxInactiveInterval(int defaultMaxInactiveInterval) {
		this.defaultMaxInactiveInterval = defaultMaxInactiveInterval;
	}
	
	final class JdbcSession implements ExpiringSession {
		
		private final MapSession cached;
		private Map<String, Object> sessionInformationDelta = new HashMap<String,Object>();
		private Map<String, Object> attributeDelta = new HashMap<String,Object>();
		
		protected JdbcSession() {
			this(new MapSession());
			sessionInformationDelta.put(CREATION_TIME_ATTR, getCreationTime());
			sessionInformationDelta.put(MAX_INACTIVE_ATTR, getMaxInactiveIntervalInSeconds());
			sessionInformationDelta.put(LAST_ACCESSED_ATTR, getLastAccessedTime());
		}
		
		protected JdbcSession(MapSession cached) {
			this.cached = cached;
		}
		
		public void setLastAccessedTime(long lastAccessedTime) {
			cached.setLastAccessedTime(lastAccessedTime);
			sessionInformationDelta.put(LAST_ACCESSED_ATTR, getLastAccessedTime());
		}

		public String getId() {
			return cached.getId();
		}

		public Object getAttribute(String attributeName) {
			return cached.getAttribute(attributeName);			
		}

		public Set<String> getAttributeNames() {
			return cached.getAttributeNames();
		}

		public void setAttribute(String attributeName, Object attributeValue) {
			cached.setAttribute(attributeName, attributeValue);
			attributeDelta.put(attributeName, attributeValue);
		}

		public void removeAttribute(String attributeName) {
			cached.removeAttribute(attributeName);
			attributeDelta.put(attributeName, null);
		}

		public long getCreationTime() {
			return cached.getCreationTime();
		}

		public long getLastAccessedTime() {
			return cached.getLastAccessedTime();
		}

		public void setMaxInactiveIntervalInSeconds(int interval) {
			cached.setMaxInactiveIntervalInSeconds(interval);
			sessionInformationDelta.put(MAX_INACTIVE_ATTR, getMaxInactiveIntervalInSeconds());			
		}

		public int getMaxInactiveIntervalInSeconds() {
			return cached.getMaxInactiveIntervalInSeconds();
		}

		public boolean isExpired() {
			return cached.isExpired();
		}
		
		private void saveDelta() {			
 			
			for (Entry<String, Object> e : sessionInformationDelta.entrySet()) {
				
				String allowedColumnValues = String.format("%s|%s|%s", CREATION_TIME_ATTR, MAX_INACTIVE_ATTR, LAST_ACCESSED_ATTR);
				if (!Pattern.matches(allowedColumnValues, e.getKey())) { // To prevent injection
					continue;
				
				}
				String updateQuery = String.format("UPDATE spring_sessions SET %s = :value WHERE session_id = :session_id", e.getKey());				
				jdbcTemplate.update(updateQuery, new MapSqlParameterSource("session_id", getId()).addValue("value", e.getValue()));
				
			}			
			sessionInformationDelta = new HashMap<String,Object>(sessionInformationDelta.size());
			
			String insertQuery = "INSERT INTO spring_sessions_attributes (session_id, uniqueKey, attributeName, attributeValue) "
					+ "VALUES (:session_id, :uniqueKey, :attributeName, :attributeValue) ON DUPLICATE KEY "
					+ "UPDATE attributeValue = :attributeValue";
			
			String deleteQuery = "DELETE FROM spring_sessions_attributes WHERE "
					+ "uniqueKey = :uniqueKey";
			
			List<MapSqlParameterSource> insertSqlParameterSourceList = new LinkedList<MapSqlParameterSource>();
			List<MapSqlParameterSource> deleteSqlParameterSourceList = new LinkedList<MapSqlParameterSource>();
			
			for (Entry<String, Object> e : attributeDelta.entrySet()) {
				
				String keyHash = DigestUtils.md5DigestAsHex(e.getKey().getBytes());
				String uniqueKey = String.format("%s_%s", getId(), keyHash);
				
				if (e.getValue() == null) {
					deleteSqlParameterSourceList.add(
							new MapSqlParameterSource("uniqueKey", uniqueKey));
				} else {									
					insertSqlParameterSourceList.add(new MapSqlParameterSource("session_id", getId())
					.addValue("uniqueKey", uniqueKey)
					.addValue("attributeName", e.getKey())					
					.addValue("attributeValue", SerializationUtils.serialize(e.getValue())));
				}
				jdbcTemplate.batchUpdate(deleteQuery, deleteSqlParameterSourceList.toArray(new MapSqlParameterSource[deleteSqlParameterSourceList.size()]));
				jdbcTemplate.batchUpdate(insertQuery, insertSqlParameterSourceList.toArray(new MapSqlParameterSource[insertSqlParameterSourceList.size()]));
				
			}			
			attributeDelta = new HashMap<String,Object>(attributeDelta.size());
						
		}
		
	}
	
}
