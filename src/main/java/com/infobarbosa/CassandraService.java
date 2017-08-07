package com.infobarbosa;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.BoundStatement;

public class CassandraService{
	
	private static CassandraService cassandraService;
	private final static String CASSANDRA_ENDPOINT = System.getenv("CASSANDRA_ENDPOINT");
	private final static String CASSANDRA_KEYSPACE = System.getenv("CASSANDRA_KEYSPACE");
	private Cluster cluster = null;
	private Session session = null;

	private CassandraService(){
		cluster = Cluster.builder()
            			 .addContactPoint( CASSANDRA_ENDPOINT )
            			 .build();
    	
    	session = cluster.connect( CASSANDRA_KEYSPACE );
	}

	public static CassandraService getInstance(){
		if( cassandraService == null ){
			cassandraService = new CassandraService();
		}

		return cassandraService;
	}

	public void save(String id, String tweet){
		System.out.println("Salvando tweet com chave " + id );
		PreparedStatement stmt = session.prepare("insert into tweets(id, tweet) values(?, ?)");
		BoundStatement bound = stmt.bind(id, tweet);
		session.execute(bound);
		System.out.println("Tweet com chave " + id + " salvo.");
	}

	public void close(){
		if( cluster != null){
			cluster.close();
		}
	}
}