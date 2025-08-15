package org.neo4j.cdc.client;

public class Neo4j {

    public static String testImage() {
        String image = System.getenv("NEO4J_TEST_IMAGE");
        return image != null ? image : "neo4j:2025-enterprise";
    }
}
