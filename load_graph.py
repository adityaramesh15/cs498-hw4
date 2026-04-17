from neo4j import GraphDatabase 
import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
neo4j_password = os.getenv("NEO4J_PASS")

# driver = GraphDatabase.driver("bolt://35.226.166.143:7687", auth=("neo4j", neo4j_password))

# print(driver.get_server_info()) 


URI = "bolt://34.123.109.175:7687" 
AUTH = ("neo4j", neo4j_password)

def load_data():
    df = pd.read_csv("taxi_trips_clean.csv")
    # Convert dataframe to a list of dictionaries for bulk loading
    records = df.to_dict('records')
    
    query = """
    UNWIND $batch AS row
    
    // 1. Merge Nodes (MERGE prevents duplicates)
    MERGE (d:Driver {driver_id: toString(row.driver_id)})
    MERGE (c:Company {name: toString(row.company)})
    MERGE (dropoff:Area {area_id: toInteger(row.dropoff_area)})
    
    // 2. Merge Relationships
    MERGE (d)-[:WORKS_FOR]->(c)
    MERGE (d)-[:TRIP {
        trip_id: toString(row.trip_id),
        fare: toFloat(row.fare),
        trip_seconds: toInteger(row.trip_seconds)
    }]->(dropoff)
    """
    
    driver = GraphDatabase.driver(URI, auth=AUTH)
    
    try:
        driver.verify_connectivity()
        print("Successfully connected to Neo4j!")
    except Exception as e:
        print(f"Failed to connect: {e}")
        exit(1)
        
    with driver.session() as session:
        print("Loading data into Neo4j... this may take a moment.")
        session.run(query, records=records)
        print("Data successfully loaded!")
        
    driver.close()

if __name__ == "__main__":
    load_data()