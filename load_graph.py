from neo4j import GraphDatabase 
import os
from dotenv import load_dotenv
import pandas as pd

load_dotenv()
neo4j_password = os.getenv("NEO4J_PASS")

# driver = GraphDatabase.driver("bolt://35.226.166.143:7687", auth=("neo4j", neo4j_password))

# print(driver.get_server_info()) 


URI = "bolt://35.226.166.143:7687" 
AUTH = ("neo4j", neo4j_password)

def load_data():
    df = pd.read_csv("taxi_trips_clean.csv")
    # Convert dataframe to a list of dictionaries for bulk loading
    records = df.to_dict('records')
    
    query = """
    UNWIND $records AS row
    
    // Create/Merge Nodes
    MERGE (d:Driver {driver_id: row.driver_id})
    MERGE (c:Company {name: row.company})
    MERGE (a:Area {area_id: row.dropoff_area})
    
    // Create/Merge Relationships
    MERGE (d)-[:WORKS_FOR]->(c)
    MERGE (d)-[t:TRIP {trip_id: row.trip_id}]->(a)
    ON CREATE SET t.fare = row.fare, t.trip_seconds = row.trip_seconds
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