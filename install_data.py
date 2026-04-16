from neo4j import GraphDatabase 
import os
from dotenv import load_dotenv

load_dotenv()
neo4j_password = os.getenv("NEO4J_PASS")

driver = GraphDatabase.driver("bolt://35.226.166.143:7687", auth=("neo4j", "neo4j_password"))

print(driver.get_server_info()) 

