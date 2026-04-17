from flask import Flask, request, jsonify
from neo4j import GraphDatabase
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count


load_dotenv()
neo4j_password = os.getenv("NEO4J_PASS")

app = Flask(__name__)
spark = SparkSession.builder.appName("TaxiSparkAPI").getOrCreate()

URI = "bolt://localhost:7687"
AUTH = ("neo4j", neo4j_password)
driver = GraphDatabase.driver(URI, auth=AUTH)

# --- Endpoint 1: Graph Summary ---
@app.route('/graph-summary', methods=['GET'])
def graph_summary():
    query = """
    MATCH (d:Driver) WITH count(d) AS dc
    MATCH (c:Company) WITH dc, count(c) AS cc
    MATCH (a:Area) WITH dc, cc, count(a) AS ac
    MATCH ()-[t:TRIP]->() 
    RETURN dc AS driver_count, cc AS company_count, ac AS area_count, count(t) AS trip_count
    """
    with driver.session() as session:
        result = session.run(query).single()
        return jsonify({
            "driver_count": result["driver_count"],
            "company_count": result["company_count"],
            "area_count": result["area_count"],
            "trip_count": result["trip_count"]
        })

# --- Endpoint 2: Top Companies ---
@app.route('/top-companies', methods=['GET'])
def top_companies():
    n = int(request.args.get('n', 10))
    query = """
    MATCH (c:Company)<-[:WORKS_FOR]-(:Driver)-[t:TRIP]->()
    RETURN c.name AS name, count(t) AS trip_count
    ORDER BY trip_count DESC
    LIMIT $n
    """
    with driver.session() as session:
        results = session.run(query, n=n)
        companies = [{"name": record["name"], "trip_count": record["trip_count"]} for record in results]
        return jsonify({"companies": companies})

# --- Endpoint 3: High-Fare Trips ---
@app.route('/high-fare-trips', methods=['GET'])
def high_fare_trips():
    area_id = int(request.args.get('area_id'))
    min_fare = float(request.args.get('min_fare'))
    query = """
    MATCH (d:Driver)-[t:TRIP]->(a:Area)
    WHERE a.area_id = $area_id AND t.fare > $min_fare
    RETURN t.trip_id AS trip_id, t.fare AS fare, d.driver_id AS driver_id
    ORDER BY fare DESC
    """
    with driver.session() as session:
        results = session.run(query, area_id=area_id, min_fare=min_fare)
        trips = [{"trip_id": record["trip_id"], "fare": record["fare"], "driver_id": record["driver_id"]} for record in results]
        return jsonify({"trips": trips})

# --- Endpoint 4: Co-Area Drivers ---
@app.route('/co-area-drivers', methods=['GET'])
def co_area_drivers():
    driver_id = request.args.get('driver_id')
    query = """
    MATCH (d1:Driver {driver_id: $driver_id})-[:TRIP]->(a:Area)<-[:TRIP]-(d2:Driver)
    WHERE d1 <> d2
    RETURN d2.driver_id AS driver_id, count(DISTINCT a) AS shared_areas
    ORDER BY shared_areas DESC
    """
    with driver.session() as session:
        results = session.run(query, driver_id=driver_id)
        co_drivers = [{"driver_id": record["driver_id"], "shared_areas": record["shared_areas"]} for record in results]
        return jsonify({"co_area_drivers": co_drivers})

# --- Endpoint 5: Average Fare by Company ---
@app.route('/avg-fare-by-company', methods=['GET'])
def avg_fare_by_company():
    query = """
    MATCH (c:Company)<-[:WORKS_FOR]-(:Driver)-[t:TRIP]->()
    RETURN c.name AS name, round(avg(t.fare), 2) AS avg_fare
    ORDER BY avg_fare DESC
    """
    with driver.session() as session:
        results = session.run(query)
        companies = [{"name": record["name"], "avg_fare": record["avg_fare"]} for record in results]
        return jsonify({"companies": companies})
    
# --- Spark Endpoint 1: Area Trip Stats ---
@app.route('/area-stats', methods=['GET'])
def area_stats():
    area_id = int(request.args.get('area_id'))
    
    # Read the data
    df = spark.read.csv("taxi_trips_clean.csv", header=True, inferSchema=True)
    
    # Filter, group, and aggregate
    stats = df.filter(col("dropoff_area") == area_id) \
              .agg(
                  count("*").alias("trip_count"),
                  avg("fare").alias("avg_fare"),
                  avg("trip_seconds").alias("avg_trip_seconds")
              ).collect()[0]
              
    return jsonify({
        "area_id": area_id,
        "trip_count": stats["trip_count"] or 0,
        "avg_fare": round(stats["avg_fare"], 2) if stats["avg_fare"] else 0.0,
        "avg_trip_seconds": int(stats["avg_trip_seconds"]) if stats["avg_trip_seconds"] else 0
    })

# --- Spark Endpoint 2: Top Pickup Areas ---
@app.route('/top-pickup-areas', methods=['GET'])
def top_pickup_areas():
    n = int(request.args.get('n', 10))
    
    df = spark.read.csv("taxi_trips_clean.csv", header=True, inferSchema=True)
    
    # GroupBy, aggregate, order, and limit (No pandas used here per instructions!)
    top_areas = df.groupBy("pickup_area") \
                  .agg(count("*").alias("trip_count")) \
                  .orderBy(col("trip_count").desc()) \
                  .limit(n) \
                  .collect()
                  
    areas_list = [{"pickup_area": row["pickup_area"], "trip_count": row["trip_count"]} for row in top_areas]
    return jsonify({"areas": areas_list})

# --- Spark Endpoint 3: Company Comparison (Spark SQL) ---
@app.route('/company-compare', methods=['GET'])
def company_compare():
    c1 = request.args.get('company1')
    c2 = request.args.get('company2')
    
    df = spark.read.csv("taxi_trips_clean.csv", header=True, inferSchema=True)
    
    # Add fare_per_minute column
    df = df.withColumn("fare_per_minute", col("fare") / (col("trip_seconds") / 60.0))
    
    # Register as temp view
    df.createOrReplaceTempView("trips")
    
    # Single Spark SQL query
    query = f"""
        SELECT company, 
               COUNT(*) AS trip_count, 
               ROUND(AVG(fare), 2) AS avg_fare, 
               ROUND(AVG(fare_per_minute), 2) AS avg_fare_per_minute,
               CAST(ROUND(AVG(trip_seconds)) AS INT) AS avg_trip_seconds
        FROM trips 
        WHERE company IN ('{c1}', '{c2}')
        GROUP BY company
    """
    results = spark.sql(query).collect()
    
    # Check if both companies were found
    found_companies = [row["company"] for row in results]
    if c1 not in found_companies or c2 not in found_companies:
        return jsonify({"error": "one or more companies not found"})
            
    comparison = [row.asDict() for row in results]
    return jsonify({"comparison": comparison})

if __name__ == '__main__':
    # Run the app on 0.0.0.0 to allow external access, on port 5000
    app.run(host='0.0.0.0', port=5000)