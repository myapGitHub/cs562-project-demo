import psycopg2
from tabulate import tabulate

# Database configuration - replace with your credentials
DB_CONFIG = {
    'host': 'localhost',
    'database': 'cs562_project',
    'user': 'yjiang',
    'password': 'cs561'
}

def run_sql_query(query):
    """Execute a SQL query and return formatted results"""
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                
                # Get column names
                columns = [desc[0] for desc in cur.description]
                
                # Fetch all rows
                rows = cur.fetchall()
                
                # Print results
                print(tabulate(rows, headers=columns, tablefmt='psql'))
                
                return rows
                
    except Exception as e:
        print(f"Error executing query: {e}")
        return None

# Test Case 1
print("\nTest Case 1: Basic Filtered Aggregation")
run_sql_query("""
SELECT cust, SUM(quant) AS sum_quant
FROM sales
WHERE state = 'NY'
GROUP BY cust
HAVING SUM(quant) > 1000
""")

# Test Case 2
print("\nTest Case 2: Average with Filter")
run_sql_query("""
SELECT prod, AVG(quant) AS avg_quant
FROM sales
WHERE year = 2023
GROUP BY prod
HAVING AVG(quant) > 50
""")

# Test Case 3
print("\nTest Case 3: Count Distinct Products")
run_sql_query("""
SELECT cust, COUNT(DISTINCT prod) AS count_prod
FROM sales
WHERE month = 12
GROUP BY cust
HAVING COUNT(DISTINCT prod) >= 3
""")
