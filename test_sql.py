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


print("\nTest Case 1: Basic Filtered Aggregation")
run_sql_query("""
SELECT 
    cust,
    SUM(CASE WHEN state='NY' THEN quant ELSE 0 END) AS sum_quant_NY,
    AVG(CASE WHEN state='NY' THEN quant ELSE NULL END) AS avg_quant_NY,
    SUM(CASE WHEN state='NJ' THEN quant ELSE 0 END) AS sum_quant_NJ,
    SUM(CASE WHEN state='CT' THEN quant ELSE 0 END) AS sum_quant_CT,
    AVG(CASE WHEN state='CT' THEN quant ELSE NULL END) AS avg_quant_CT
FROM 
    sales
GROUP BY 
    cust
HAVING 
    SUM(CASE WHEN state='NY' THEN quant ELSE 0 END) > 2 * SUM(CASE WHEN state='NJ' THEN quant ELSE 0 END)
    OR 
    AVG(CASE WHEN state='NY' THEN quant ELSE NULL END) > AVG(CASE WHEN state='CT' THEN quant ELSE NULL END)
""")


run_sql_query("""
SELECT cust, prod, SUM(quant) AS sum_quant
FROM sales
WHERE year = 2016
GROUP BY cust, prod
HAVING SUM(quant) > 500
""")