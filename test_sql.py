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


print("\nAssignment Example")
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

print("\nTest Case 1")
run_sql_query("""
SELECT cust, SUM(quant) AS sum_quant
FROM sales
WHERE state = 'NY'
GROUP BY cust;
              """)

print("\nTest Case 2")
run_sql_query("""
SELECT cust, 
       SUM(CASE WHEN state = 'NY' THEN quant ELSE 0 END) AS sum_quant_NY,
       SUM(CASE WHEN state = 'NJ' THEN quant ELSE 0 END) AS sum_quant_NJ
FROM sales
GROUP BY cust
HAVING SUM(CASE WHEN state = 'NY' THEN quant ELSE 0 END) > 
       SUM(CASE WHEN state = 'NJ' THEN quant ELSE 0 END);
              """)

print("\nTest Case 3")
run_sql_query("""
SELECT cust, prod, SUM(quant) AS sum_quant
FROM sales
WHERE state = 'NY' AND year = '2020'
GROUP BY cust, prod;
              """)

print("\nTest Case 4")
run_sql_query("""
SELECT cust,
       SUM(quant) AS sum_quant,
       AVG(quant) AS avg_quant,
       MAX(quant) AS max_quant
FROM sales
WHERE prod = 'Butter'
GROUP BY cust
HAVING AVG(quant) > 100;""")

print("\nTest Case 5")
run_sql_query("""
SELECT cust,
       SUM(CASE WHEN state = 'NY' AND month = '1' THEN quant ELSE 0 END) AS sum_quant_NY_Jan,
       SUM(CASE WHEN state = 'NJ' AND month = '2' THEN quant ELSE 0 END) AS sum_quant_NJ_Feb,
       SUM(CASE WHEN state = 'CT' AND month = '3' THEN quant ELSE 0 END) AS sum_quant_CT_Mar
FROM sales
GROUP BY cust
HAVING (SUM(CASE WHEN state = 'NY' AND month = '1' THEN quant ELSE 0 END) > 
        SUM(CASE WHEN state = 'NJ' AND month = '2' THEN quant ELSE 0 END) + 
        SUM(CASE WHEN state = 'CT' AND month = '3' THEN quant ELSE 0 END))
       OR 
       (SUM(CASE WHEN state = 'NY' AND month = '1' THEN quant ELSE 0 END) > 1000);
              """)

print("\nTest Case 6")
run_sql_query("""
SELECT cust, prod, SUM(quant) AS sum_quant
FROM sales
GROUP BY cust, prod
HAVING SUM(quant) > 500;
              """)

print("\nTest Case 7")
run_sql_query("""
SELECT cust, state, SUM(quant) AS sum_quant
FROM sales
WHERE prod = 'Butter' OR prod = 'Ice'
GROUP BY cust, state
HAVING SUM(quant) > 300;
              """)

print("\nTest Case 8")
run_sql_query("""
SELECT cust,
       SUM(CASE WHEN year = '2016' THEN quant ELSE 0 END) AS sum_quant_2016,
       SUM(CASE WHEN year = '2017' THEN quant ELSE 0 END) AS sum_quant_2017
FROM sales
GROUP BY cust
HAVING SUM(CASE WHEN year = '2017' THEN quant ELSE 0 END) > 
       SUM(CASE WHEN year = '2016' THEN quant ELSE 0 END);
              """)