import subprocess

#Reads the input and places them into a dictionary
def getArgsFile(query):
    queryDict = {
    's': [],  # SELECT ATTRIBUTE(S)
    'n': 0,    # NUMBER OF GROUPING VARIABLES
    'v': [],   # GROUPING ATTRIBUTES
    'f': [],   # F-VECT
    'sigma': [], # SELECT CONDITION-VECT
    'g': ''    # HAVING CONDITION
    }
    lines = query.strip().splitlines()

    for line in lines: #Iterate thru the lines to get args
        line = line.strip()
        line_length = len(line)
        if line_length == 0: #Line is empty, so go to next operator
            continue

        if line.startswith("SELECT ATTRIBUTE"):
            args = next(lines, None) 
            attribute_list = []
            if args: #If it is not empty
                for attribute in args.split(','):
                    attribute_list.append(attribute.strip())
            queryDict['s'] = attribute_list 

        elif line.startswith("NUMBER OF GROUPING VARIABLES"):


        elif line.startswith("GROUPING ATTRIBUTES"):


        elif line.startswith("F-VECT"):


        elif line.startswith("SELECT CONDITION-VECT"):


        elif line.startswith("HAVING_CONDITION"):


        else: #If we are in a line with args FOR an operator
            continue

    return queryDict
    
    
def main():
    """
    This is the generator code. It should take in the MF structure and generate the code
    needed to run the query. That generated code should be saved to a 
    file (e.g. _generated.py) and then run.
    """


    #Algorithm goes here
    body = """
    for row in cur:
        if row['quant'] > 10:
            _global.append(row)
    """

    # Note: The f allows formatting with variables.
    #       Also, note the indentation is preserved.
    tmp = f"""
import os
import psycopg2
import psycopg2.extras
import tabulate
from dotenv import load_dotenv

# DO NOT EDIT THIS FILE, IT IS GENERATED BY generator.py

def query():
    load_dotenv()

    user = os.getenv('USER')
    password = os.getenv('PASSWORD')
    dbname = os.getenv('DBNAME')

    conn = psycopg2.connect("dbname="+dbname+" user="+user+" password="+password,
                            cursor_factory=psycopg2.extras.DictCursor)
    cur = conn.cursor()
    cur.execute("SELECT * FROM sales")
    
    _global = []
    {body}
    
    return tabulate.tabulate(_global,
                        headers="keys", tablefmt="psql")

def main():
    print(query())
    
if "__main__" == __name__:
    main()
    """

    # Write the generated code to a file
    open("_generated.py", "w").write(tmp)
    # Execute the generated code
    subprocess.run(["python", "_generated.py"])


if "__main__" == __name__:
    main()
