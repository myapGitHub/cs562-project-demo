import subprocess

#Asks for query from user interactively to feed a string into getArgs()
def getArgsManual():
    print("Input query: ")
    user_input = []
    while True:
        line = input()
        if line.strip() == '':
            break
        user_input.append(line)
    return getArgs("\n".join(user_input))

#Asks for query from user via file to feed the contents into getArgs()
def getArgsFromFile(filename):
    print(f"Using {filename} as file input", filename)
    with open(filename, 'r') as file:
        contents = file.read()
    return getArgs(contents)

#Reads the input and places them into a dictionary
def getArgs(query):
    #print("we are in")
    queryDict = {
        's': [],      # SELECT ATTRIBUTE(S)
        'n': 0,       # NUMBER OF GROUPING VARIABLES(n)
        'v': [],      # GROUPING ATTRIBUTES(v)
        'f': [],      # F-VECT(f)
        'sigma': {},  # SELECT CONDITION-VECT(sigma)
        'g': ''       # HAVING CONDITION(g)
    }

    lines = iter(query.strip().splitlines())
    for line in lines: 
        #Go thru all the lines in the input
        #Each line gets the arguments for hte corresponding FI operator value
        line = line.strip()
        if len(line) == 0: #Not a FI operator, or empty
            continue

        #Get all values of select attribute and store it into the queryDict with key 's'
        if line.startswith("SELECT ATTRIBUTE"):
            args = next(lines, None)
            lst = []
            if args and len(args) > 0:
                for value in args.split(','):
                    lst.append(value.strip())
                queryDict['s'] = lst
            else:
                queryDict['s'] = []

        #Get value of grouping variables and store it into the queryDict with key 'n'
        elif line.startswith("NUMBER OF GROUPING VARIABLES"):
            args = next(lines, None)
            if args and len(args) > 0:
                queryDict['n'] = int(args.strip())
            else:
                queryDict['n'] = 0

        #Get all values of grouping attribute and store it into the queryDict with key 'v'
        elif line.startswith("GROUPING ATTRIBUTES"):
            args = next(lines, None)
            lst = []
            if args and len(args) > 0:
                for value in args.split(','):
                    lst.append(value.strip())
                queryDict['v'] = lst
            else:
                queryDict['v'] = []

        #Get all values of F-Vector aggregates and store it into the queryDict with key 'f'
        elif line.startswith("F-VECT"):
            args = next(lines, None)
            lst = []
            if args and len(args) > 0:
                for value in args.split(','):
                    lst.append(value.strip())
                queryDict['f'] = lst
            else:
                queryDict['f'] = []
           
        #Get all values of such that attribute and store it into the queryDict with key 'sigma'
        #Additionally, get expression of having and store it into queryDict with key 'g'     
        elif line.startswith("SELECT CONDITION-VECT"):
            #print("COND VECT")
            while True:
                args = next(lines, None)
                if args is None or args.strip() == '':
                    break
                #HAVING CONDITION 
                #Added it in sigma to prevent it from being skipped
                elif args.strip().startswith("HAVING_CONDITION"):
                    args = next(lines, None)
                    if args and len(args) > 0:
                        queryDict['g'] = args.strip()
                    else:
                        queryDict['g'] = ''
                    break
                elif '.' in args:
                    idx, condition = args.strip().split('.', 1)
                    queryDict['sigma'][int(idx.strip())] = condition.strip().replace('’', "'")
    #print(queryDict)
    return queryDict

    
#Calculates the aggregate based on input
def aggregateFunctions(aggregate):
    result = 0
    if aggregate.equals("max"):
        pass
    elif aggregate.equals("count"):
        pass
    elif aggregate.equals("avg"):
        pass
    elif aggregate.equals("min"):
        pass
    elif aggregate.equals("sum"):
        pass
    else: #Error, we work with the 5 aggregates we learned in class only
        raise TypeError("Type of aggregate must be max, count, avg, min, or sum")
    return result
    
def main():
    """
    This is the generator code. It should take in the MF structure and generate the code
    needed to run the query. That generated code should be saved to a 
    file (e.g. _generated.py) and then run.
    """
    
    #Get input from user to ask whether they want to use a file or manually input the query
    whileLoop = 1
    while(whileLoop):
        inputType = input("Please indicate whether you would like you input the query using manual input(0) or a file(1).\n")
        #print(inputType)
        inputType = int(inputType)
        if inputType != 1 and inputType != 0:
            print("The selected input is not 0 or 1, please input a valid option.")
        else:
            whileLoop = 0


    #Process their input and store it into query. From a file, or manual, query will be a dictionary with FI operators as keys, and the corresponding value(s) for each operator
    if inputType == 1:
        filename = input("Please enter a valid textfile name that holds your query: ")
        query = getArgsFromFile(filename)
        
    if inputType == 0:
        query = getArgsManual()
        
    #print(query)
      
      
    select_attrs = ", ".join(query['s']) if query['s'] else "*"
    sql_query = f"SELECT {select_attrs} FROM sales"  
    
    
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
    cur.execute(\"\"\"{sql_query}\"\"\")
    
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
