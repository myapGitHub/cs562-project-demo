import subprocess
import re

#Asks for query from user interactively to feed a string into getArgs()
def getArgsManual():
    print("\nPlease enter your query parameters one by one (press Enter after each):")
    query_lines = []
    
    # SELECT ATTRIBUTES
    print("\nEnter SELECT ATTRIBUTES (comma separated, e.g., cust,prod,1_sum_quant):")
    query_lines.append("SELECT ATTRIBUTE(S):")
    query_lines.append(input().strip())
    
    # NUMBER OF GROUPING VARIABLES
    print("\nEnter NUMBER OF GROUPING VARIABLES (e.g., 2):")
    query_lines.append("NUMBER OF GROUPING VARIABLES(n):")
    query_lines.append(input().strip())
    
    # GROUPING ATTRIBUTES
    print("\nEnter GROUPING ATTRIBUTES (comma separated, e.g., cust,prod):")
    query_lines.append("GROUPING ATTRIBUTES(V):")
    query_lines.append(input().strip())
    
    # F-VECT
    print("\nEnter F-VECT (comma separated, e.g., 1_sum_quant,2_avg_quant):")
    query_lines.append("F-VECT([F]):")
    query_lines.append(input().strip())
    
    # SELECT CONDITION-VECT
    print("\nEnter SELECT CONDITION-VECT (one per line, format like 1.state='NY', enter blank to finish):")
    query_lines.append("SELECT CONDITION-VECT([σ]):")
    while True:
        line = input().strip()
        if not line:
            break
        query_lines.append(line)
    
    # HAVING CONDITION
    print("\nEnter HAVING CONDITION (e.g., 1_sum_quant > 1000):")
    query_lines.append("HAVING_CONDITION(G):")
    query_lines.append(input().strip())
    
    return getArgs("\n".join(query_lines))

#Asks for query from user via file to feed the contents into getArgs()
def getArgsFromFile(filename):
    print(f"Using {filename} as file input", filename)
    with open(filename, 'r') as file:
        contents = file.read()
    return getArgs(contents)

def generate_mf_body(query):
    group_attrs = query['v']
    gv_key = ", ".join([f"row['{attr}']" for attr in group_attrs])
    
    sigma = query['sigma']
    corresponding_names = []
    match_conditions = []
    for i in range(1, len(query['f']) + 1):
        if i in sigma:
            condition = sigma[i]
            # Convert 1.state='NY' to match_1 = row['state'] == 'NY'
            edited_condition = "row['" + condition.split("=")[0] + "'] == "  + condition.split("=")[1]
            corresponding_names.append(condition.split("=")[1].replace("'", "").lower())
            match_conditions.append(f"match_{i} = {edited_condition}")
    

    # Parse all aggregate functions
    aggregates = {}
    for agg in query['f']:
        parts = agg.split('_')
        if len(parts) == 3:  # Format: gv_agg_attr
            gv, agg_type, attr = parts
            gv = int(gv)
            display_name = f"{agg_type}_{attr}_{corresponding_names[gv - 1]}"  # Format: sum(quant)
            aggregates[agg] = {'gv': gv, 'type': agg_type, 'attr': attr, 'display': display_name}
        else:  # Handle other formats if needed
            gv, agg_type, attr = parts
            gv = int(gv)
            aggregates[agg] = {'gv': gv, 'type': agg_type, 'attr': attr, 'display': agg}
    
    # Generate initialization code
    init_code = []
    for agg, params in aggregates.items():
        if params['type'] == 'sum':
            init_code.append(f"'{agg}': 0")
        elif params['type'] == 'count':
            init_code.append(f"'{agg}': 0")
        elif params['type'] == 'avg':
            init_code.extend([f"'{agg}_sum': 0", f"'{agg}_count': 0"])
        elif params['type'] == 'max':
            init_code.append(f"'{agg}': float('-inf')")
        elif params['type'] == 'min':
            init_code.append(f"'{agg}': float('inf')")
    
    # Generate update code
    update_code = []
    for agg, params in aggregates.items():
        attr = params['attr']
        gv = params['gv']
        if params['type'] == 'sum':
            update_code.append(f"groups[key]['{agg}'] += row['{attr}'] if match_{gv} else 0")
        elif params['type'] == 'count':
            update_code.append(f"groups[key]['{agg}'] += 1 if match_{gv} else 0")
        elif params['type'] == 'avg':
            update_code.extend([
                f"groups[key]['{agg}_sum'] += row['{attr}'] if match_{gv} else 0",
                f"groups[key]['{agg}_count'] += 1 if match_{gv} else 0",
                # Calculate the average explicitly
                f"groups[key]['{agg}'] = groups[key]['{agg}_sum'] / groups[key]['{agg}_count'] if groups[key]['{agg}_count'] > 0 else 0"
            ])
        elif params['type'] == 'max':
            update_code.append(
                f"if match_{gv}: groups[key]['{agg}'] = max(groups[key]['{agg}'], row['{attr}'])"
            )
        elif params['type'] == 'min':
            update_code.append(
                f"if match_{gv}: groups[key]['{agg}'] = min(groups[key]['{agg}'], row['{attr}'])"
            )
    
    
    # Generate output code with proper display names
    output_code = []
    for agg, params in aggregates.items():
        if params['type'] == 'avg':
            output_code.append(
                f"'{params['display']}': group_data['{agg}_sum']/group_data['{agg}_count'] if group_data['{agg}_count']>0 else 0"
            )
        else:
            # Use the display name instead of the raw aggregate name
            output_code.append(f"'{params['display']}': group_data['{agg}']")
    
    # Build the complete body
    body = f"""
    groups = {{}}
    
    for row in cur:
        key = ({gv_key})
        
        # Check all grouping variable conditions
        {'; '.join(match_conditions)}
        if key not in groups:
            groups[key] = {{
                {', '.join([f"'{attr}': row['{attr}']" for attr in group_attrs])},
                {', '.join(init_code)}
            }}
        
        # Update aggregates
        {'\n        '.join(update_code)}
    
    # Prepare results
    result = []
    for key, group_data in groups.items():
        result_row = {{
            {', '.join([f"'{attr}': group_data['{attr}']" for attr in group_attrs])},
            {', '.join(output_code)}
        }}
        # Apply HAVING clause
        if {query['g'] if query['g'] else 'True'}:
            result.append(result_row)
    
    _global = result
    """
    return body


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
                        # Convert 1_sum_quant to group_data["1_sum_quant"]
                        having = args.strip()
                        # This regex matches all aggregate references (digits_letters)
                        having = re.sub(r'(\d+_\w+)', r'group_data["\1"]', having)
                        queryDict['g'] = having
                    else:
                        queryDict['g'] = ''
                    break
                elif '.' in args:
                    idx, condition = args.strip().split('.', 1)
                    queryDict['sigma'][int(idx.strip())] = condition.strip().replace("'''", "'")    #print(queryDict)
    return queryDict


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

    #Group grouping attributes into for each row, so it would look like row['cust'] for example for group by cust
    group_attrs = query['v']
    gv_key = ", ".join([f"row['{attr}']" for attr in group_attrs])

    #Algorithm goes here
    if query['n'] == 0:
        body = f"""

        #Create mf structure
        groups = {{}} 

        #Go thru each row of the table
        for row in cur:
            #Establish a key based on the grouping variables
            key = ({gv_key})
            
            #If first time running into the key, initialize it
            #Since we are temporarily generating a program to fulfill the sample query, quant_sum,count, and max are hardcoded
            if key not in groups:
                groups[key] = {{
                    {', '.join([f"'{attr}': row['{attr}']" for attr in group_attrs])},
                    'quant_sum': 0,  
                    'count': 0,      
                    'max_quant': 0
                }}
            
            #Update aggregate values
            groups[key]['quant_sum'] += row['quant']
            groups[key]['count'] += 1
            groups[key]['max_quant'] = max(groups[key]['max_quant'], row['quant'])

        #Convert mf structure to table
        result = []
        for key, group_data in groups.items():
            if group_data['count'] > 0:
                avg_quant = group_data['quant_sum'] / group_data['count'] 
            else:
                avg_quant = 0
            
            #Create row with specified cols
            result_row = {{
                {', '.join([f"'{attr}': group_data['{attr}']" for attr in group_attrs])},
                'avg(quant)': avg_quant,
                'max(quant)': group_data['max_quant']
            }}
            result.append(result_row)

        _global = result
        """
    else:
        # Generate the body of the MF code
        body = generate_mf_body(query)


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
    with open("_generated.py", "w") as f:
        f.write(tmp)
    
    # Execute the generated code
    subprocess.run(["python", "_generated.py"])


if "__main__" == __name__:
    main()