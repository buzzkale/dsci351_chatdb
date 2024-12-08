# SQL imports
import sqlalchemy
import pymysql
pymysql.install_as_MySQLdb()
import pandas as pd

# XML imports 
from lxml import etree
import xml.etree.ElementTree as ET
UK_JOBS_XML = "monster_uk-job_sample.xml"
BIBS_XML = "bibs.xml"
MOVIES_XML = "movies.xml"

import random

# For establishing connection to terminal (display SQL)
global my_conn
my_conn = None

# For defining a database and its characteristics
class sql_database_class:
    def __init__(self, table_name, columns, num_cols, str_cols):
        self.table_name = table_name
        self.columns = columns
        self.num_cols  = num_cols   #columns with nums
        self.str_cols = str_cols    #cols with strings

# Running chatdb
def chatdb(): 
    # Selecting a database
    print("What database system would you like to use?")
    print("   1: MySQL")
    print("   2: XML")
    language = input("> ")
    language = error_checking(language, 1, 2)
    
    if language == 1:
        mysql()
    elif language == 2:
        xml()
    return yes_no("Continue using ChatDB?")
    
def mysql():
    global my_conn
    print("You are now using MySQL.")
    print("Please select a database you would like to explore:")
    print("   1: Books of the Decade")
    print("   2: Spotify: Most Streamed Songs")
    print("   3: Google Merchandise Dataset")
    dataset = input("> ")
    dataset = error_checking(dataset, 1, 3)
    if dataset == 1:
        my_conn = sqlalchemy.create_engine('mysql+mysqldb://root:Dsci-351@localhost/books')
        mysql_query(my_conn, f"Dataset #{dataset}: Books of the Decade", dataset)
    elif dataset == 2:
        my_conn = sqlalchemy.create_engine('mysql+mysqldb://root:Dsci-351@localhost/spotify')
        mysql_query(my_conn, f"Dataset #{dataset}: Spotify Most Streamed Songs", dataset)
    elif dataset == 3:
        my_conn = sqlalchemy.create_engine('mysql+mysqldb://root:Dsci-351@localhost/google')
        mysql_query(my_conn, f"Dataset #{dataset}: Google Merchandise Dataset", dataset)
    # I don't think the below is required because if we run a 3 directory setup, it will still work but I will keep
    # the logic for now
    # my_conn = sqlalchemy.create_engine('mysql+mysqldb://root:Dsci-351@localhost/final')
    # if dataset in [1, 2]:
    #     mysql_query(my_conn, f"Dataset #{dataset}", dataset)
        
def mysql_query(my_conn, dataset, dataset_index): # menu UI
    print("You are now using the", dataset, "dataset.")
    exit = False
    while exit == False:
        print("Enter a query, or enter \'?\' for assistance.")
        query = input("> ")
        if query == "?":
            print("Help menu:")
            print("   1: Generate example queries")
            print("   2: Generate example queries by function")
            choice = input("> ")
            sql_query_generation(choice, dataset_index, my_conn)
        else:
            try:
                data = pd.read_sql(query, my_conn)
                if data.empty:
                    print("Query executed successfully, but returned no results.")
                else:
                    print("\nQuery Results:")
                    print(data.to_string(index=False))
                    print(f"\nNumber of rows: {len(data)}")
            except Exception as e:
                print(f"Error executing query: {e}")
        exit = yes_no("Would you like to enter another query?")

def get_column_titles(dataset_index): # framework for our SQL db's
    if dataset_index == 1:  # Books of the Decade
        return ["`Index`" if col.lower() == "index" else col for col in ["Index", "Book_Name", "Author", "Rating", "Number_of_Votes", "Score"]]
    elif dataset_index == 2:  # Spotify: Most Streamed Songs
        return ["track_name", "artists_name", "artist_count", "released_year", "released_month", "released_day", "in_spotify_playlists", "in_spotify_charts", "streams", "in_apple_playlists", "in_apple_charts", "in_deezer_playlists", "in_deezer_charts", "in_shazam_charts", "bpm", "key", "mode", "danceability_%", "valence_%", "energy_%", "acousticness_%", "instrumentalness_%", "liveness_%", "speechiness_%", "cover_url"]
    elif dataset_index == 3:  # Google Merchandise (3 csv dataset)
        return {
            'items': ["id", "name", "brand", "variant", "category", "price_in_usd"],
            'events1': ["user_id", "ga_session_id", "country", "device", "type", "item_id", "date"],
            'users': ["id", "ltv", "date"]
        }
    return []

def sql_query_generation(menu_option, dataset_index, my_conn): # Overal generation logic
    functions = ["asc", "desc", "where", "group by", "having", "order by", "limit", "offset"]
    # set db: books of the decade
    book_db = sql_database_class("books_of_the_decade", 
                                columns = ["`Index`" if col.lower() == "index" else col for col in ["Index", "Book_Name", "Author", "Rating", "Number_of_Votes", "Score"]],
                                num_cols = ["Rating", "Number_of_Votes", "Score"],
                                str_cols = ["Book_Name", "Author", "Index"]) 
        # added index to the above str_cols section; see how it affects code!!!
    # set db: spotify 
    spotify_db = sql_database_class("spotify_most_streamed_songs", 
                                columns= ["track_name",	"artist_name", "artist_count", "released_year", "released_month", "released_day", "in_spotify_playlists", "in_spotify_charts", "streams", "in_apple_playlists", "in_apple_charts",	"in_deezer_playlists", "in_deezer_charts", "in_shazam_charts", "bpm", "`key`", "mode", "danceability_pct", "valence_pct", "energy_pct",	"acousticness_pct",	"instrumentalness_pct",	"liveness_pct",	"speechiness_pct",	"cover_url"],
                                num_cols = ["artist_count", "released_year", "in_apple_playlists"],
                                str_cols = ["track_name", "artist_name", "key"])
    # use db: books of the decade


    if dataset_index == 1: 
        # DB 1 specific
        column_titles = book_db.columns
        functions_operators = ["where", "group by", "order by","having"]
        attribute = random.choice(column_titles)
        function = random.choice(functions_operators)
        non_agg_columns = book_db.str_cols
        numeric_columns = book_db.num_cols
        book_names = ["The Invisible Life of Addie LaRue", "Project Hail Mary", "The Midnight Library", "Piranesi", "Lessons in Chemistry"]
        authors = ["Victoria Schwab", "T.J. Klune", "Andy Weir", "Matt Haig", "Suzanne Collins"]
        if menu_option == "1": # generate "random" queries
            query = f"SELECT {attribute} FROM {book_db.table_name}"
            for _ in range(10):
                if function == "where":
                    condition_column = random.choice(column_titles)
                    if condition_column in [non_agg_columns[0], non_agg_columns[1]]:
                        value = random.choice(book_names if condition_column == "Book_Name" else authors)
                        query += f" WHERE `{condition_column}` = '{value}'"
                    else:
                        operator = random.choice([">", "<", "="])
                        value = random.randint(1, 5) if condition_column == numeric_columns[0] else random.randint(1000, 2000000)
                        query += f" WHERE `{condition_column}` {operator} {value}"
                
                elif function == "group by":
                    group_column = random.choice(non_agg_columns + numeric_columns)
                    agg_function = random.choice(["COUNT", "AVG", "MAX", "MIN"])
                    agg_column = random.choice(numeric_columns)
                    query = f"SELECT `{group_column}`, {agg_function}({agg_column}) FROM {book_db.table_name} GROUP BY `{group_column}`"
                

                elif function == "having":
                    group_column = random.choice(non_agg_columns)
                    agg_function = random.choice(["COUNT", "AVG", "MAX", "MIN"])
                    agg_column = random.choice(numeric_columns)
                    operator = random.choice([">", "<"])
                    value = random.randint(1, 1000000)
                    query = f"SELECT `{group_column}`, {agg_function}({agg_column}) FROM {book_db.table_name} GROUP BY `{group_column}` HAVING {agg_function}(`{agg_column}`) {operator} {value}"

                elif function == "order by":
                    sort_column = random.choice(column_titles)
                    direction = random.choice(["ASC", "DESC"])
                    query += f" ORDER BY `{sort_column}` {direction}"

                elif function == "limit":
                    limit_value = random.choice([1, 5, 10, 50])
                    query += f" LIMIT {limit_value}"

                print(f"Ex #{_+1}: {query};")
                try:
                    # execute the query
                    result = pd.read_sql(query, my_conn)
                    # print the results
                    if result.empty:
                        print("Query executed successfully, but returned no results.")
                    else:
                        print("\nQuery Results:")
                        print(result.to_string(index=False))
                except Exception as e:
                    print(f"Error executing query: {e}")
                print("\n" + "-"*50 + "\n")  # separator between queries
        elif menu_option == "2": # user selects what functions they want (# books of the decade)
            print("Select a function you would like an example of:")
            for i, func in enumerate(functions, 1):
                print(f"{i}: {func}")
            selection = int(input("> ")) - 1
            # Generate query based on user_functions
            result = generate_query_from_functions(book_db, my_conn, selection)
            # random.randint(0, 8)
    # "Spotify: Most Streamed Songs"
    elif dataset_index == 2: 
        # DB 2 specific
        column_titles = spotify_db.columns
        functions_operators = ["where", "group by", "order by", "having"]
        attribute = random.choice(column_titles)
        function = random.choice(functions_operators)
        non_agg_columns = spotify_db.str_cols
        numeric_columns = spotify_db.num_cols
        track_names = ["As It Was","See You Again","Moonlight","STAY (with Justin Bieber)","Pink + White", "Cupid", "Summertime Sadness"]
        artist_names = ["SZA","The Weeknd","Taylor Swift","Jung Kook","Billie Eilish", "Joji", "Bad Bunny", "Bruno Mars", "NewJeans"]
        if menu_option == "1": # generate "random" queries
           for _ in range(10): # creates 10 queries
                query = f"SELECT {attribute} FROM {spotify_db.table_name}"
                if function == "where":
                    condition_column = random.choice(column_titles)
                    if condition_column in [non_agg_columns[0], non_agg_columns[1]]:
                        value = random.choice(track_names if condition_column == "track_name" else artist_names)
                        query += f" WHERE `{condition_column}` = '{value}'"
                    else:
                        operator = random.choice([">=", "<=", "="])
                        value = random.randint(1, 7) if condition_column == numeric_columns[0] else random.randint(1000, 50000)
                        query += f" WHERE `{condition_column}` {operator} {value}"
                
                elif function == "group by":
                    group_column = random.choice(non_agg_columns + numeric_columns)
                    agg_function = random.choice(["COUNT", "AVG", "MAX", "MIN"])
                    agg_column = random.choice(numeric_columns)
                    query = f"SELECT `{group_column}`, {agg_function}({agg_column}) FROM {spotify_db.table_name} GROUP BY `{group_column}`"

                elif function == "having":
                    group_column = random.choice(non_agg_columns)
                    agg_function = random.choice(["COUNT", "AVG", "MAX", "MIN"])
                    agg_column = random.choice(numeric_columns)
                    operator = random.choice([">", "<"])
                    value = random.randint(1, 50000)
                    query = f"SELECT `{group_column}`, {agg_function}({agg_column}) FROM {spotify_db.table_name} GROUP BY `{group_column}` HAVING {agg_function}(`{agg_column}`) {operator} {value}"

                elif function == "order by":
                    sort_column = random.choice(column_titles)
                    direction = random.choice(["ASC", "DESC"])
                    query += f" ORDER BY `{sort_column}` {direction}"

                elif function == "limit":
                    limit_value = random.choice([1, 5, 10, 50])
                    query += f" LIMIT {limit_value}"
            
                print(f"Ex #{_+1}: {query};")
                try:
                    # execute the query
                    result = pd.read_sql(query, my_conn)
                    # print the results
                    if result.empty:
                        print("Query executed successfully, but returned no results.")
                    else:
                        print("\nQuery Results:")
                        print(result.to_string(index=False))
                except Exception as e:
                    print(f"Error executing query: {e}")
                
                print("\n" + "-"*50 + "\n")  # separator between queries
        elif menu_option == "2": # user selects what functions they want (# spotify most streamed songs)
            print("Select a function you would like an example of:")
            for i, func in enumerate(functions, 1):
                print(f"{i}: {func}")
            selection = int(input("> ")) - 1
            # Generate query based on user_functions
            result = generate_query_from_functions(spotify_db, my_conn, selection)
            # random.randint(0, 8)
            return result
    # "Google Merchandise"
    elif  dataset_index == 3: 
        # DB 3 specific
        tables = get_column_titles(dataset_index)
        valid_joins = [
            ('events1', 'items', 'item_id', 'id'),
            ('events1', 'users', 'user_id', 'id')
        ]
        if menu_option == "1":  # generate random queries
            for _ in range(10): # creates 10 queries
                # Randomly select one of the two valid join pairs
                table1, table2, join_column1, join_column2 = random.choice(valid_joins)

                # Select random columns from each table
                column1 = random.choice(tables[table1])
                column2 = random.choice(tables[table2])

                # "join" query structure 
                query = f"""
                SELECT {table1}.{column1}, {table2}.{column2}
                FROM {table1}
                JOIN {table2} ON {table1}.{join_column1} = {table2}.{join_column2}
                """

                # add where clause
                if random.choice([True, False]):
                    where_table = random.choice([table1, table2])
                    where_column = random.choice(tables[where_table])
                    where_value = random.randint(1, 1000)  # Adjust range as needed
                    query += f"\nWHERE {where_table}.{where_column} > {where_value}"

                # add order by
                if random.choice([True, False]):
                    order_table = random.choice([table1, table2])
                    order_column = random.choice(tables[order_table])
                    order_direction = random.choice(["ASC", "DESC"])
                    query += f"\nORDER BY {order_table}.{order_column} {order_direction}"

                # add limit
                if random.choice([True, False]):
                    limit_value = random.randint(1, 50)
                    query += f"\nLIMIT {limit_value}"

                print(f"Generated query #{_+1}:\n{query}")

                try:
                    result = pd.read_sql(query, my_conn)
                    if result.empty:
                        print("Query executed successfully, but returned no results.")
                    else:
                        print("\nQuery Results:")
                        print(result.to_string(index=False))
                        print(f"\nNumber of rows: {len(result)}")
                except Exception as e:
                    print(f"Error executing query: {e}")

                print("\n" + "-"*50 + "\n")  # Separator between queries
            return result

# *****Specific generation by databases***** 

def generate_query_from_functions(db, my_conn, selection): # no join query selection requests
    # functions = ["asc", "desc", "where", "group by", "having", "sort by (asc)", "sort by (desc)", "limit", "offset"]
    
    # functions_operators = ["where", "group by", "sort by", "having"]
    # functions = ["asc", "desc", "where", "group by", "sort by", "having", "limit", "skip", "offset"]
    if selection == 0:
        query = f"SELECT {random.choice(db.str_cols[:2])} FROM {db.table_name} ORDER BY {random.choice(db.num_cols)} ASC;"
    elif selection == 1:
        query = f"SELECT {random.choice(db.str_cols[:2])} FROM {db.table_name} ORDER BY {random.choice(db.num_cols)} DESC;"
    elif selection == 2:
        query = f"SELECT {random.choice(db.str_cols[:2])} FROM {db.table_name} WHERE {random.choice(db.num_cols)} >= {random.randint(1, 5)};"
    elif selection == 3:
        col1 = random.choice(db.str_cols[:2])
        query = f"SELECT {col1}, SUM({random.choice(db.num_cols)}) FROM {db.table_name} GROUP BY {col1};"
    elif selection == 4:
        print("having")
        col1 = random.choice(db.str_cols[:2])
        col2 = random.choice(db.num_cols)
        operator = random.choice([">=", "<=", "="])
        query = f"SELECT {col1}, SUM({col2}) as total FROM {db.table_name} GROUP BY {col1} HAVING total {operator} {random.randint(1, 5)};"
    elif selection == 5:
        print('order by')
        col1 = random.choice(db.str_cols[:2])
        col2 = random.choice(db.num_cols)
        operator = random.choice([">=", "<=", "="])
        direction = random.choice(["ASC", "DESC"])
        query = f"SELECT {col1}, SUM({col2}) as total FROM {db.table_name} GROUP BY {col1} HAVING total {operator} {random.randint(1, 5)} ORDER BY {col2} {direction};"
    elif selection == 6:
        col1 = random.choice(db.str_cols[:2])
        query = f"SELECT {col1}, SUM({random.choice(db.num_cols)}) as total FROM {db.table_name} GROUP BY {col1} LIMIT {random.randint(1, 10)};"
    elif selection == 7:
        col1 = random.choice(db.str_cols[:2])
        query = f"SELECT {col1}, SUM({random.choice(db.num_cols)}) as total FROM {db.table_name} GROUP BY {col1} LIMIT {random.randint(6, 10)} OFFSET {random.randint(1, 3)};"
    
    print("Example Query:\n", query)
    execute_query = not yes_no("Do you want to execute the query?")

    if execute_query:
        try:
            data = pd.read_sql(query, my_conn)
            if data.empty:
                print("Query executed successfully, but returned no results.")
            else:
                print("\nQuery Results:")
                print(data.to_string(index=False))
                print(f"\nNumber of rows: {len(data)}")
        except Exception as e:
            print(f"Error executing query: {e}")

def get_shared_columns(): # for join purposes
    return {
        ('items', 'events1'): ('id', 'item_id'),
        ('users', 'events1'): ('id', 'user_id')
    }

 
# XML Queries
def load_xml_file(file_path):
    # try:
    #     tree = ET.parse(file_path)
    #     root = tree.getroot()
    #     return root
    # except ET.ParseError as e:
    #     print(f"Error parsing XML file: {e}")
    #     return None

    try:
        tree = etree.parse(file_path)
        return tree
    except etree.XMLSyntaxError as e:
        print(f"Error parsing XML file: {e}")
        return None

def xml():
    print("You are now using XML.")
    print("Please select a database you would like to explore:")
    print("   1: Monster UK Jobs")
    print("   2: Bibliography")
    print("   3: Movies")

    dataset = input("> ")
    dataset = error_checking(dataset, 1, 3)

    if dataset == 1:
        tree = xml_1
        print("You are now using the Monster UK Jobs dataset.")
        xml_root = load_xml_file('monster_uk-job_sample.xml')
        xml_query(xml_root)
    elif dataset == 2:
        tree = xml_2
        print("You are now using the bibs dataset.")
        xml_root = load_xml_file('bibs.xml')
        xml_query(xml_root)
    elif dataset == 3:
        tree = xml_3
        print("You are now using the movies dataset.")
        xml_root = load_xml_file('movies.xml')
        xml_query(xml_root)

def xml_query(tree):
    exit = False
    while exit == False:
        print("Enter a query, or enter \'?\' for assistance.")
        query = input("> ")
        if query == "?":
            print("Help menu:")
            print("   1: Generate example queries")
            print("   2: Generate example queries by function")
            choice = input("> ")
            if choice == "1":
                generate_random_xml_queries(tree)
            else:
                generate_xml_queries_by_function(tree)
        else:
            execute_xml_query(tree, query)
            
        exit = yes_no("Would you like to enter another query?")

def xml_1():
    with open(UK_JOBS_XML, 'r') as file:
        return etree.parse(file)
    
def xml_2():
    with open(BIBS_XML, 'r') as file:
        return etree.parse(file)
    
def xml_3():
    with open(MOVIES_XML, 'r') as file:
        return etree.parse(file)

def generate_random_xml_queries(xml_root):
    # Gather all elements and attributes for query generation
    elements = [elem.tag for elem in xml_root.iter()]
    attributes = {elem.tag: list(elem.attrib.keys()) for elem in xml_root.iter() if elem.attrib}

    query_types = ['find_element', 'find_attribute', 'find_with_predicate']

    for _ in range(3):
        query_type = random.choice(query_types)
        
        if query_type == 'find_element':
            element = random.choice(elements)
            print(f"Find all '{element}' elements: .//{element}")
        
        elif query_type == 'find_attribute' and attributes:
            element = random.choice(list(attributes.keys()))
            attribute = random.choice(attributes[element])
            print(f"Find '{element}' elements with attribute '{attribute}': .//{element}[@{attribute}]")
        
        elif query_type == 'find_with_predicate':
            element = random.choice(elements)
            if attributes.get(element):
                attribute = random.choice(attributes[element])
                value = f"'value{random.randint(1, 100)}'"  # Generate a random value
                print(f"Find '{element}' elements with predicate: .//{element}[@{attribute}={value}]")
            else:
                # If no attributes, use a position predicate
                position = random.randint(1, 5)
                print(f"Find '{element}' elements with position predicate: .//{element}[{position}]")

def generate_xml_queries_by_function(xml_root):
    xpath_forms = [
        "Find elements by tag name",
        "Find elements with specific attribute",
        "Find elements with specific text",
        "Find elements by position",
        "Find elements with attribute and text"
    ]

    # Gather all elements and attributes for query generation
    elements = [elem.tag for elem in xml_root.iter()]
    attributes = {elem.tag: list(elem.attrib.keys()) for elem in xml_root.iter() if elem.attrib}

    print("Select an xpath feature to explore:")
    count = 0
    for f in xpath_forms:
        count += 1
        count_string = str(count)
        print(count_string + ": " + f)

    form = input("> ")

    for _ in range(3):
        
        if form == "find elements by tag name":
            element = random.choice(elements)
            print(f"XPath Query: .//{element}")

        elif form == "find elements with specific attribute" and attributes:
            element = random.choice(list(attributes.keys()))
            attribute = random.choice(attributes[element])
            value = f"value{random.randint(1, 100)}"  # Simulate a possible attribute value
            print(f"XPath Query: .//{element}[@{attribute}='{value}']")

        elif form == "find elements with specific text":
            element = random.choice(elements)
            text_value = f"text{random.randint(1, 100)}"  # Simulate a possible text content
            print(f"XPath Query: .//{element}[text()='{text_value}']")

        elif form == "find elements by position":
            element = random.choice(elements)
            position = random.randint(1, 5)  # Random position
            print(f"XPath Query: .//{element}[position()={position}]")

        elif form == "find elements with attribute and text":
            if attributes:
                element = random.choice(list(attributes.keys()))
                attribute = random.choice(attributes[element])
                attr_value = f"value{random.randint(1, 100)}"
                text_value = f"text{random.randint(1, 100)}"
                print(f"XPath Query: .//{element}[@{attribute}='{attr_value}'][text()='{text_value}']")

def execute_xml_query(xml_root, xpath):
    try:
        # Execute the XPath query
        result = xml_root.xpath(xpath)
        
        # Process and print results based on their type
        for elem in result:
            if isinstance(elem, etree._Element):
                # Print element as a string if it's an XML element
                print(etree.tostring(elem, pretty_print=True, encoding='unicode'))
            else:
                # Print text or attribute values directly
                print(elem)

    except Exception as e:
        print(f"Error executing XPath query: {e}")


def error_checking(variable, lower_bound, upper_bound):
    entry = int(variable)
    while entry < lower_bound or entry > upper_bound:
        print("Input is out of bounds. Please enter a number between", lower_bound, "and", upper_bound)
        entry = int(input("> "))
    
    return entry
def yes_no(prompt):
    print(prompt, "(yes/no)")
    
    response = input("> ")
    if response.lower() == "no":
        response = True
    elif response.lower() == "yes":
        response = False
    
    return response
def main():
    print("Welcome to ChatDB!")
    exit = False
    while exit == False:
        exit = chatdb()
    
    print("Bye!")
if __name__ == "__main__":
    main()