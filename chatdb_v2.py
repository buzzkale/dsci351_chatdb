import sqlalchemy
import pymysql
pymysql.install_as_MySQLdb()
import pandas as pd
from pymongo import MongoClient
client = MongoClient()
final = client['final']
# iris = final['iris']
# property_level = final['property_level']
# us_category_id = final['US_category_id']
# ca_category_id = final['CA_category_id']
import random

# sql query to display
global my_conn
my_conn = None


class sql_database_class:
    def __init__(self, table_name, columns, num_cols, str_cols):
        self.table_name = table_name
        self.columns = columns
        self.num_cols  = num_cols   #columns with nums
        self.str_cols = str_cols    #cols with strings

    '''
    def generate_query(self):
        select_columns = ", ".join(self.columns)
        query = f"""
        SELECT {select_columns}
        FROM {self.table_name}
        ORDER BY {self.order_by_column} {self.sort_order};
        """
        return query.strip()'''

# running chatdb
def chatdb(): 
    # selecting a database
    print("What database system would you like to use?")
    print("   1: MySQL")
    print("   2: MongoDB")
    language = input("> ")
    language = error_checking(language, 1, 2)
    
    if language == 1:
        mysql()
    elif language == 2:
        mongodb()
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
    my_conn = sqlalchemy.create_engine('mysql+mysqldb://root:Dsci-351@localhost/final')
    if dataset in [1, 2]:
        mysql_query(my_conn, f"Dataset #{dataset}", dataset)
    elif dataset == 3:
        sql_query_generation("1", dataset, my_conn)

    # if dataset == 1:
    #     # my_conn = sqlalchemy.create_engine('mysql+mysqldb://root:Dsci-351@localhost/books_of_the_decade')
    #     mysql_query(my_conn, "Books of the Decade", dataset)
    # elif dataset == 2:
    #     # my_conn = sqlalchemy.create_engine('mysql+mysqldb://root:Dsci-351@localhost/spotify_most_streamed_songs')
    #     mysql_query(my_conn, "Spotify: Most Streamed Songs", dataset)
    # elif dataset == 3:
    #     # my_conn = sqlalchemy.create_engine('mysql+mysqldb://root:Dsci-351@localhost/google_merchandise')
    #     mysql_query(my_conn, "Google Merchandise", dataset)
        
def mysql_query(my_conn, dataset, dataset_index):
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
            # data = pd.read_sql(query, my_conn)
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

# testing
def get_column_titles(dataset_index):
    if dataset_index == 1:  # Books of the Decade
        return ["`Index`" if col.lower() == "index" else col for col in ["Index", "Book_Name", "Author", "Rating", "Number_of_Votes", "Score"]]
    elif dataset_index == 2:  # Spotify: Most Streamed Songs
        return ["track_name", "artist(s)_name", "artist_count", "released_year", "released_month", "released_day", "in_spotify_playlists", "in_spotify_charts", "streams", "in_apple_playlists", "in_apple_charts", "in_deezer_playlists", "in_deezer_charts", "in_shazam_charts", "bpm", "key", "mode", "danceability_%", "valence_%", "energy_%", "acousticness_%", "instrumentalness_%", "liveness_%", "speechiness_%", "cover_url"]
    elif dataset_index == 3:  # Google Merchandise
        # Assume three tables: products, orders, customers
        return {
            'items': ["id", "name", "brand", "variant", "category", "price_in_usd"],
            'events1': ["user_id", "ga_session_id", "country", "device", "type", "item_id", "date"],
            'users': ["id", "ltv", "date"]
        }
    return []

def get_shared_columns():
    return {
        'items' : 'id',
        'events1' : 'item_id',
        'users' : 'id'
        # 'products': 'product_id',
        # 'orders': 'product_id',
        # 'customers': 'customer_id'
    }

def sql_query_generation(menu_option, dataset_index, my_conn):
    column_titles = []
   
    functions = ["asc", "desc", "where", "group by", "having", "limit", "offset"]

    book_db = sql_database_class("books_of_the_decade", 
                                columns= ["`Index`" if col.lower() == "index" else col for col in ["Index", "Book_Name", "Author", "Rating", "Number_of_Votes", "Score"]],
                                num_cols = ["Rating", "Number_of_Votes", "Score"],
                                str_cols = ["Book_Name", "Author"])
    
    spotify_db = sql_database_class("spotify_most_streamed_songs", 
                                columns= ["track_name",	"artist_name",	"artist_count",	"released_year",	"released_month",	"released_day",	"in_spotify_playlists",	"in_spotify_charts",	"streams",	"in_apple_playlists",	"in_apple_charts",	"in_deezer_playlists",	"in_deezer_charts",	"in_shazam_charts",	"bpm",	"`key`",	"mode",	"danceability_pct",	"valence_pct",	"energy_pct",	"acousticness_pct",	"instrumentalness_pct",	"liveness_pct",	"speechiness_pct",	"cover_url"],
                                num_cols = ["artist_count", "streams", "in_apple_playlists"],
                                str_cols = ["artist_name", "released_year", "bpm"])

    if dataset_index == 1: # books of the decade
        if menu_option == "1": # generate "random" queries
            query = generate_query_from_functions(book_db, random.randint(0, 6))
        elif menu_option == "2": # user selects what functions they want
            print("Select a function you would like an example of:")
            for i, func in enumerate(functions, 1):
                print(f"{i}: {func}")
            selection = int(input("> ")) - 1
            # Generate query based on user_functions
            query = generate_query_from_functions(book_db, selection)
            #print("Generated query:", query)
    elif  dataset_index == 2: # "Spotify: Most Streamed Songs"
        if menu_option == "1": # generate "random" queries
            query = generate_query_from_functions(spotify_db, random.randint(0, 6))
        elif menu_option == "2": 
            print("Select a function you would like an example of:")
            for i, func in enumerate(functions, 1):
                print(f"{i}: {func}")
            selection = int(input("> ")) - 1
            # Generate query based on user_functions
            query = generate_query_from_functions(spotify_db, selection)
        return 
    elif  dataset_index == 3: # "Google Merchandise"
        tables = get_column_titles(dataset_index)
        shared_columns = get_shared_columns()
        # brand = ["\"Google\"", "\"Android\"", "\"Youtube\"", "\"#IamRemarkable\""]
        # category = ["\"Apparel\"", "\"New\"", "\"Drinkware\"", "\"Campus Collection\"", "\"Clearance\""]
        if menu_option == "1":  # Generate random queries
            for _ in range(10):
                table1, table2 = random.sample(list(tables.keys()), 2)
                join_column = shared_columns[table1]
                
                query = f"SELECT * FROM {table1} JOIN {table2} ON {table1}.{join_column} = {table2}.{join_column}"
                
                print(f"Generated join query: {query}")
                
                try:
                    result = pd.read_sql(query, my_conn)
                    if result.empty:
                        print("Query executed successfully, but returned no results.")
                    else:
                        print("\nQuery Results:")
                        print(result.to_string(index=False))
                except Exception as e:
                    print(f"Error executing query: {e}")
                    
        else:
            column_titles = get_column_titles(dataset_index)
        
        if menu_option == "1":  # Generate random queries
            for _ in range(10):
                attribute = random.choice(column_titles)
                function = random.choice(["where", "group by", "sort by", "having"])
                
                query = f"SELECT `{attribute}` FROM {dataset}"
                
                if function == "where":
                    condition_column = random.choice(column_titles)
                    operator = random.choice([">", "<", "="])
                    value = random.randint(1, 100)  # Example value
                    query += f" WHERE `{condition_column}` {operator} {value}"
                
                elif function == "group by":
                    group_column = random.choice(column_titles)
                    agg_function = random.choice(["COUNT", "AVG"])
                    agg_column = random.choice(column_titles)
                    query += f" GROUP BY `{group_column}` HAVING {agg_function}(`{agg_column}`) > {value}"
                
                elif function == "sort by":
                    sort_column = random.choice(column_titles)
                    direction = random.choice(["ASC", "DESC"])
                    query += f" ORDER BY `{sort_column}` {direction}"
                
                elif function == "having":
                    group_column = random.choice(column_titles)
                    agg_function = random.choice(["COUNT", "AVG"])
                    agg_column = random.choice(column_titles)
                    operator = random.choice([">", "<"])
                    value = random.randint(1, 100)  # Example value
                    query += f" GROUP BY `{group_column}` HAVING {agg_function}(`{agg_column}`) {operator} {value}"
                
                print(f"Generated query #{_+1}: {query};")
                
                try:
                    result = pd.read_sql(query, my_conn)
                    if result.empty:
                        print("Query executed successfully, but returned no results.")
                    else:
                        print("\nQuery Results:")
                        print(result.to_string(index=False))
                except Exception as e:
                    print(f"Error executing query: {e}")
        return 
  
def generate_query_from_functions(db, selection):
    functions = ["asc", "desc", "where", "group by", "having", "limit", "offset"]

    if selection == 0:
        query = f"SELECT {random.choice(db.str_cols)} FROM {db.table_name} ORDER BY {random.choice(db.num_cols)} ASC;"
    elif selection == 1:
        query = f"SELECT {random.choice(db.str_cols)} FROM {db.table_name} ORDER BY {random.choice(db.num_cols)} DESC;"
    elif selection == 2:
        query = f"SELECT {random.choice(db.str_cols)} FROM {db.table_name} WHERE {random.choice(db.num_cols)} > {random.randint(1, 5)};"
    elif selection == 3:
        col1 = random.choice(db.str_cols)
        query = f"SELECT {col1}, SUM({random.choice(db.num_cols)}) FROM {db.table_name} GROUP BY {col1};"
    elif selection == 4:
        print("having")
        col1 = random.choice(db.str_cols)
        col2 = random.choice(db.num_cols)
        query = f"SELECT {col1}, SUM({col2}) as total FROM {db.table_name} GROUP BY {col1} HAVING total > {random.randint(1, 5)};"
    elif selection == 5:
        col1 = random.choice(db.str_cols)
        query = f"SELECT {col1}, SUM({random.choice(db.num_cols)}) as total FROM {db.table_name} GROUP BY {col1} LIMIT {random.randint(1, 10)};"
    elif selection == 6:
        col1 = random.choice(db.str_cols)
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
   
def mongodb():
    print("You are now using MongoDB.")
    print("Please select a database you would like to explore:")
    print("   1: Iris Dataset")
    print("   2: Real Estate Dataset")
    print("   3: YouTube Trending Dataset")
    dataset = input("> ")
    dataset = error_checking(dataset, 1, 3)
    # if dataset == 1:
    #     print("You are now using the Yelp dataset. Enter a query, or enter \'?\' for assistance.")
    #     query = input("> ")
    #     if query == "?":
    #         print("Here are some sample queries.") 
    if dataset == 1:
        collection = final['iris']
    elif dataset == 2:
        collection = final['property_level']
    elif dataset == 3:
        collection = final['US_category_id']
    
    print(f"You are now using the {collection.name} collection. Enter a query, or enter '?' for assistance.")
    
    while True:
        query = input("> ")
        if query.lower() == 'exit':
            break
        elif query == "?":
            show_mongodb_help()
        else:
            execute_mongodb_query(collection, query)
def show_mongodb_help():
    print("MongoDB Query Help:")
    print("1. Find all documents: db.collection.find()")
    print("2. Find with criteria: db.collection.find({field: value})")
    print("3. Find with projection: db.collection.find({}, {field: 1})")
    print("4. Count documents: db.collection.count_documents({})")
    print("5. Aggregate: db.collection.aggregate([{$group: {_id: '$field', count: {$sum: 1}}}])")
    print("Enter 'exit' to quit.")
def execute_mongodb_query(collection, query):
    try:
        # Parse the query
        if query.startswith("db." + collection.name + ".find("):
            # Extract parameters from find()
            params = query[19:-1]
            if params:
                criteria, projection = eval(f"({params})")
            else:
                criteria, projection = {}, {}
            
            results = collection.find(criteria, projection)
            for doc in results:
                print(doc)
        
        elif query.startswith("db." + collection.name + ".count_documents("):
            criteria = eval(query[30:-1])
            count = collection.count_documents(criteria)
            print(f"Count: {count}")
        
        elif query.startswith("db." + collection.name + ".aggregate("):
            pipeline = eval(query[25:-1])
            results = collection.aggregate(pipeline)
            for doc in results:
                print(doc)
        
        else:
            print("Unsupported query. Please try again or type '?' for help.")
    
    except Exception as e:
        print(f"Error executing query: {e}")
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