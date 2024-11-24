# import sqlalchemy
# import pymysql
# pymysql.install_as_MySQLdb()
# import pandas as pd

import random

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
    print("You are now using MySQL.")
    print("Please select a database you would like to explore:")
    print("   1: Books of the Decade")
    print("   2: Spotify: Most Streamed Songs")
    print("   3: Google Merchandise Dataset")

    dataset = input("> ")
    dataset = error_checking(dataset, 1, 3)

    if dataset == 1:
        # my_conn = sqlalchemy.create_engine('mysql+mysqldb://root:Dsci-351@localhost/books_of_the_decade')
        my_conn = "test"
        mysql_query(my_conn, "Books of the Decade", dataset)
    elif dataset == 2:
        my_conn = sqlalchemy.create_engine('mysql+mysqldb://root:Dsci-351@localhost/spotify_most_streamed_songs')
        mysql_query(my_conn, "Spotify: Most Streamed Songs", dataset)
    elif dataset == 3:
        my_conn = sqlalchemy.create_engine('mysql+mysqldb://root:Dsci-351@localhost/google_merchandise')
        mysql_query(my_conn, "Google Merchandise", dataset)
        
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
            sql_query_generation(choice, dataset_index)
        else:
            data = pd.read_sql(query, my_conn)
            data

        exit = yes_no("Would you like to enter another query?")

def sql_query_generation(menu_option, dataset_index):
    column_titles = []

    functions_end = ["asc", "desc"]
    functions_operators = ["where", "group by", "sort by", "having"]
    functions_number = ["limit", "skip", "offset"]

    functions = ["asc", "desc", "where", "group by", "sort by", "having", "limit", "skip", "offset"]

    if dataset_index == 1: # books of the decade
        column_titles = ["Book_Name", "Author", "Rating", "Number_of_Votes", "Score"]
        book_name = ["\"The Invisible Life of Addie LaRue\"", "\"Project Hail Mary\"", "\"The Midnight Library\"", "\"Piranesi\"", "\"Lessons in Chemistry\""]
        authors = ["\"Victoria Schwab\"", "\"T.J. Klune\"", "\"Andy Weir\"", "\"Matt Haig\"", "\"Suzanne Collins\""]

        if menu_option == "1": # generate "random" queries
            counter = 0
            while counter < 3:
                # pseudo random number generator between 0 and x (amount of attributes)
                attribute = random.randint(0, len(column_titles)-1)
                att1 = column_titles[attribute] # select Book_Name
                # pseudo random number generator between 0 and x (amount of functions in functions list)
                function = random.randint(0, len(functions_operators)-1)
                func1 = functions_operators[function] # select Book_Name where
                if func1 == "where":
                    attribute = random.randint(0, len(column_titles)-1)
                    att2 = column_titles[attribute] # select Book_Name where Author
                    if att2 == "Book_Name":
                        entry = random.randint(0, 4)
                        close = "= "
                        close += book_name[entry] # select Book_Name where Book_Name 
                    elif att2 == "Author":
                        entry = random.randint(0, 4)
                        close = "= "
                        close += authors[entry]
                    elif att2 == "Rating":
                        arg = str(random.randint(0, 4))
                        close = "> " + arg
                    elif att2 == "Number_of_Votes":
                        arg = str(random.randint(2, 2000000))
                        close = "> " + arg
                    elif att2 == "Score":
                        arg = str(random.randint(0, 62442))
                        close = "> " + arg
                    example = "select " + att1 + " from books_of_the_decade " + func1 + " " + att2 + " " + close + ";"
                elif func1 == "group by":
                    att2 = "Author" # select Book_Name where Author
                    example = "select " + att1 + " from books_of_the_decade " + func1 + " " + att2 + ";"
                elif func1 == "having":
                    attribute = random.randint(0, len(column_titles)-1)
                    att2 = column_titles[attribute] # select Book_Name where Author
                    if att2 == "Book_Name":
                        entry = random.randint(0, 4)
                        close = "= "
                        close += book_name[entry] # select Book_Name where Book_Name 
                    elif att2 == "Author":
                        entry = random.randint(0, 4)
                        close = "= "
                        close += authors[entry]
                    elif att2 == "Rating":
                        arg = str(random.randint(0, 4))
                        close = "> " + arg
                    elif att2 == "Number_of_Votes":
                        arg = str(random.randint(50, 2000000))
                        close = "> " + arg
                    elif att2 == "Score":
                        arg = str(random.randint(0, 62442))
                        close = "> " + arg
                    example = "select " + att1 + " from books_of_the_decade " + func1 + " " + att2 + " " + close + ";"
                
                print(example)
                counter += 1
        elif menu_option == "2": # user selects what functions they want
            user_functions = []
            print("Select a function you would like an example of:")
            counter = 0
            for func in functions:
                counter += 1
                cnt = str(counter)
                print(cnt + ":" + " " + func)
            selection = int(input("> "))
            user_functions.append(functions[selection-1])
            
            # adding more than one function for examples
            add_func = False
            while add_func == False:
                add_func = yes_no("Would you like to see another function?")
                
                print("Select a function you would like an example of:")
                counter = 0
                for func in functions:
                    counter += 1
                    cnt = str(counter)
                    print(cnt + ":" + " " + func)
                selection = int(input("> "))
                user_functions.append(functions[selection-1])

            # TODO: do not allow conflicting functions in the same statement (e.g. asc and desc)
            # if user_functions contains asc and user_functions contains desc
            # print("you have selected conflicting functions. please try again")
            # return user to menu where they select which help option

            # TODO: generate statement according to user selections


    # elif dataset_index == 2:
    #     column_titles = ["userId", "bookIndex", "score"]
        
    #     entry = random.randin(1, 80000) # userID
    #     entry = random.randin(1, 2327) # bookindex 
    #     entry = random.randin(1, 5) # score


    # elif dataset_index == 3:
    

   

def mongodb():
    print("You are now using MongoDB.")
    print("Please select a database you would like to explore:")
    print("   1: Yelp Dataset")
    print("   2: Iris Dataset")
    print("   3: Real Estate Predictions")

    dataset = input("> ")
    dataset = error_checking(dataset, 1, 3)

    if dataset == 1:
        print("You are now using the Yelp dataset. Enter a query, or enter \'?\' for assistance.")
        query = input("> ")

        if query == "?":
            print("Here are some sample queries.")    

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