import mysql.connector
import pandas as pd
import json
from pymongo import MongoClient
from pymongo import ASCENDING, DESCENDING
# from sqlalchemy import create_engine
from datetime import datetime
import time
from tqdm import tqdm


#-------------------------------------------------------------------- GOAL 2 CREATE DATABASE MYSQL-------------------------------------------------------------------- 
# Creates the database ProjectDBA
# mydb = mysql.connector.connect(
#    host="localhost",
#    user="root",
#    password='1234'
# )

# mycursor = mydb.cursor()
# mycursor.execute('DROP DATABASE IF EXISTS ProjectDBA')
# mycursor.execute("CREATE DATABASE ProjectDBA")

# mydb.close()

# # get the data
# df_ratings = pd.read_csv('ratings.csv')
# df_movies = pd.read_csv('movies.csv')
# df_tags = pd.read_csv('tags.csv')
# df_users = pd.read_csv('users_data.csv')

# df_ratings = df_ratings.head(1000000)
# df_tags = df_tags.head(1000000)

# # Tratar valores NaN
# df_ratings = df_ratings.fillna(0)  # Substituir os NaN por 0 
# df_tags = df_tags.fillna('default_value')  # Substituir os NaN por 'default_value'

# df_list_ratings = df_ratings.values.tolist()
# df_list_movies = df_movies.values.tolist()
# df_list_tags = df_tags.values.tolist()
# df_list_users = df_users.values.tolist()

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="1234",
    database="ProjectDBA",
)

# # DROP tables if exist
mycursor = mydb.cursor()
# table_name = "Rating"
# table_movie = "Movies"
# table_tags = "Tags"
# table_users = "Users"
# drop_table_Rating = f"DROP TABLE IF EXISTS {table_name};"
# drop_table_Movie = f"DROP TABLE IF EXISTS {table_movie};"
# drop_table_Tag = f"DROP TABLE IF EXISTS {table_tags};"
# drop_table_Users = f"DROP TABLE IF EXISTS {table_users};"
# mycursor.execute(drop_table_Movie)
# mycursor.execute(drop_table_Rating)
# mycursor.execute(drop_table_Tag)
# mycursor.execute(drop_table_Users)

# # create Movies schema
# create_table_Movies = """
# CREATE TABLE Movies (
#    movieId INT AUTO_INCREMENT PRIMARY KEY,
#    title VARCHAR(255),
#    genres VARCHAR(255)
# );
# """

# # Execute the query to create the table
# mycursor.execute(create_table_Movies)

# # Define the SQL queries with placeholders for the data
# insert_Movies = f"INSERT INTO {table_movie} (movieId, title, genres) VALUES (%s, %s, %s)"

# # Execute the queries for each data record in the lists
# mycursor.executemany(insert_Movies, df_list_movies)

# # create Users schema
# create_table_Users = """
# CREATE TABLE Users (
#    userId INT AUTO_INCREMENT PRIMARY KEY,
#    name VARCHAR(255)
# );
# """

# # Execute the query to create the table
# mycursor.execute(create_table_Users)

# # Define the SQL queries with placeholders for the data
# insert_Users = f"INSERT INTO {table_users} (userId, name) VALUES (%s, %s)"

# # Execute the queries for each data record in the lists
# mycursor.executemany(insert_Users, df_list_users)

# # create Rating schema
# create_table_Rating = """
# CREATE TABLE Rating (
#    userId INT,
#    movieId INT,
#    rating FLOAT,
#    timestamp INT,
#    FOREIGN KEY (userId) REFERENCES Users(userId),
#    FOREIGN KEY (movieId) REFERENCES Movies(movieId)    
# );
# """

# # Execute the query to create the table
# mycursor.execute(create_table_Rating)

# # Commit the changes
# mydb.commit()

# # Define the SQL queries with placeholders for the data
# insert_Rating = f"INSERT INTO {table_name} (userId, movieId, rating, timestamp) VALUES (%s, %s, %s, %s)"

# # Execute the queries for each data record in the lists
# mycursor.executemany(insert_Rating, df_list_ratings)

# # create Tags schema
# create_table_Tag = """
# CREATE TABLE Tags (
#    userId INT ,
#    movieId INT,
#    tag VARCHAR(255),
#    timestamp INT,
#    FOREIGN KEY (userId) REFERENCES Users(userId),
#    FOREIGN KEY (movieId) REFERENCES Movies(movieId)
# );
# """

# # Execute the query to create the table
# mycursor.execute(create_table_Tag)

# # Define the SQL queries with placeholders for the data
# insert_Tags = f"INSERT INTO {table_tags} (userId, movieId, tag, timestamp) VALUES (%s,%s, %s, %s)"

# # Execute the queries for each data record in the lists
# mycursor.executemany(insert_Tags, df_list_tags)

# # Commit the changes and close the connection
# mydb.commit()

# #--------------------------------------------------------------------GOAL 2 CREATE DATABASE MONGODB -------------------------------------------------------------------- 

# # MongoDB Connection
mongo_client = MongoClient('localhost', 27017)
mongo_db = mongo_client.ProjectDBA
mongo_collection_movies = mongo_db['Movies']
mongo_collection_users = mongo_db['Users']
mongo_collection_ratings = mongo_db['Rating']
mongo_collection_tags = mongo_db['Tags']

# # # Read data from CSV files
# df_movies = pd.read_csv('movies.csv')
# df_users = pd.read_csv('users_data.csv')
# df_ratings = pd.read_csv('ratings.csv')
# df_tags = pd.read_csv('tags.csv')

# df_movies = df_movies.head(1000000)
# df_users = df_users.head(1000000)
# df_ratings = df_ratings.head(1000000)
# df_tags = df_tags.head(1000000)

# # Handle NaN values
# df_movies = df_movies.fillna('default_value')
# df_users = df_users.fillna('default_value')
# df_ratings = df_ratings.fillna(0)
# df_tags = df_tags.fillna('default_value')

# # Insert data into Movies collection                                                            
# mongo_collection_movies.insert_many(df_movies.to_dict(orient='records'))

# # Insert data into Users collection
# mongo_collection_users.insert_many(df_users.to_dict(orient='records'))

# # Insert data into Rating collection
# mongo_collection_ratings.insert_many(df_ratings.to_dict(orient='records'))

# # Insert data into Tags collection
# mongo_collection_tags.insert_many(df_tags.to_dict(orient='records'))


#--------------------------------------------------------------------  GOAL 3 MYSQL QUERIES --------------------------------------------------------------------  
# # QUERY 1

# # query para ir buscar todos os filmes que comecem pela letra 'A' e tem o genero de 'Sci-Fi'
# print("Query para ir buscar todos os filmes que comecem pela letra 'A' e tem o genero de 'Sci-Fi' : ")
# select_query_with_A_Sci_Fi = """
# SELECT m.movieId, m.title,m.genres
# FROM Movies m
# WHERE m.genres LIKE '%Sci-Fi%' and m.title LIKE 'a%'
# """

# # Executar a query
# mycursor.execute(select_query_with_A_Sci_Fi)


# resultavg = mycursor.fetchall()

# # Criar um DataFrame a partir dos resultados
# columns = ['movieId', 'title','genres']
# result_avg = pd.DataFrame(resultavg, columns=columns)
# print(result_avg)
# print("\n")

# #--------------------------------------------------//------------------------------------------
# QUERY 2

# Genero de filme com mais filmes

# print("Query para ir buscar o género mais popular : ")
# select_query_count_by_genre = """
# SELECT genres, COUNT(*) AS movie_count
# FROM movies
# GROUP BY genres
# ORDER BY movie_count DESC
# LIMIT 1;
# """


# # Executar a query
# mycursor.execute(select_query_count_by_genre)


# resultavg = mycursor.fetchall()

# # Criar um DataFrame a partir dos resultados
# columns = ['genres', "movie_count"]
# result_avg = pd.DataFrame(resultavg, columns=columns)
# print(result_avg)
# print("\n")


# ------------------------------------------------------//------------------------------------------------
# QUERY 3

# Filmes que um utilizador avaliou
# print("Query para saber que filmes um user avaliou: ")
# select_query_filmes_avaliados_by_user = """
# SELECT m.movieId, m.title, m.genres, r.rating, r.timestamp
# FROM Rating r
# JOIN Movies m ON r.movieId = m.movieId
# INNER JOIN Users u ON u.userId = r.userId
# WHERE r.userId = 1;
# """

# mycursor.execute(select_query_filmes_avaliados_by_user)

# result = mycursor.fetchall()

# # Criar um DataFrame a partir dos resultados
# columns = ['movieId', 'title', 'genres', 'rating', 'timestamp']
# result_df = pd.DataFrame(result, columns=columns)
# print(result_df)
# print("\n")


# #------------------------------------------------------//------------------------------------------------
# QUERY 4

# query para saber quais foram as tag que um utilizador utilizou para os filmes
# print("Query para saber quais foram as tag que um utilizador utilizou para os filmes: ")
# select_query_user_vote = """
# SELECT m.movieId, m.title, r.rating, u.name, t.tag
# FROM Users u
# INNER JOIN Rating r ON u.userId = r.userId
# INNER JOIN Movies m ON r.movieId = m.movieId
# INNER JOIN Tags t ON u.userId = t.userId AND m.movieId = t.movieId
# WHERE u.userId = 14;
# """

# # Executar a query
# mycursor.execute(select_query_user_vote)


# resultUser = mycursor.fetchall()

# # Criar um DataFrame a partir dos resultados
# columns = ['movieId', 'title', 'rating', 'name', 'tag']
# result_userVote = pd.DataFrame(resultUser, columns=columns)
# print(result_userVote)
# print("\n")







# Funcionais mas nao usadas
# #------------------------------------------------------//------------------------------------------------
# NAO USADA
#query para saber a avg de rating de todos os filmes onde contenha o genero  comedia
# Caso queiram meter por ordem decrescente, tendo a média mais alta no topo da lista podemos adicionar 
# no final da query: "ORDER BY avg_rating DESC"
# print("Query para saber a avg de rating de todos os filmes onde contenha o genero  comedia: ")
# select_query_bygenre_knowingtheAvg = """
# SELECT m.movieId, m.title, m.genres, AVG(r.rating) as avg_rating
# FROM Movies m
# INNER JOIN Rating r ON m.movieId = r.movieId
# WHERE genres LIKE '%Comedy%'
# GROUP BY m.movieId, m.title
# """


# # Executar a query
# mycursor.execute(select_query_bygenre_knowingtheAvg)


# resultGenreAVG = mycursor.fetchall()

# # Criar um DataFrame a partir dos resultados
# columns = ['movieId', 'title','genres', 'avg_rating']
# result_GenreAvg = pd.DataFrame(resultGenreAVG, columns=columns)
# print(result_GenreAvg)
# print("\n")







# #------------------------------------------------------INSERT-------------------------------------------

#query para inserir um movie com ratings e tags

# new_movie_data = {
#     'title': 'A luz da noite',
#     'genres': 'Adventure'
# }
# movie_id = 193896
# delete_ratings_query = "DELETE FROM Rating WHERE movieId = %s"
# mycursor.execute(delete_ratings_query, (movie_id,))

# # Delete tags for the movie
# delete_tags_query = "DELETE FROM Tags WHERE movieId = %s"
# mycursor.execute(delete_tags_query, (movie_id,))

# # Delete the movie itself
# delete_movie_query = "DELETE FROM Movies WHERE movieId = %s"
# mycursor.execute(delete_movie_query, (movie_id,))

# # mydb.commit()
# new_ratings_data = [
#     {'userId': 1, 'rating': 4.5},
#     {'userId': 2, 'rating': 3.8},
#     {'userId': 3, 'rating': 5.0}
# ]

# new_tags_data = [
#     {'userId': 1, 'tag': 'exciting'},
#     {'userId': 2, 'tag': 'must watch'},
#     {'userId': 3, 'tag': 'favorite'}
# ]

# # Insert a new movie
# insert_movie_query = "INSERT INTO Movies (title, genres) VALUES (%s, %s)"
# mycursor.execute(insert_movie_query, (new_movie_data['title'], new_movie_data['genres']))
# mydb.commit()

# # Get the last inserted movieId
# mycursor.execute("SELECT LAST_INSERT_ID()")
# new_movie_id = mycursor.fetchone()[0]
# print("New movie id:", new_movie_id)

# # Reinitialize cursor
# mycursor = mydb.cursor()

# # Insert ratings for the new movie
# insert_ratings_query = "INSERT INTO Rating (userId, movieId, rating, timestamp) VALUES (%s, %s, %s, UNIX_TIMESTAMP())"
# for rating_data in new_ratings_data:
#     mycursor.execute(insert_ratings_query, (rating_data['userId'], new_movie_id, rating_data['rating']))
#     mydb.commit()

# # Reinitialize cursor
# mycursor = mydb.cursor()

# # Insert tags for the new movie
# insert_tags_query = "INSERT INTO Tags (userId, movieId, tag, timestamp) VALUES (%s, %s, %s, UNIX_TIMESTAMP())"
# for tag_data in new_tags_data:
#     mycursor.execute(insert_tags_query, (tag_data['userId'], new_movie_id, tag_data['tag']))
#     mydb.commit()

# # Reinitialize cursor
# mycursor = mydb.cursor()

# # #----------------------------------------------------//--------------------------------------------------------

# Visualizar o insert
# print("Query para buscar o movie: ")
# select_movie_find = """
# SELECT m.movieId, m.title, m.genres, r.rating, t.tag, r.userId
# FROM Movies m
# JOIN Rating r ON m.movieId = r.movieId
# JOIN Tags t ON m.movieId = t.movieId AND t.userId = r.userId
# WHERE m.title = 'A luz da noite';
# """

# # Executar a query
# mycursor.execute(select_movie_find)

# result = mycursor.fetchall()

# # Criar um DataFrame a partir dos resultados
# columns = ['movieId', 'title', 'genres', 'rating', 'tag', 'userId']
# result_df = pd.DataFrame(result, columns=columns)

# print(result_df)
# print("\n")

# # #------------------------------------------------UPDATE------------------------------------------

# # Visualizar o update
# # query para dar update do rating
# update_rating_data = {
#     'rating': 3.4,
#     'userId': 2,
# }

# # Dar um novo rating a um movie
# update_rating_query = """UPDATE Rating SET rating = %s, 
#                         timestamp = UNIX_TIMESTAMP()
#                         WHERE movieId = %s
#                         AND userId = %s
#                         """
# mycursor.execute(update_rating_query, (update_rating_data['rating'], new_movie_id, update_rating_data['userId']))

# mydb.commit()

# print(f"Rating atualizado: {update_rating_data['rating']}")
# print("\n")

# # #-----------------------------------------------//----------------------------------------------------

# print("Query para buscar o movie: ")
# select_movie_find = """
# SELECT m.movieId, m.title, m.genres, r.rating, t.tag, r.userId
# FROM Movies m
# JOIN Rating r ON m.movieId = r.movieId
# JOIN Tags t ON m.movieId = t.movieId AND t.userId = r.userId
# WHERE m.title = 'A luz da noite';
# """

# # Executar a query
# mycursor.execute(select_movie_find)


# result = mycursor.fetchall()

# # Criar um DataFrame a partir dos resultados
# columns = ['movieId', 'title', 'genres', 'rating', 'tag', 'userId']
# result_df = pd.DataFrame(result, columns=columns)
# print(result_df)
# print("\n")

# mydb.close()


#------------------------------------------------------------------- GOAL 3 MONGODB QUERIES ------------------------------------------------------------------- 
# QUERY 1
# query para ir buscar todos os filmes que comecem pela letra 'A' e tem o genero de 'Sci-Fi'
# query = {
#     "title": {"$regex": "^A", "$options": "i"},  # Case-insensitive regex for titles starting with 'A'
#     "genres": {"$regex": "Sci-Fi", "$options": "i"}  # Case-insensitive regex for genres containing 'Sci-Fi'
# }

# # Find documents in the movies collection
# docs = mongo_collection_movies.find(query)

# # Create a list to store the results
# result_list = []

# # Iterate through the results and append relevant information to the result list
# for doc in docs:
#     result_list.append({
#         "movieId": doc.get("movieId"),
#         "title": doc.get("title"),
#         "genres": doc.get("genres")
#     })

# # Create a DataFrame from the result list
# columns = ["movieId", "title", "genres"]
# result_df = pd.DataFrame(result_list, columns=columns)

# # Print the DataFrame
# print(result_df)
# print("\n")

#-----------------------------------------------//----------------------------------------------------
# QUERY 2

# Aggregation pipeline to find the genre with the most movies
# pipeline = [
#     {"$group": {"_id": "$genres", "movie_count": {"$sum": 1}}},
#     {"$sort": {"movie_count": -1}},
#     {"$limit": 1}
# ]

# result_list = list(mongo_collection_movies.aggregate(pipeline))

# # Create a DataFrame from the result list
# columns = ['_id', "movie_count"] # tive de meter _id para imprimir bem o Drama (isto é a result_list [{'_id': 'Drama', 'movie_count': 8402}])
# result_df = pd.DataFrame(result_list, columns=columns)
# result_df = result_df.rename(columns={'_id': 'genre'})

# # Print the DataFrame
# print(result_df)
# print("\n")


#-----------------------------------------------//----------------------------------------------------
# QUERY 3

# print("Query para saber que filmes um user avaliou Mongo: ")
# # User ID for which you want to find rated movies
# user_id = 1

# # Find all ratings documents for the user
# ratings = mongo_collection_ratings.find({"userId": user_id})

# # Create a list to store the data
# data = []

# # Extract relevant data from the ratings documents
# for rating in ratings:
#     movie_data = {
#         "userId": rating["userId"],
#         "movieId": rating["movieId"],
#         "rating": rating["rating"],
#         "timestamp": rating["timestamp"]
#     }
#     data.append(movie_data)

# # Create a DataFrame from the list
# result_df = pd.DataFrame(data)

# # Merge with the movies collection to get additional movie details
# result_df = pd.merge(result_df, pd.DataFrame(list(mongo_collection_movies.find())), on="movieId")

# columns= ["movieId", "title", "genres", "rating", "timestamp"]
# print(result_df.to_string(index=True, columns=columns))


#-----------------------------------------------//----------------------------------------------------
# QUERY 4
# query para saber quais foram as tag que um utilizador utilizou para os filmes
# user_id = 14

# # MongoDB query to get tags for movies rated by a specific user
# query = {"userId": user_id}

# # Find documents in the tags collection
# docs = mongo_collection_tags.find(query)

# # Create a list to store the results
# result_list = []
# user_query = {"userId": user_id}
# user_info = mongo_collection_users.find_one(user_query)

# # Iterate through the results and append relevant information to the result list
# for doc in docs:
    
#     movie_query = {"movieId": doc["movieId"]}
#     movie_info = mongo_collection_movies.find_one(movie_query)

#     rating_query = {"userId": user_id, "movieId": doc["movieId"]}
#     rating_info = mongo_collection_ratings.find_one(rating_query)

    
#     if rating_info is not None: #handle de um erro de nonetype
#       # Append relevant information to the result list
#       result_list.append({
#          "movieId": doc.get("movieId"),
#          "title": movie_info.get("title"),
#          "rating": rating_info.get("rating"),
#          "name": user_info.get("name"),
#          "tag": doc.get("tag")
#       })

# # Create a DataFrame from the result list
# columns = ["movieId", "title", "rating", "name", "tag"]
# result_df = pd.DataFrame(result_list, columns=columns)

# # Print the DataFrame
# print(result_df)
# print("\n")





#Em progresso
#-----------------------------------------------//----------------------------------------------------

# query para saber quais foram as tag que um utilizador utilizou para os filmes
# # User ID for which you want to retrieve movie tags
# user_id = 14

# # MongoDB query to get ratings for movies rated by a specific user
# ratings_query = {"userId": user_id}
# ratings = list(mongo_collection_ratings.find(ratings_query))

# # Create a list to store the results
# result_list = []
# user_query = {"userId": user_id}
# user_info = mongo_collection_users.find_one(user_query)

# # Iterate through the ratings and retrieve additional movie and tag information
# for rating in ratings:
#     movie_query = {"movieId": rating["movieId"]}
#     movie_info = mongo_collection_movies.find_one(movie_query)

#     tags_query = {"userId": user_id, "movieId": rating["movieId"]}
#     tags = list(mongo_collection_tags.find(tags_query))

#     # Iterate through tags for the current movie and append relevant information to the result list
#     for tag in tags:
#         result_list.append({
#             "movieId": movie_info["movieId"],
#             "title": movie_info["title"],
#             "rating": rating["rating"],
#             "name": user_info["name"],
#             "tag": tag["tag"]
#         })

# # Create a DataFrame from the result list
# columns = ["movieId", "title", "rating", "name", "tag"]
# result_df = pd.DataFrame(result_list, columns=columns)

# # Print the DataFrame
# print(result_df)

#-----------------------------------------------//----------------------------------------------------

# genre_query = {"genres": {"$regex": "Comedy"}}
# comedy_movies = mongo_collection_movies.find(genre_query)

# # Create a list to store the results
# result_list = []

# # Iterate through the Comedy movies
# for movie in comedy_movies:
#     # Fetch ratings for each movie
#     rating_query = {"movieId": movie["movieId"]}
#     ratings = mongo_collection_ratings.find(rating_query)

#     # Calculate average rating
#     total_ratings = 0
#     count_ratings = 0
#     for rating in ratings:
#         total_ratings += rating["rating"]
#         count_ratings += 1

#     # Avoid division by zero
#     avg_rating = total_ratings / count_ratings if count_ratings > 0 else 0

#     # Append relevant information to the result list
#     result_list.append({
#         "movieId": movie["movieId"],
#         "title": movie["title"],
#         "genres": movie["genres"],
#         "avg_rating": avg_rating
#     })

# # Create a DataFrame from the result list
# columns = ["movieId", "title", "genres", "avg_rating"]
# result_df = pd.DataFrame(result_list, columns=columns)

# # Print the DataFrame
# print(result_df)





#-----------------------------------------------INSERTS----------------------------------------------------
# mongo_collection_counters = mongo_db['Counters']

# # Function to get the next sequence value for a given key
# def get_next_sequence_value(sequence_name):
#     sequence_document = mongo_collection_counters.find_one_and_update(
#         {"_id": sequence_name},
#         {"$inc": {"sequence_value": 1}},
#         upsert=True,
#         return_document=True
#     )
#     return sequence_document["sequence_value"]

# # Find the largest movieId from the movies collection
# largest_movie_id = mongo_collection_movies.find_one(sort=[("movieId", DESCENDING)])

# # Use the largest movieId as the initial value for the counter
# if largest_movie_id:
#     largest_movie_id_value = largest_movie_id.get("movieId", 0)
#     mongo_collection_counters.update_one(
#         {"_id": "movieId"},
#         {"$setOnInsert": {"sequence_value": largest_movie_id_value}},
#         upsert=True
#     )
#     print(f"Largest movieId in the movies collection: {largest_movie_id_value}")


# # Determine the next movieId
# next_movie_id = get_next_sequence_value("movieId")

# # Insert a new movie with an auto-incremented movieId
# new_movie_data = {
#     'movieId': next_movie_id,
#     'title': 'A luz da noite',
#     'genres': 'Adventure'
# }
# mongo_collection_movies.insert_one(new_movie_data)
# print(f"Movie adicionado: {new_movie_data['title']}, Movie ID: {new_movie_data['movieId']}")
# print("\n")

# # Insert ratings for the new movie
# new_ratings_data = [
#     {'userId': 1, 'rating': 4.5},
#     {'userId': 2, 'rating': 3.8},
#     {'userId': 3, 'rating': 5.0}
#     # Add more ratings as needed
# ]

# for rating_data in new_ratings_data:
#     rating_data['timestamp'] = int(datetime.utcnow().timestamp())
#     rating_data['movieId'] = new_movie_data['movieId']
#     mongo_collection_ratings.insert_one(rating_data)
#     print(f"Rating added: {rating_data['rating']} for Movie ID: {new_movie_data['movieId']}")
# print("\n")

# # Insert tags for the new movie
# new_tags_data = [
#     {'userId': 1, 'tag': 'exciting'},
#     {'userId': 2, 'tag': 'must watch'},
#     {'userId': 3, 'tag': 'favorite'}
#     # Add more tags as needed
# ]

# for tag_data in new_tags_data:
#     tag_data['timestamp'] = int(datetime.utcnow().timestamp())
#     tag_data['movieId'] = new_movie_data['movieId']
#     mongo_collection_tags.insert_one(tag_data)
#     print(f"Tag added: {tag_data['tag']} for Movie ID: {new_movie_data['movieId']}")
# print("\n")


# # -----------------------------------------------//----------------------------------------------------
# # Query to find the movie with ratings

# # Specify the title of the movie you want to find
# movie_title = "A luz da noite"

# # Find the movie with the specified title
# movie = mongo_collection_movies.find_one({"title": movie_title})

# if movie:
#     movie_id = movie.get("movieId")
#     print(f"Movie found: {movie_title}, Movie ID: {movie_id}")

#     # Find ratings for the movie
#     movie_ratings = list(mongo_collection_ratings.find({"movieId": movie_id}))

#     # Find tags for the movie
#     movie_tags = list(mongo_collection_tags.find({"movieId": movie_id}))

#     if movie_ratings:
#         # Create a DataFrame from the results
#         columns = ['movieId', 'title', 'genres', 'rating', 'tag', 'userId']
#         result_df = pd.DataFrame(
#             [{"movieId": movie_id, "title": movie["title"], "genres": movie["genres"],
#               "rating": rating["rating"], "tag": tag["tag"], "userId": rating["userId"]} 
#              for rating in movie_ratings for tag in movie_tags if rating["userId"] == tag["userId"]],
#             columns=columns
#         )
#         print(result_df)
#     else:
#         print("No ratings found for the movie.")
# else:
#     print(f"Movie with title '{movie_title}' not found.")


# #-----------------------------------------------UPDATE----------------------------------------------------
# movie_title = "A luz da noite"

# movie = mongo_collection_movies.find_one({"title": movie_title})

# if movie:
#     movie_id = movie.get("movieId")
#     print(f"Movie found: {movie_title}, Movie ID: {movie_id}")

#     # Find ratings for the movie
#     movie_ratings = list(mongo_collection_ratings.find({"movieId": movie_id}))

#     movie_tags = list(mongo_collection_tags.find({"movieId": movie_id}))
    
#     if movie_ratings:
#         # Update the rating for the movie (assuming userId 14 in this example)
#         new_rating_value = 3.4
#         mongo_collection_ratings.update_one(
#             {"movieId": movie_id, "userId": 2},  # Adjust userId as needed
#             {"$set": {"rating": new_rating_value}}
#         )

#         # Show the updated ratings
#         updated_movie_ratings = list(mongo_collection_ratings.find({"movieId": movie_id}))

#         # Create a DataFrame from the results
#         columns = ['movieId', 'title', 'genres', 'rating', 'tag', 'userId']
#         updated_result_df = pd.DataFrame(
#             [{"movieId": movie_id, "title": movie["title"], "genres": movie["genres"],
#               "rating": rating["rating"], "tag": tag["tag"], "userId": rating["userId"]} 
#              for rating in updated_movie_ratings for tag in movie_tags if rating["userId"] == tag["userId"]],
#             columns=columns
#         )
#         print("Updated Ratings:")
#         print(updated_result_df)
#     else:
#         print("No ratings found for the movie.")
# else:
#     print(f"Movie with title '{movie_title}' not found.")


#-------------------------------------------------------------------- GOAL 4 INDEXING AND OPTIMIZATION -------------------------------------------------------------------- 

#------------------------------------------------------- QUERY 1 -----------------------------------------------------------
# query para ir buscar todos os filmes que comecem pela letra 'A' e tem o genero de 'Comedy'
# print("------------------------------------------QUERIES INICIAIS------------------------------------------")
# print("\n")
# print("MySQL")
# print("\n")

# drop_idx_title_genres = """DROP INDEX idx_title_genres ON Movies;"""
# mycursor.execute(drop_idx_title_genres)

# print("Query para ir buscar todos os filmes que comecem pela letra 'A' e tem o genero de 'Sci-Fi': ")
# time_i = time.time()

# select_query_with_A_Comedy = """
# SELECT m.movieId, m.title,m.genres
# FROM Movies m
# WHERE m.genres LIKE '%Sci-Fi%' and m.title LIKE 'a%'
# """

# # Executar a query
# mycursor.execute(select_query_with_A_Comedy)
# resultavg = mycursor.fetchall()

# # Criar um DataFrame a partir dos resultados
# columns = ['movieId', 'title','genres']
# result_avg = pd.DataFrame(resultavg, columns=columns)

# time_f = time.time()
# print('total time MySQL = ', time_f-time_i) #cerca de 0.12335777282714844

# # Explain the query execution plan
# explain_query = f"EXPLAIN {select_query_with_A_Comedy};"
# mycursor.execute(explain_query)
# explain_result = mycursor.fetchall()

# # Print the execution plan
# print("Query Execution Plan:")
# for row in explain_result:
#     print(row)

# # ---------------------------------- RESULTADO EXPLAIN ----------------------------------
# # (1, 'SIMPLE', 'm', None, 'ALL', None, None, None, None, 58191, 1.23, 'Using where')
# # 58191: The estimated number of rows to be examined

# print(result_avg)
# print("\n")

# print("MongoDB") #------------------------------------------------------------------------------------
# print("\n")


# mongo_collection_movies.drop_index([("title", 1), ("genres", 1)])


# print("Query para ir buscar todos os filmes que comecem pela letra 'A' e tem o genero de 'Sci-Fi' MongoDB: ")
# time_i = time.time()

# query = {
#     "title": {"$regex": "^A", "$options": "i"},  # Case-insensitive regex for titles starting with 'A'
#     "genres": {"$regex": "Sci-Fi", "$options": "i"}  # Case-insensitive regex for genres containing 'Comedy'
# }

# # Find documents in the movies collection
# docs = mongo_collection_movies.find(query)

# # Create a list to store the results
# result_list = []

# # Iterate through the results and append relevant information to the result list
# for doc in docs:
#     result_list.append({
#         "movieId": doc.get("movieId"),
#         "title": doc.get("title"),
#         "genres": doc.get("genres")
#     })

# # Create a DataFrame from the result list
# columns = ["movieId", "title", "genres"]
# result_df = pd.DataFrame(result_list, columns=columns)

# time_f = time.time()
# print('total time MongoDB = ', time_f-time_i) # cerca de 0.40848875045776367

# # Use explain to get query execution plan
# explain_result = mongo_collection_movies.find(query).explain()

# # Print the execution plan
# print("Query Execution Plan:")
# for key, value in explain_result.items():
#     print(f"{key}: {value}")

# # ---------------------------------- RESULTADO EXPLAIN ----------------------------------
# # # 'totalKeysExamined': 0, 'totalDocsExamined': 58101S

# # Print the DataFrame
# print(result_df)
# print("\n")

# print("------------------------------------------QUERIES OPTIMIZADAS------------------------------------------")

# idx_title_genres = """CREATE INDEX idx_title_genres ON Movies (title, genres);"""
# mycursor.execute(idx_title_genres)

# print("Query para ir buscar todos os filmes que comecem pela letra 'A' e tem o genero de 'Sci-Fi': ")
# time_i = time.time()

# select_query_with_A_Sci_Fi = """
# SELECT m.movieId, m.title,m.genres
# FROM Movies m
# WHERE m.genres LIKE '%Sci-Fi%' and m.title LIKE 'a%'
# """

# # Executar a query
# mycursor.execute(select_query_with_A_Sci_Fi)
# resultavg = mycursor.fetchall()

# # Criar um DataFrame a partir dos resultados
# columns = ['movieId', 'title','genres']
# result_avg = pd.DataFrame(resultavg, columns=columns)

# time_f = time.time()
# print('total time MySQL = ', time_f-time_i) # cerca de 0.020010709762573242

# # Explain the query execution plan
# explain_query = f"EXPLAIN {select_query_with_A_Sci_Fi};"
# mycursor.execute(explain_query)
# explain_result = mycursor.fetchall()

# # Print the execution plan
# print("Query Execution Plan:")
# for row in explain_result:
#     print(row)

# # ---------------------------------- RESULTADO EXPLAIN ----------------------------------
# # (1, 'SIMPLE', 'm', None, 'range', 'idx_title_genres', 'idx_title_genres', '1023', None, 6608, 11.11, 'Using where; Using index')
# # 6608: The estimated number of rows to be examined

# print(result_avg)
# print("\n")

# #COM O INDEX PASSA DE 0.12335777282714844 PARA 0.020010709762573242

# print("MongoDB") #------------------------------------------------------------------------------------
# print("\n")

# # mongo_collection_movies.create_index([("title", 1), ("genres", 1)])

# print("Query para ir buscar todos os filmes que comecem pela letra 'A' e tem o genero de 'Sci-Fi' MongoDB: ")
# time_i = time.time()

# query = {
#     "title": {"$regex": "^A", "$options": "i"},  # Case-insensitive regex for titles starting with 'A'
#     "genres": {"$regex": "Sci-Fi", "$options": "i"}  # Case-insensitive regex for genres containing 'Sci-Fi'
# }

# # Use projection to fetch only the necessary fields
# projection = {"movieId": 1, "title": 1, "genres": 1, "_id": 0}

# # Find documents in the movies collection
# docs = mongo_collection_movies.find(query, projection)

# # Convert the cursor to a list of dictionaries
# result_list = list(docs)

# # Create a DataFrame directly from the list of dictionaries
# result_df = pd.DataFrame.from_records(result_list, columns=["movieId", "title", "genres"])

# time_f = time.time()
# print('total time MongoDB = ', time_f-time_i) # cerca de 0.3869962692260742

# # Use explain to get query execution plan
# explain_result = mongo_collection_movies.find(query).explain()

# # Print the execution plan
# print("Query Execution Plan:")
# for key, value in explain_result.items():
#     print(f"{key}: {value}")

# # ---------------------------------- RESULTADO EXPLAIN ----------------------------------
# # # USO DE PROJECTION DIMINUI 'totalDocsExamined'
# # # 'totalKeysExamined': 58101, 'totalDocsExamined': 951


# # Print the DataFrame
# print(result_df)
# print("\n")

# # CRIAÇÃO DO INDEX PIORA A PERFORMANCE DA QUERY 
# # COM O USO DO INDEX E PROJECTION PASSA DE 0.40532398223876953 PARA 0.4318406581878662 (PIORA)
# # COM O USO DA PROJECTION PASSA DE 0.40848875045776367 PARA 0.3869962692260742 (MELHORIA POUCO SIGNIFICATIIVA)


#------------------------------------------------------- QUERY 2 -----------------------------------------------------------
# query para ir buscar o género mais popular 
# print("------------------------------------------QUERIES INICIAIS------------------------------------------")
# print("\n")
# print("MySQL")
# print("\n")

# print("Query para ir buscar o género mais popular : ")

# drop_idx_genres = """DROP INDEX idx_genres ON Movies;"""
# mycursor.execute(drop_idx_genres)

# # # Corre-se quando se passa da QUERY 1 para a QUERY 2
# drop_idx_genres = """DROP INDEX idx_title_genres ON Movies;"""
# # mycursor.execute(drop_idx_genres)

# time_i = time.time()

# select_query_count_by_genre = """
# SELECT genres, COUNT(*) AS movie_count
# FROM movies
# GROUP BY genres
# ORDER BY movie_count DESC
# LIMIT 1;
# """
 
# # Executar a query
# mycursor.execute(select_query_count_by_genre)

# resultavg = mycursor.fetchall()

# # Criar um DataFrame a partir dos resultados
# columns = ['genres', "movie_count"]
# result_avg = pd.DataFrame(resultavg, columns=columns)

# time_f = time.time()
# print('total time MySQL = ', time_f-time_i) # cerca de 0.20741772651672363

# # Explain the query execution plan
# explain_query = f"EXPLAIN {select_query_count_by_genre};"
# mycursor.execute(explain_query)
# explain_result = mycursor.fetchall()

# # Print the execution plan
# print("Query Execution Plan:")
# for row in explain_result:
#     print(row)

# # # ---------------------------------- RESULTADO EXPLAIN ----------------------------------
# # (1, 'SIMPLE', 'movies', None, 'ALL', None, None, None, None, 58191, 100.0, 'Using temporary; Using filesort')
# # 58191: The estimated number of rows to be examined 

# print(result_avg)
# print("\n")


# print("MongoDB") #------------------------------------------------------------------------------------
# print("\n")

# print("Query para ir buscar o género mais popular : ")

# mongo_collection_movies.drop_index([("genres", 1)])
# # # Corre-se quando se passa da QUERY 1 para a QUERY 2
# # mongo_collection_movies.drop_index([("title", 1), ("genres", 1)])


# time_i = time.time()

# # Aggregation pipeline to find the genre with the most movies
# pipeline = [
#     {"$group": {"_id": "$genres", "movie_count": {"$sum": 1}}},
#     {"$sort": {"movie_count": -1}},
#     {"$limit": 1}
# ]

# result_list = list(mongo_collection_movies.aggregate(pipeline))

# # Create a DataFrame from the result list
# columns = ['_id', "movie_count"] 
# result_df = pd.DataFrame(result_list, columns=columns)
# result_df = result_df.rename(columns={'_id': 'genre'})

# time_f = time.time()
# print('total time MongoDB = ', time_f-time_i) # cerca de 0.1620786190032959

# # Use explain to analyze the aggregation pipeline
# explanation = mongo_collection_movies.database.command('aggregate', 'movies', pipeline=pipeline, explain=True)

# # Print the explanation
# print(explanation)

# # # ---------------------------------- RESULTADO EXPLAIN ----------------------------------
# # #  'indexFilterSet': False

# # Print the DataFrame
# print(result_df)
# print("\n")


# print("------------------------------------------QUERIES OPTIMIZADAS------------------------------------------")

# print("\n")
# print("MySQL")
# print("\n")

# idx_genres  = """CREATE INDEX idx_genres ON movies (genres);"""
# mycursor.execute(idx_genres)

# print("Query para ir buscar o género mais popular : ")

# time_i = time.time()

# select_query_count_by_genre = """
# SELECT genres, COUNT(*) AS movie_count
# FROM movies
# GROUP BY genres
# ORDER BY movie_count DESC
# LIMIT 1;
# """

# # Executar a query
# mycursor.execute(select_query_count_by_genre)

# resultavg = mycursor.fetchall()

# # Criar um DataFrame a partir dos resultados
# columns = ['genres', "movie_count"]
# result_avg = pd.DataFrame(resultavg, columns=columns)

# time_f = time.time()
# print('total time MySQL = ', time_f-time_i) # cerca de 0.09758877754211426

# # Explain the query execution plan
# explain_query = f"EXPLAIN {select_query_count_by_genre};"
# mycursor.execute(explain_query)
# explain_result = mycursor.fetchall()

# # Print the execution plan
# print("Query Execution Plan:")
# for row in explain_result:
#     print(row)

# # # ---------------------------------- RESULTADO EXPLAIN ----------------------------------
# # # (1, 'SIMPLE', 'movies', None, 'index', 'idx_genres', 'idx_genres', '1023', None, 58191, 100.0, 'Using index; Using temporary; Using filesort')
# # # 58191: The estimated number of rows to be examined 

# print(result_avg)
# print("\n")

# print("MongoDB") #------------------------------------------------------------------------------------
# print("\n")

# mongo_collection_movies.create_index([("genres", 1)])
# # mongo_collection_movies.create_index([("title", 1), ("genres", 1)])


# print("Query para ir buscar o género mais popular: ")

# time_i = time.time()

# # Aggregation pipeline to find the genre with the most movies
# pipeline = [
#     {"$group": {"_id": "$genres", "movie_count": {"$sum": 1}}},
#     {"$sort": {"movie_count": -1}},
#     {"$limit": 1},
# ]

# result_list = list(mongo_collection_movies.aggregate(pipeline))

# # Create a DataFrame from the result list
# columns = ['_id', "movie_count"] 
# result_df = pd.DataFrame(result_list, columns=columns)
# result_df = result_df.rename(columns={'_id': 'genre'})

# time_f = time.time()
# print('total time MongoDB = ', time_f-time_i) # cerca de 0.15456342697143555

# # Use explain to analyze the aggregation pipeline
# explanation = mongo_collection_movies.database.command('aggregate', 'movies', pipeline=pipeline, explain=True)

# # Print the explanation
# print(explanation)

# # ---------------------------------- RESULTADO EXPLAIN ----------------------------------
# # # EMBORA TENHAM SIDO CRIADOS OS INDEXES NAO FORAM UTILIZADOS 
# # # 'indexFilterSet': False

# # Print the DataFrame
# print(result_df)
# print("\n")


# #------------------------------------------------------- QUERY 3 -----------------------------------------------------------
# # query para ir buscar Filmes que um utilizador avaliou
# print("------------------------------------------QUERIES INICIAIS------------------------------------------")
# print("\n")
# print("MySQL")
# print("\n")

# print("Query para saber que filmes um user avaliou: ")

# drop_idx_user_movie = """DROP INDEX idx_user_movie ON Rating;"""
# # Usar caso aidna existam de testes anteriores
# drop_idx_user_movieR = """DROP INDEX idx_user_movieR ON Rating;"""
# drop_idx_user_movieT = """DROP INDEX idx_user_movieT ON Tags;"""

# # query para ver os indexes
# query = """
# SELECT TABLE_NAME, INDEX_NAME, COLUMN_NAME, NON_UNIQUE, SEQ_IN_INDEX, CARDINALITY, INDEX_TYPE
# FROM information_schema.STATISTICS
# WHERE TABLE_NAME = 'Rating';
# """

# # mycursor.execute(drop_idx_user_movie)
# # # mycursor.execute(drop_idx_user_movieR)
# # # mycursor.execute(drop_idx_user_movieT)


# time_i = time.time()

# select_query_filmes_avaliados_by_user = """
# SELECT m.movieId, m.title, m.genres, r.rating, r.timestamp
# FROM Rating r
# JOIN Movies m ON r.movieId = m.movieId
# INNER JOIN Users u ON u.userId = r.userId
# WHERE r.userId = 1;
# """

# # Executar a query
# mycursor.execute(select_query_filmes_avaliados_by_user)

# result = mycursor.fetchall()


# # Criar um DataFrame a partir dos resultados
# columns = ['movieId', 'title', 'genres', 'rating', 'timestamp']
# result_df = pd.DataFrame(result, columns=columns)

# time_f = time.time() # cerca de 2.7164535522460938
# print('total time MySQL = ', time_f-time_i) 

# # Explain the query execution plan
# explain_query = f"EXPLAIN {select_query_filmes_avaliados_by_user};"
# mycursor.execute(explain_query)
# explain_result = mycursor.fetchall()

# # Print the execution plan
# print("Query Execution Plan:")
# for row in explain_result:
#     print(row)

# # # ---------------------------------- RESULTADO EXPLAIN ----------------------------------
# # # Users table 
# # (1, 'SIMPLE', 'u', None, 'const', 'PRIMARY', 'PRIMARY', '4', 'const', 1, 100.0, 'Using index')
# # 4: The number of rows to be examined using this join

# # # Rating table
# # (1, 'SIMPLE', 'r', None, 'ALL', 'rating_ibfk_2', None, None, None, 997545, 10.0, 'Using where')
# # 'rating_ibfk_2': The index used for the join
# # 10.0: The estimated percentage of the table that will be scanned.

# # # Movies table
# # (1, 'SIMPLE', 'm', None, 'eq_ref', 'PRIMARY', 'PRIMARY', '4', 'projectdba.r.movieId', 1, 100.0, None)
# # 4: The number of rows to be examined using this join


# print(result_df)
# print("\n")


# print("MongoDB") #------------------------------------------------------------------------------------
# print("\n")

# print("Query para saber que filmes um user avaliou: ")

# # mongo_collection_ratings.drop_index([("userId", 1), ("movieId", 1)])

# time_i = time.time()

# # User ID for which you want to find rated movies
# user_id = 1

# # Find all ratings documents for the user
# ratings = mongo_collection_ratings.find({"userId": user_id})

# # Create a list to store the data
# data = []

# # Extract relevant data from the ratings documents
# for rating in ratings:
#     movie_data = {
#         "userId": rating["userId"],
#         "movieId": rating["movieId"],
#         "rating": rating["rating"],
#         "timestamp": rating["timestamp"]
#     }
#     data.append(movie_data)

# # Create a DataFrame from the list
# result_df = pd.DataFrame(data)

# # Merge with the movies collection to get additional movie details
# result_df = pd.merge(result_df, pd.DataFrame(list(mongo_collection_movies.find())), on="movieId")

# columns= ["movieId", "title", "genres", "rating", "timestamp"]

# time_f = time.time()
# print('total time MongoDB = ', time_f-time_i) # cerca de 2.2778894901275635


# # Use explain to get query execution plan
# explain_result = mongo_collection_ratings.find({"userId": user_id}).explain()

# # Print the execution plan
# print("Query Execution Plan:")
# for key, value in explain_result.items():
#     print(f"{key}: {value}")

# # ---------------------------------- RESULTADO EXPLAIN ----------------------------------
# # # SEM INDEXES
# # 'indexFilterSet': False
# # 'totalKeysExamined': 0, 'totalDocsExamined': 1000017,


# # Print the DataFrame
# print(result_df.to_string(index=True, columns=columns))
# print("\n")


# print("------------------------------------------QUERIES OPTIMIZADAS------------------------------------------")

# print("\n")
# print("MySQL")
# print("\n")

# print("Query para saber que filmes um user avaliou: ")

# idx_user_movies = """CREATE INDEX idx_user_movie ON Rating (userId, movieId);"""
# mycursor.execute(idx_user_movies)

# time_i = time.time()

# select_query_filmes_avaliados_by_user = """
# SELECT m.movieId, m.title, m.genres, r.rating, r.timestamp
# FROM Rating r
# JOIN Movies m ON r.movieId = m.movieId
# INNER JOIN Users u ON u.userId = r.userId
# WHERE r.userId = 1;
# """

# # Executar a query
# mycursor.execute(select_query_filmes_avaliados_by_user)

# result = mycursor.fetchall()


# # Criar um DataFrame a partir dos resultados
# columns = ['movieId', 'title', 'genres', 'rating', 'timestamp']
# result_df = pd.DataFrame(result, columns=columns)

# time_f = time.time()
# print('total time MySQL = ', time_f-time_i) # cerca de 0.004016876220703125

# # Explain the query execution plan
# explain_query = f"EXPLAIN {select_query_filmes_avaliados_by_user};"
# mycursor.execute(explain_query)
# explain_result = mycursor.fetchall()

# # Print the execution plan
# print("Query Execution Plan:")
# for row in explain_result:
#     print(row)

# # # ---------------------------------- RESULTADO EXPLAIN ----------------------------------
# # # Users table 
# # (1, 'SIMPLE', 'u', None, 'const', 'PRIMARY', 'PRIMARY', '4', 'const', 1, 100.0, 'Using index')
# # 4: The number of rows to be examined using this join

# # # Rating table
# # (1, 'SIMPLE', 'r', None, 'ref', 'rating_ibfk_2,idx_user_movie', 'idx_user_movie', '5', 'const', 17, 100.0, 'Using index condition')
# # 'rating_ibfk_2,idx_user_movie': Indexes used for the table. In this case, it uses the composite index 'rating_ibfk_2' and 'idx_user_movie'.
# # 'idx_user_movie': The specific index being used
# # 100.0: The estimated percentage of the table that will be scanned. In this case, it's a full scan ('Using index condition')

# # # Movies table
# # (1, 'SIMPLE', 'm', None, 'eq_ref', 'PRIMARY', 'PRIMARY', '4', 'projectdba.r.movieId', 1, 100.0, None)

# print(result_df)
# print("\n")

# print("MongoDB") #------------------------------------------------------------------------------------
# print("\n")

# print("Query para saber que filmes um user avaliou: ")

# mongo_collection_ratings.create_index([("userId", 1), ("movieId", 1)])


# time_i = time.time()

# # User ID for which you want to find rated movies
# user_id = 1

# projection = {"_id": 0, "userId": 1, "movieId": 1, "rating": 1, "timestamp": 1}

# # CHANGED only getting the fields needed (using the projection)
# ratings = mongo_collection_ratings.find({"userId": user_id}, projection)

# # Create a list to store the data
# data = []

# # Extract relevant data from the ratings documents
# for rating in ratings:
#     movie_data = {
#         "userId": rating["userId"],
#         "movieId": rating["movieId"],
#         "rating": rating["rating"],
#         "timestamp": rating["timestamp"]
#     }
#     data.append(movie_data)

# # Create a DataFrame from the list
# result_df = pd.DataFrame(data)

# # Merge with the movies collection to get additional movie details
# result_df = pd.merge(result_df, pd.DataFrame(list(mongo_collection_movies.find())), on="movieId")

# columns= ["movieId", "title", "genres", "rating", "timestamp"]

# time_f = time.time()
# print('total time MongoDB = ', time_f-time_i) # cerca de 0.7196941375732422

# # Use explain to get query execution plan
# explain_result = mongo_collection_ratings.find({"userId": user_id}, projection).explain()

# # Print the execution plan
# print("Query Execution Plan:")
# for key, value in explain_result.items():
#     print(f"{key}: {value}")

# # ---------------------------------- RESULTADO EXPLAIN ----------------------------------
# # # USO DE INDEX E PROJECTION DIMINUI 'totalDocsExamined'
# # 'indexesUsed': ['userId_1_movieId_1']
# # 'totalKeysExamined': 17, 'totalDocsExamined': 17

# # Print the DataFrame
# print(result_df.to_string(index=True, columns=columns))
# print("\n")

#------------------------------------------------------- QUERY 4 -----------------------------------------------------------
# # query para saber quais foram as tag que um utilizador utilizou para os filmes
# print("------------------------------------------QUERIES INICIAIS------------------------------------------")
# print("\n")
# print("MySQL")
# print("\n")

# print("Query para saber quais foram as tag que um utilizador utilizou para os filmes: ")

# drop_idx_user_movieR = """DROP INDEX idx_user_movieR ON Rating;"""
# drop_idx_user_movieT = """DROP INDEX idx_user_movieT ON Tags;"""
# drop_idx_user_movie = """DROP INDEX idx_user_movie ON Rating;"""



# mycursor.execute(drop_idx_user_movieR)
# mycursor.execute(drop_idx_user_movieT)
# # Correr na passagem de QUERY 3 para QUERY 4
# # mycursor.execute(drop_idx_user_movie)


# # result = mycursor.fetchall()

# # print(result)



# time_i = time.time()

# select_query_user_vote = """
# SELECT m.movieId, m.title, r.rating, u.name, t.tag
# FROM Users u
# INNER JOIN Rating r ON u.userId = r.userId
# INNER JOIN Movies m ON r.movieId = m.movieId
# INNER JOIN Tags t ON u.userId = t.userId AND m.movieId = t.movieId
# WHERE u.userId = 14;
# """

# # Executar a query
# mycursor.execute(select_query_user_vote)


# resultUser = mycursor.fetchall()

# # Criar um DataFrame a partir dos resultados
# columns = ['movieId', 'title', 'rating', 'name', 'tag']
# result_userVote = pd.DataFrame(resultUser, columns=columns)

# time_f = time.time() # cerca de 4.548292636871338
# print('total time MySQL = ', time_f-time_i) 

# # Explain the query execution plan
# explain_query = f"EXPLAIN {select_query_user_vote};"
# mycursor.execute(explain_query)
# explain_result = mycursor.fetchall()

# # Print the execution plan
# print("Query Execution Plan:")
# for row in explain_result:
#     print(row)

# # # ---------------------------------- RESULTADO EXPLAIN ----------------------------------
# # # Users table 
# # (1, 'SIMPLE', 'u', None, 'const', 'PRIMARY', 'PRIMARY', '4', 'const', 1, 100.0, None)
# # 4: The number of rows to be examined 

# # # Ratings table 
# # (1, 'SIMPLE', 'r', None, 'ALL', 'rating_ibfk_2', None, None, None, 997545, 10.0, 'Using where')
# # 10.0: The percentage of how many rows were filtered.

# # # Movies table 
# # (1, 'SIMPLE', 'm', None, 'eq_ref', 'PRIMARY', 'PRIMARY', '4', 'projectdba.r.movieId', 1, 100.0, None)
# # 1: The number of rows to be examined

# # # Tags table 
# # (1, 'SIMPLE', 't', None, 'ref', 'movieId', 'movieId', '5', 'projectdba.r.movieId', 27, 10.0, 'Using where')
# # 27: The number of rows to be examined.
# # 10.0: The percentage of how many rows were filtered.

# print(result_userVote)
# print("\n")


# print("MongoDB") #------------------------------------------------------------------------------------
# print("\n")

# print("Query para saber que filmes um user avaliou: ")

# mongo_collection_tags.drop_index([("userId", 1), ("movieId", 1)])
# # Correr na passagem de QUERY 3 para QUERY 4
# # mongo_collection_ratings.drop_index([("userId", 1), ("movieId", 1)])


# time_i = time.time()

# user_id = 14

# # MongoDB query to get tags for movies rated by a specific user
# query = {"userId": user_id}

# # Find documents in the tags collection
# docs = mongo_collection_tags.find(query)

# # Create a list to store the results
# result_list = []
# user_query = {"userId": user_id}
# user_info = mongo_collection_users.find_one(user_query)

# # Iterate through the results and append relevant information to the result list
# for doc in docs:
    
#     movie_query = {"movieId": doc["movieId"]}
#     movie_info = mongo_collection_movies.find_one(movie_query)

#     rating_query = {"userId": user_id, "movieId": doc["movieId"]}
#     rating_info = mongo_collection_ratings.find_one(rating_query)

    
#     if rating_info is not None: #handle de um erro de nonetype
#       # Append relevant information to the result list
#       result_list.append({
#          "movieId": doc.get("movieId"),
#          "title": movie_info.get("title"),
#          "rating": rating_info.get("rating"),
#          "name": user_info.get("name"),
#          "tag": doc.get("tag")
#       })


# # Create a DataFrame from the result list
# columns = ["movieId", "title", "rating", "name", "tag"]
# result_df = pd.DataFrame(result_list, columns=columns)


# time_f = time.time()
# print('total time MongoDB = ', time_f-time_i) # cerca de 3.242851734161377

# # # Use explain to get query execution plan
# explain_result = mongo_collection_tags.find(query).explain()

# # Print the execution plan
# print("Query Execution Plan:")
# for key, value in explain_result.items():
#     print(f"{key}: {value}")

# # # ---------------------------------- RESULTADO EXPLAIN ----------------------------------
# # # 'totalKeysExamined': 0, 'totalDocsExamined': 1000009


# # Print the DataFrame
# print(result_df)
# print("\n")


# print("------------------------------------------QUERIES OPTIMIZADAS------------------------------------------")

# print("\n")
# print("MySQL")
# print("\n")

# print("Query para saber quais foram as tag que um utilizador utilizou para os filmes: ")

# idx_user_moviesR = """CREATE INDEX idx_user_movieR ON Rating (userId, movieId);"""
# idx_user_moviesT = """CREATE INDEX idx_user_movieT ON Tags (userId, movieId);"""

# mycursor.execute(idx_user_moviesR)
# mycursor.execute(idx_user_moviesT)


# time_i = time.time()

# select_query_user_vote = """
# SELECT m.movieId, m.title, r.rating, u.name, t.tag
# FROM Users u
# INNER JOIN Rating r ON u.userId = r.userId
# INNER JOIN Movies m ON r.movieId = m.movieId
# INNER JOIN Tags t ON u.userId = t.userId AND m.movieId = t.movieId
# WHERE u.userId = 14;
# """

# # Executar a query
# mycursor.execute(select_query_user_vote)


# resultUser = mycursor.fetchall()

# # Criar um DataFrame a partir dos resultados
# columns = ['movieId', 'title', 'rating', 'name', 'tag']
# result_userVote = pd.DataFrame(resultUser, columns=columns)

# time_f = time.time() # cerca de 0.009812593460083008
# print('total time MySQL = ', time_f-time_i) 

# # Explain the query execution plan
# explain_query = f"EXPLAIN {select_query_user_vote};"
# mycursor.execute(explain_query)
# explain_result = mycursor.fetchall()

# # Print the execution plan
# print("Query Execution Plan:")
# for row in explain_result:
#     print(row)

# # # ---------------------------------- RESULTADO EXPLAIN ----------------------------------
# # # Users table 
# # (1, 'SIMPLE', 'u', None, 'const', 'PRIMARY', 'PRIMARY', '4', 'const', 1, 100.0, None)
# # 4: The number of rows to be examined 

# # # Tags table 
# # (1, 'SIMPLE', 't', None, 'ref', 'movieId,idx_user_movieT', 'idx_user_movieT', '5', 'const', 13, 100.0, 'Using index condition')
# # 100.0: The percentage of how many rows were filtered.
# # 'movieId,idx_user_movieT': The index used.
# # 5: The number of rows to be examined.
# # 13: The number of rows to be examined (using an index condition).

# # # Ratings table 
# # (1, 'SIMPLE', 'r', None, 'ref', 'rating_ibfk_2,idx_user_movieR', 'idx_user_movieR', '10', 'const,projectdba.t.movieId', 1, 100.0, None)
# # 'rating_ibfk_2,idx_user_movieR': The index used.
# # 10: The number of rows to be examined.

# # # Movies table 
# # (1, 'SIMPLE', 'm', None, 'eq_ref', 'PRIMARY', 'PRIMARY', '4', 'projectdba.t.movieId', 1, 100.0, None)
# # 100.0: The percentage of how many rows were filtered.

# print(result_userVote)
# print("\n")

# print("MongoDB") #------------------------------------------------------------------------------------
# print("\n")

# print("Query para saber que filmes um user avaliou: ")

# mongo_collection_tags.create_index([("userId", 1), ("movieId", 1)])

# time_i = time.time()

# user_id = 14

# # MongoDB query to get tags for movies rated by a specific user
# query = {"userId": user_id}

# # Find documents in the tags collection
# docs = mongo_collection_tags.find(query)

# # Create a list to store the results
# result_list = []
# user_query = {"userId": user_id}
# user_info = mongo_collection_users.find_one(user_query)

# # Iterate through the results and append relevant information to the result list
# for doc in docs:
    
#     movie_query = {"movieId": doc["movieId"]}
#     movie_info = mongo_collection_movies.find_one(movie_query)

#     rating_query = {"userId": user_id, "movieId": doc["movieId"]}
#     rating_info = mongo_collection_ratings.find_one(rating_query)

    
#     if rating_info is not None: #handle de um erro de nonetype
#       # Append relevant information to the result list
#       result_list.append({
#          "movieId": doc.get("movieId"),
#          "title": movie_info.get("title"),
#          "rating": rating_info.get("rating"),
#          "name": user_info.get("name"),
#          "tag": doc.get("tag")
#       })

# # Create a DataFrame from the result list
# columns = ["movieId", "title", "rating", "name", "tag"]
# result_df = pd.DataFrame(result_list, columns=columns)


# time_f = time.time()
# print('total time MongoDB = ', time_f-time_i) # cerca de 3.6900792121887207

# # # Use explain to get query execution plan
# explain_result = mongo_collection_tags.find(query).explain()

# # Print the execution plan
# print("Query Execution Plan:")
# for key, value in explain_result.items():
#     print(f"{key}: {value}")

# # # ---------------------------------- RESULTADO EXPLAIN ----------------------------------
# # # 'indexesUsed': ['userId_1_movieId_1']
# # # 'totalKeysExamined': 13, 'totalDocsExamined': 13


# # Print the DataFrame
# print(result_df)
# print("\n")

#------------------------------------------------------- INSERT -----------------------------------------------------------

# query para inserir um movie com ratings e tags
# print("------------------------------------------QUERIES INICIAIS------------------------------------------")
# new_movie_data = {
#     'title': 'A luz no escuro',
#     'genres': 'Drama'
# }

# # mydb.commit()
# new_ratings_data = [
#     {'userId': 4, 'rating': 4.6},
#     {'userId': 5, 'rating': 3.2},
#     {'userId': 6, 'rating': 5.0}
# ]

# new_tags_data = [
#     {'userId': 4, 'tag': 'exciting'},
#     {'userId': 5, 'tag': 'must watch'},
#     {'userId': 6, 'tag': 'favorite'}
# ]

# time_i = time.time()

# # Insert a new movie
# insert_movie_query = "INSERT INTO Movies (title, genres) VALUES (%s, %s)"
# mycursor.execute(insert_movie_query, (new_movie_data['title'], new_movie_data['genres']))
# mydb.commit()

# # Get the last inserted movieId
# mycursor.execute("SELECT LAST_INSERT_ID()")
# new_movie_id = mycursor.fetchone()[0]
# print("New movie id:", new_movie_id)

# # Reinitialize cursor
# mycursor = mydb.cursor()

# # Insert ratings for the new movie
# insert_ratings_query = "INSERT INTO Rating (userId, movieId, rating, timestamp) VALUES (%s, %s, %s, UNIX_TIMESTAMP())"
# for rating_data in new_ratings_data:
#     mycursor.execute(insert_ratings_query, (rating_data['userId'], new_movie_id, rating_data['rating']))
#     mydb.commit()

# # Reinitialize cursor
# mycursor = mydb.cursor()

# # Insert tags for the new movie
# insert_tags_query = "INSERT INTO Tags (userId, movieId, tag, timestamp) VALUES (%s, %s, %s, UNIX_TIMESTAMP())"
# for tag_data in new_tags_data:
#     mycursor.execute(insert_tags_query, (tag_data['userId'], new_movie_id, tag_data['tag']))
#     mydb.commit()

# time_f = time.time()
# print('total time MySQL = ', time_f-time_i) # cerca de 0.06079888343811035

# # Reinitialize cursor
# mycursor = mydb.cursor()

# print("MongoDB") #------------------------------------------------------------------------------------
# print("\n")

# mongo_collection_counters = mongo_db['Counters']

# time_i = time.time()
# # Function to get the next sequence value for a given key
# def get_next_sequence_value(sequence_name):
#     sequence_document = mongo_collection_counters.find_one_and_update(
#         {"_id": sequence_name},
#         {"$inc": {"sequence_value": 1}},
#         upsert=True,
#         return_document=True
#     )
#     return sequence_document["sequence_value"]

# # Find the largest movieId from the movies collection
# largest_movie_id = mongo_collection_movies.find_one(sort=[("movieId", DESCENDING)])

# # Use the largest movieId as the initial value for the counter
# if largest_movie_id:
#     largest_movie_id_value = largest_movie_id.get("movieId", 0)
#     mongo_collection_counters.update_one(
#         {"_id": "movieId"},
#         {"$setOnInsert": {"sequence_value": largest_movie_id_value}},
#         upsert=True
#     )
#     print(f"Largest movieId in the movies collection: {largest_movie_id_value}")


# # Determine the next movieId
# next_movie_id = get_next_sequence_value("movieId")

# # Insert a new movie with an auto-incremented movieId
# new_movie_data = {
#     'movieId': next_movie_id,
#     'title': 'A luz no escuro',
#     'genres': 'Adventure'
# }
# mongo_collection_movies.insert_one(new_movie_data)
# print(f"Movie adicionado: {new_movie_data['title']}, Movie ID: {new_movie_data['movieId']}")
# print("\n")

# # Insert ratings for the new movie
# new_ratings_data = [
#     {'userId': 4, 'rating': 4.6},
#     {'userId': 5, 'rating': 3.2},
#     {'userId': 6, 'rating': 5.0}
#     # Add more ratings as needed
# ]

# for rating_data in new_ratings_data:
#     rating_data['timestamp'] = int(datetime.utcnow().timestamp())
#     rating_data['movieId'] = new_movie_data['movieId']
#     mongo_collection_ratings.insert_one(rating_data)
#     print(f"Rating added: {rating_data['rating']} for Movie ID: {new_movie_data['movieId']}")
# print("\n")

# # Insert tags for the new movie
# new_tags_data = [
#     {'userId': 4, 'tag': 'exciting'},
#     {'userId': 5, 'tag': 'must watch'},
#     {'userId': 6, 'tag': 'favorite'}
#     # Add more tags as needed
# ]

# for tag_data in new_tags_data:
#     tag_data['timestamp'] = int(datetime.utcnow().timestamp())
#     tag_data['movieId'] = new_movie_data['movieId']
#     mongo_collection_tags.insert_one(tag_data)
#     print(f"Tag added: {tag_data['tag']} for Movie ID: {new_movie_data['movieId']}")

# time_f = time.time()
# print('total time MongoDB = ', time_f-time_i) # cerca de 0.21699953079223633

# print("\n")


# print("------------------------------------------QUERIES OPTIMIZADAS------------------------------------------")

# # Define new movie data
# new_movie_data = {
#     'title': 'A luz de tarde',
#     'genres': 'Comedy'
# }

# # Define new ratings and tags data
# new_ratings_data = [
#     {'userId': 4, 'rating': 4.5},
#     {'userId': 5, 'rating': 3.8},
#     {'userId': 6, 'rating': 5.0}
# ]

# new_tags_data = [
#     {'userId': 4, 'tag': 'exciting'},
#     {'userId': 5, 'tag': 'must watch'},
#     {'userId': 6, 'tag': 'favorite'}
# ]


# try:
#     time_i = time.time()
#     # Insert a new movie
#     insert_movie_query = "INSERT INTO Movies (title, genres) VALUES (%s, %s)"
#     mycursor.execute(insert_movie_query, (new_movie_data['title'], new_movie_data['genres']))

#     # Get the last inserted movieId
#     mycursor.execute("SELECT LAST_INSERT_ID()")
#     new_movie_id = mycursor.fetchone()[0]
#     print("New movie id:", new_movie_id)

#     # Insert ratings for the new movie
#     insert_ratings_query = "INSERT INTO Rating (userId, movieId, rating, timestamp) VALUES (%s, %s, %s, UNIX_TIMESTAMP())"
#     for rating_data in new_ratings_data:
#         mycursor.execute(insert_ratings_query, (rating_data['userId'], new_movie_id, rating_data['rating']))

#     # Insert tags for the new movie
#     insert_tags_query = "INSERT INTO Tags (userId, movieId, tag, timestamp) VALUES (%s, %s, %s, UNIX_TIMESTAMP())"
#     for tag_data in new_tags_data:
#         mycursor.execute(insert_tags_query, (tag_data['userId'], new_movie_id, tag_data['tag']))

#     # Commit the transaction
#     mydb.commit()
#     time_f = time.time()
#     print('total time MySQL = ', time_f-time_i) # cerca de 0.03068256378173828

# except Exception as e:
#     # Rollback the transaction in case of an error
#     print(f"Error: {e}")
#     mydb.rollback()

# finally:
#     # Close the cursor
#     mycursor.close()


# print("MongoDB") #------------------------------------------------------------------------------------
# print("\n")

# mongo_collection_counters = mongo_db['Counters']

# time_i = time.time()
# # Function to get the next sequence value for a given key
# def get_next_sequence_value(sequence_name):
#     sequence_document = mongo_collection_counters.find_one_and_update(
#         {"_id": sequence_name},
#         {"$inc": {"sequence_value": 1}},
#         upsert=True,
#         return_document=True
#     )
#     return sequence_document["sequence_value"]

# # Find the largest movieId from the movies collection
# largest_movie_id = mongo_collection_movies.find_one(sort=[("movieId", DESCENDING)])

# # Use the largest movieId as the initial value for the counter
# if largest_movie_id:
#     largest_movie_id_value = largest_movie_id.get("movieId", 0)
#     mongo_collection_counters.update_one(
#         {"_id": "movieId"},
#         {"$setOnInsert": {"sequence_value": largest_movie_id_value}},
#         upsert=True
#     )
#     print(f"Largest movieId in the movies collection: {largest_movie_id_value}")

# # Determine the next movieId
# next_movie_id = get_next_sequence_value("movieId")

# # Insert a new movie with an auto-incremented movieId
# new_movie_data = {
#     'movieId': next_movie_id,
#     'title': 'A luz de tarde',
#     'genres': 'Comedy'
# }
# mongo_collection_movies.insert_one(new_movie_data)
# print(f"Movie added: {new_movie_data['title']}, Movie ID: {new_movie_data['movieId']}")
# print("\n")

# # Insert ratings and tags for the new movie TUDO NO MESMO LOOP PARA SER MAIS SUFICIENTE
# new_data = [
#     {'userId': 4, 'rating': 4.5, 'tag': 'exciting'},
#     {'userId': 5, 'rating': 3.8, 'tag': 'must watch'},
#     {'userId': 6, 'rating': 5.0, 'tag': 'favorite'}
# ]

# for data in new_data:
#     data['timestamp'] = int(datetime.utcnow().timestamp())
#     data['movieId'] = new_movie_data['movieId']
    
#     # Insert ratings
#     mongo_collection_ratings.insert_one({'userId': data['userId'], 'movieId': data['movieId'], 'rating': data['rating'], 'timestamp': data['timestamp']})
#     print(f"Rating added: {data['rating']} for Movie ID: {new_movie_data['movieId']}")

#     # Insert tags
#     mongo_collection_tags.insert_one({'userId': data['userId'], 'movieId': data['movieId'], 'tag': data['tag'], 'timestamp': data['timestamp']})
#     print(f"Tag added: {data['tag']} for Movie ID: {new_movie_data['movieId']}")

# time_f = time.time()
# print('total time MongoDB = ', time_f-time_i) # cerca de 0.1590101718902588
    
# print("\n")


# ------------------------------------------------------------------ TENTATIVA MONGO OPTIMIZATION ------------------------------------------------------------------


# ------------------------------------ BE CAREFUL THIS CREATION TAKES A LOT OF TIME -----------------------------------
# merged_collection = mongo_db['MergedCollection']
# 
# Read data from CSV files
# df_movies = pd.read_csv('movies.csv')
# print("df_movies")
# df_users = pd.read_csv('users_data.csv')
# print("df_users")
# df_ratings = pd.read_csv('ratings.csv')
# print("df_ratings")
# df_tags = pd.read_csv('tags.csv')
# print("df_tags")


# # Merge dataframes based on common columns
# merged_df = pd.merge(df_ratings, df_tags, on=['userId', 'movieId'], how='outer')
# print("merged_df 1")
# merged_df = pd.merge(merged_df, df_movies, on='movieId', how='outer')
# print("merged_df 2")
# merged_df = pd.merge(merged_df, df_users, on='userId', how='outer')
# print("merged_df 3")


# # Handle NaN values
# merged_df = merged_df.fillna('default_value')
# print("merged_df 4")

# # Create a new collection
# merged_collection = mongo_db['MergedCollection']
# print("Collection created")


# # Convert the merged dataframe to a dictionary and insert into the collection
# records = merged_df.to_dict(orient='records')
# print("records")


# # Add a progress bar
# with tqdm(total=len(records), desc="Inserting Records", unit="record") as pbar:
#     # Insert records one by one
#     for record in records:
#         merged_collection.insert_one(record)
#         pbar.update(1)

# print("Data insertion completed.")


# -------------------------------------------------------------QUERY 1 -------------------------------------------------------------

# merged_collection.create_index([("title", 1), ("genres", 1)])

# query = {
#     "title": {"$regex": "^A", "$options": "i"},  # Case-insensitive regex for titles starting with 'A'
#     "genres": {"$regex": "Sci-Fi", "$options": "i"}  # Case-insensitive regex for genres containing 'Sci-Fi'
# }

# time_i = time.time()

# # Find documents in the merged_collection
# docs = merged_collection.find(query)

# # Create a list to store the results
# result_list = []

# # Iterate through the results and append relevant information to the result list
# for doc in docs:
#     result_list.append({
#         "movieId": doc.get("movieId"),
#         "title": doc.get("title"),
#         "genres": doc.get("genres")
#     })


# # Create a DataFrame from the result list
# columns = ["movieId", "title", "genres"]
# result_df = pd.DataFrame(result_list, columns=columns)

# time_f = time.time()
# print('total time MongoDB = ', time_f-time_i) # cerca de 135.5147156715393

# # Print the DataFrame
# print(result_df)
# print("\n")
