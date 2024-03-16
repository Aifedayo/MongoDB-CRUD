from dotenv import load_dotenv, find_dotenv
from datetime import datetime as dt
import os
import pprint

from pymongo import MongoClient

load_dotenv(find_dotenv())

password = os.environ.get("MONGODB_PWD")

connection_string = f"mongodb+srv://akeemifedayolag:{password}@tutorial-cluster.k5y6y.mongodb.net/?retryWrites=true&w=majority&appName=Tutorial-Cluster&authSource=admin"

client = MongoClient(connection_string)
dbs = client.list_database_names()
production = (client.production)

def create_book_collection():
    # SCHEME VALIDATION
    book_validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["title", "authors", "publish_year", "type", "copies"],
            "properties": {
                "title": {
                    "bsonType": "string",
                    "description": "must be a string and is not empty"
                },
                "authors": {
                    "bsonType": "array",
                    "items": {
                        "bsonType": "objectId",
                        "description": "must be a objectId and is not empty"
                    }
                },
                "publish_year": {
                    "bsonType": "date", 
                    "minimum": 1900,
                    "description": "must be a date and is not empty"
                },
                "type": {
                    "enum": ["Fiction", "Non-Fiction"],
                    "description": "Can only be one of: Fiction, Non-Fiction"    
                },
                "copies": {
                    "bsonType": "int",
                    "minimum": 0,
                    "description": "must be a non-negative integer"
                }
            },
        },
        "validationAction": "error"
    }

    # Create a book collection
    try:
        # book collection = production.book_collection
        production.create_collection("book_collection")
    except Exception as e:
        print(f"Failed to create collection: {e}")


    production.command("collMod", "book_collection", validator=book_validator) # Modify collection


def create_author_collection():
    author_validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["first_name", "last_name", "birth_date", "nationality"],
            "properties": {
                "first_name": {
                    "bsonType": "string",
                    "description": "must be a string and is not empty"
                },
                "last_name": {
                    "bsonType": "string",
                    "description": "must be a string and is not empty"
                },
                "birth_date": {
                    "bsonType": "date", 
                    "description": "must be a date and is not empty"
                },
                "nationality": {
                    "bsonType": "string",
                    "description": "must be a string and is not empty"
                }
            },
        },
        "validationAction": "error"
    }
    try:
        production.create_collection("author_collection", validator=author_validator)
    except Exception as e:
        print(f"Failed to create collection: {e}")


def create_data():
    # Add bulk authors
    authors = [
        {"first_name": "Jane", "last_name": "Austen", "birth_date": dt(1975, 10, 18), "nationality": "British"},
        {"first_name": "Charles", "last_name": "Dickens", "birth_date": dt(1912, 7, 15), "nationality": "British"},
        {"first_name": "Mark", "last_name": "Twain", "birth_date": dt(1935, 11, 15), "nationality": "American"},
        {"first_name": "Ernest", "last_name": "Hemingway", "birth_date": dt(1899, 3, 15), "nationality": "American"},
        {"first_name": "F.Scott", "last_name": "Fit", "birth_date": dt(1999, 3, 15), "nationality": "American"}
    ]

    # Add bulk authors
    author_collection = production.author_collection
    author_ids = author_collection.insert_many(authors).inserted_ids

    books = [
        {"title": "Pride and Prejudice", "authors": [author_ids[0], author_ids[1]], "publish_year": dt(1913, 6, 1), "type": "Fiction", "copies": 200},
        {"title": "Great Expectations", "authors": [author_ids[1]], "publish_year": dt(1916, 7, 1), "type": "Fiction", "copies": 250},
        {"title": "To Kill a Mockingbird", "authors": [author_ids[2]], "publish_year": dt(1960, 6, 1), "type": "Fiction", "copies": 300},
        {"title": "1984", "authors": [author_ids[3]], "publish_year": dt(1949, 6, 1), "type": "Non-Fiction", "copies": 50},
        {"title": "But a Mockingbird", "authors": [author_ids[0]], "publish_year": dt(1916, 7, 1), "type": "Fiction", "copies": 24}
    ]

    # add bulk books
    book_collection = production.book_collection
    book_ids = book_collection.insert_many(books).inserted_ids

    # Add book references to authors
    author_collection.update_many({}, {"$set": {"books": list(book_ids)}})

    print("Data added successfully")



if __name__ == "__main__":
    # create_author_collection()