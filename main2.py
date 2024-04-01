from dotenv import load_dotenv, find_dotenv
from datetime import datetime as dt
import os
import pprint
import pymongo
from pymongo import MongoClient
from bson import datetime as bson_dt

# Load environment variables
load_dotenv(find_dotenv())
password = os.environ.get("MONGODB_PWD")
connection_string = f"mongodb+srv://akeemifedayolag:{password}@tutorial-cluster.k5y6y.mongodb.net/?retryWrites=true&w=majority&appName=Tutorial-Cluster&authSource=admin"

# MongoDB client
client = MongoClient(connection_string)
production = client.production

printer = pprint.PrettyPrinter()

def create_book_validation():
    """
    Create the book collection with validation schema.
    """
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
                        "description": "must be an objectId and is not empty"
                    }
                },
                "publish_year": {
                    "bsonType": "date",
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

    try:
        production.create_collection("book_collection")
        production.command("collMod", "book_collection", validator=book_validator)
        print("Book collection created successfully with validation.")
    except pymongo.errors.CollectionInvalid:
        print("Book collection already exists.")
    except Exception as e:
        print(f"Failed to create or modify book collection: {e}")


def create_author_validation():
    """
    Create the author collection with validation schema.
    """
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
                    "description": "must be a date"
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
        production.create_collection("author_collection")
        production.command("collMod", "author_collection", validator=author_validator)
        print("Author collection created successfully with validation.")
    except pymongo.errors.CollectionInvalid:
        print("Author collection already exists.")
    except Exception as e:
        print(f"Failed to create or modify author collection: {e}")


def create_data():
    """
    Insert sample data into author and book collections.
    """
    authors = [
        {"first_name": "Jane", "last_name": "Austen", "birth_date": dt(1975, 10, 18), "nationality": "British"},
        {"first_name": "Charles", "last_name": "Dickens", "birth_date": dt(1912, 7, 15), "nationality": "British"},
        {"first_name": "Mark", "last_name": "Twain", "birth_date": dt(1935, 11, 15), "nationality": "American"},
        {"first_name": "Ernest", "last_name": "Hemingway", "birth_date": dt(1899, 3, 15), "nationality": "American"},
        {"first_name": "F.Scott", "last_name": "Fit", "birth_date": dt(1999, 3, 15), "nationality": "American"},
        {"first_name": "M.Mark", "last_name": "Predict", "birth_date": dt(1991, 7, 15), "nationality": "Nigerian"}
    ]


    try:
        author_collection = production.author_collection
        author_ids = author_collection.insert_many(authors).inserted_ids
        print("Authors inserted successfully.")
    except pymongo.errors.BulkWriteError as e:
        print(f"Bulk write failed: {e.details}")
        return

    books = [
        {"title": "Pride and Prejudice", "authors": [author_ids[0], author_ids[1]], "publish_year": dt(1913, 6, 1), "type": "Fiction", "copies": 200},
        {"title": "Great Expectations", "authors": [author_ids[1]], "publish_year": dt(1916, 7, 1), "type": "Fiction", "copies": 250},
        {"title": "To Kill a Mockingbird", "authors": [author_ids[2]], "publish_year": dt(1960, 6, 1), "type": "Fiction", "copies": 300},
        {"title": "1984", "authors": [author_ids[3]], "publish_year": dt(1949, 6, 1), "type": "Non-Fiction", "copies": 50},
        {"title": "But a Mockingbird", "authors": [author_ids[0]], "publish_year": dt(1916, 7, 1), "type": "Fiction", "copies": 24}
    ]

    try:
        book_collection = production.book_collection
        book_collection.insert_many(books)
        print("Books inserted successfully.")
    except pymongo.errors.BulkWriteError as e:
        print(f"Bulk write failed: {e.details}")


# Retrieve all books that contains letter "a"
books_containing_a = production.book_collection.find({"title": {"$regex": "a{1}"}})
printer.pprint(list(books_containing_a))

# Query and Projection Operators
authors_and_books = production.author_collection.aggregate([
    {
        "$lookup": {
            "from": "book_collection",
            "localField": "_id",
            "foreignField": "authors",
            "as": "books"
        }
    }
])
# printer.pprint(list(authors_and_books))

authors_book_count = production.author_collection.aggregate([
    {
        "$lookup": {
            "from": "book_collection",
            "localField": "_id",
            "foreignField": "authors",
            "as": "books"
        }
    },
    {
        "$addFields": {
            "total_books": {"$size": "$books"}
        }
    },
    {
        "$project": {
            "_id": 0,
            "first_name": 1,
            "last_name": 1,
            "total_books": 1
        }
    }
])

# printer.pprint(list(authors_book_count))

# Return authors with age within 50 - 100
books_with_old_authors = production.book_collection.aggregate([
    {
        "$lookup": {
            "from": "author_collection",
            "localField": "authors",
            "foreignField": "_id",
            "as": "authors"
        }
    },
    {
        "$set": {
            "authors": {
                "$map": {
                    "input": "$authors",
                    "in": {
                        "age": {
                            "$dateDiff": {
                                "startDate": "$$this.birth_date", 
                                "endDate": "$$NOW",
                                "unit": "year"
                            }
                        },
                        "first_name": "$$this.first_name",
                        "last_name": "$$this.last_name",
                    }
                }
            }
        }
    },
    {
        "$match": {
            "$and": [
                {"authors.age": {"$gte": 50}},
                {"authors.age": {"$lte": 100}}
            ]
        }
    },
    {
        "$sort": {
            "age": 1 # ascending order
        }
    }
])

printer.pprint(books_with_old_authors)


#############################################
#          PyMongo Arrow Demo               #
#############################################

import pyarrow
from pymongoarrow.api import Schema
from pymongoarrow.monkey import patch_all
import pymongoarrow as pma
from bson import ObjectId

patch_all()

author = Schema({"_id": ObjectId, "first_name": pyarrow.String(), "last_name": pyarrow.String(),
                "birth_date": dt})

df = production.author_collection.find_pandas_all({}, schema=author)
print(df.head())



# if __name__ == "__main__":
    # create_author_validation()
    # create_book_validation()
    # create_data()
