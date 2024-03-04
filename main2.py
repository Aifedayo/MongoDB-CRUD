from dotenv import load_dotenv, find_dotenv
import os
import pprint

from pymongo import MongoClient

load_dotenv(find_dotenv())

password = os.environ.get("MONGODB_PWD")

connection_string = f"mongodb+srv://akeemifedayolag:{password}@tutorial-cluster.k5y6y.mongodb.net/?retryWrites=true&w=majority&appName=Tutorial-Cluster"

client = MongoClient(connection_string)
dbs = client.list_database_names()
production = (client.production)

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
                "bsonType": "integer",
                "minimum": 0,
                "description": "must be a non-negative integer"
            }
        },
    }
    "validationAction": "error"
}

# Create a book collection
try:
    # book collection = production.book_collection
    production.create_collection("book_collection")
except Exception as e:
    print(f"Failed to create collection: {e}")


production.command("collMod", "book_collection", validator=book_validator) # Modify collection