from dotenv import load_dotenv, find_dotenv
import os
import pprint

from pymongo import MongoClient

load_dotenv(find_dotenv())

password = os.environ.get("MONGODB_PWD")

connection_string = f"mongodb+srv://akeemifedayolag:{password}@tutorial-cluster.k5y6y.mongodb.net/?retryWrites=true&w=majority&appName=Tutorial-Cluster"

client = MongoClient(connection_string)


dbs = client.list_database_names()
test_db = client["test"]
collection = test_db.list_collection_names()

def insert_test_doc():
    collection = test_db.test # collection name in the test db is also called test
    test_document = {
        "name": "Akeem",
        "type": "Test"
    }
    inserted_id = collection.insert_one(test_document).inserted_id
    print(inserted_id) # BSON object ID



# TO create a db that doesn't exist
production = client.production
person_collection = production.person_collection

def create_documents():
    first_names = ["Akeem", "Zainab", "Abdullah", "Maryam"]
    last_names = ["Lagundoye", "Abdulkareem", "Abdulhakeem", "Abass"]
    ages = [12, 11, 8, 10]

    docs = []

    for first_names, last_names, age in zip(first_names, last_names, ages):
        doc = {"first_names": first_names,
               "last_names": last_names, 
               "age": age}
        docs.append(doc)

    person_collection.insert_many(docs)

def insert_into_documents(collection, doc):
    collection.insert_one(doc)
    return collection.id

printer = pprint.PrettyPrinter()

def find_all_people():
    people = person_collection.find()

    for person in people:
        pprint.pprint(person)


def find_specific_key(person):
    person = person_collection.find_one({"first_name": person})
    printer.pprint(person)








if __name__ == "__main__":
    # insert_test_doc()
    # create_documents()
    insert_into_documents(collection=person_collection, doc={"first_name": "Abdulmalik", "last_name": "Abdulazeez", "age": 2})
    find_all_people()
    find_specific_key("Abdulmalik")
    