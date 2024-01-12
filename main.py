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
production = client.production # Name of the production database
person_collection = production.person_collection # Name of the collection in the production database

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


def count_all_people():
    count = person_collection.count_documents({})
    print(f"Total people: {count}")


def get_person_by_id(person_id):
    from bson.objectid import ObjectId

    _id = ObjectId(person_id)
    person = person_collection.find_one({"_id": _id})
    printer.pprint(person)


def get_age_range(min_age, max_age):
    """
    SELECT * FROM person WHERE age BETWEEN min_age AND max_age
    """
    query = {"$and": [
            {"age": {"$gte": min_age}},
            {"age": {"$lte": max_age}}
        ]}
    people = person_collection.find(query).sort("age")

    for person in people:
        printer.pprint(person)


def project_columns():
    columns = {"_id": False, "first_name": True, "last_name": True}
    people = person_collection.find({}, columns)
    for person in people:
        printer.pprint(person)


def update_person_by_id(person_id):
    from bson.objectid import ObjectId

    _id = ObjectId(person_id)
    all_updates = {
        "$set": {"middle_name": "Aderinmola"},
        "$inc": {"age": 1},
        "$rename": {"first_name": "first", "last_name": "last"}
    }
    person_collection.update_one({"_id": _id}, all_updates)
    person_collection.update_one({"_id": _id}, {"$unset": {"middle_name": ""}})


def replace_one_person_doc(person_id, new_doc):
    from bson.objectid import ObjectId

    _id = ObjectId(person_id)
    person_collection.replace_one({"_id": _id}, new_doc)


def delete_doc_by_person_id(person_id):
    from bson.objectid import ObjectId

    _id = ObjectId(person_id)
    person_collection.delete_one({"_id": _id})




if __name__ == "__main__":
    # insert_test_doc()
    # create_documents()
    # insert_into_documents(collection=person_collection, doc={"first_name": "Abdulmalik", "last_name": "Abdulazeez", "age": 2})
    # find_all_people()
    # find_specific_key("Abdulmalik")
    # count_all_people()
    # get_person_by_id("66f939d8f873e416256b34cc")
    # get_age_range(10, 15)
    # project_columns()
    # update_person_by_id("66f939d8f873e416256b34cc")
    replace_one_person_doc("66f94af6bbe3cd7e6c06b493", {"first_name": "Abdulmalik", "last_name": "Lagundoye", "age": 2})
