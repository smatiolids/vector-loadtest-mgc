import os
from dotenv import load_dotenv
from conn import getCQLSession
from cassandra.cqlengine.query import BatchStatement
from cassandra.query import BatchType
import csv
import time

load_dotenv()

session = getCQLSession(os.environ["MODE"])
table = "default_keyspace.product_catalog_emb"

cmd_insert = f"""
INSERT INTO {table} (id, title, emb)
VALUES (:id, :title, :emb)
"""

prepared_stmt_insert = session.prepare(cmd_insert)


def read_csv(file_path):
    data = []
    with open(file_path, 'r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            data.append(row)
    return data


def save_data(data, batch_size=20):
    batch = BatchStatement(batch_type=BatchType.UNLOGGED)
    count = 0
    for row in data:
        batch.add(prepared_stmt_insert, {"id": row['asin'], "title": row['title'], "emb": [
                  float(x) for x in row['emb'].split(',')]})
        count += 1
        if count % batch_size == 0:
            rs = session.execute(batch)
            batch.clear()

        if count % 1000 == 0:
            print(f"""{count} records inserted.""")

    rs = session.execute(batch)
    batch.clear()

    return count


def main():
    # directory = '../data/emb_amz_uk'
    directory = '../data/emb_amz_uk'
    files = os.listdir(directory)
    count = 0

    # Iterate over files in directory
    for filename in files:
        count += 1
        if filename.endswith('.csv'):
            file_path = os.path.join(directory, filename)
            print(f"Loading {filename} ({count}/{len(files)} )")
            data = read_csv(file_path)
            start = time.time()
            save_data(data, 200)
            end = time.time()
            print(f"Time to load: {end - start}")


if __name__ == "__main__":
    main()
