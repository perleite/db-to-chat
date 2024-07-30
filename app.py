import chromadb
import pandas as pd

data = pd.read_csv("history.csv")

columns = ['snapshot_id', 'id_vehicle', 'id_cost_center', 'speed']
passage_data = data[columns]

documents = passage_data.apply(lambda row: row.to_string(), axis=1).tolist()
metadatas = []
ids = []

for idx, row in passage_data.iterrows():
    metadatas.append({
        "vehicle": int(row['id_vehicle']),
        "cost center": int(row['id_cost_center']),
        # Adicione outros campos como necessário
    })
    ids.append(str(row['snapshot_id']))

client = chromadb.Client()
collection = client.create_collection("dbtochat")

collection.add(
    documents=documents,
    metadatas=metadatas,
    ids=ids
)

results = collection.query(
    query_texts=["Which vehicle has a cost center equal to 991? return only the vehicle"],
    n_results=20,
)
print(results)