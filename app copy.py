import pandas as pd
import chromadb

# Caminho para o arquivo CSV
csv_file_path = 'history.csv'

# Carregar o CSV usando pandas
try:
    df = pd.read_csv(csv_file_path)
    print(f"Colunas do CSV: {df.columns.tolist()}")
except pd.errors.ParserError as e:
    print(f"Erro ao carregar o CSV: {e}")

# set datatype
if not df.empty:
    # Verificação e conversão das colunas
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
    if 'prev_date' in df.columns:
        df['prev_date'] = pd.to_datetime(df['prev_date'], errors='coerce')
    if 'next_date' in df.columns:
        df['next_date'] = pd.to_datetime(df['next_date'], errors='coerce')
    if 'activity_start_date' in df.columns:
        df['activity_start_date'] = pd.to_datetime(df['activity_start_date'], errors='coerce')
    if 'gps_sync' in df.columns:
        df['gps_sync'] = df['gps_sync'].astype(bool, errors='ignore')

    # Configurar a conexão com o ChromaDB
    client = chromadb.Client()

    # Nome do banco de dados e coleção
    database_name = "dbtogpt"
    collection_name = "history"

    # Criar uma tabela (ou coleção) no banco de dados
    try:
        collection = client.create_collection(name=collection_name)
    except Exception as e:
        print(f"Error creating collection: {e}")
        collection = client.get_collection(name=collection_name)

    # Inserir dados no ChromaDB
    for _, row in df.iterrows():
        document = {
            "id_company": row['id_company'],
            "date": row['date'].strftime('%Y-%m-%d %H:%M:%S'),
            "id_vehicle": row['id_vehicle'],
            "prev_date": row['prev_date'].strftime('%Y-%m-%d %H:%M:%S'),
            "next_date": row['next_date'].strftime('%Y-%m-%d %H:%M:%S'),
            "duration": row['duration'],
            "activity_start_date": row['activity_start_date'].strftime('%Y-%m-%d %H:%M:%S'),
            "id_onboard_computer": row['id_onboard_computer'],
            "id_devixe": row['id_devixe'],
            "id_activity_group": row['id_activity_group'],
            "id_activity": row['id_activity'],
            "id_cost_center": row['id_cost_center'],
            "id_role": row['id_role'],
            "id_vehicle_operator": row['id_vehicle_operator'],
            "id_structure_cr": row['id_structure_cr'],
            "id_workfront": row['id_workfront'],
            "id_eletronic_fence": row['id_eletronic_fence'],
            "id_implement": row['id_implement'],
            "id_implement_type": row['id_implement_type'],
            "id_workflow": row['id_workflow'],
            "post_process_type": row['post_process_type'],
            "id_package_rule": row['id_package_rule'],
            "id_package_rule_structure": row['id_package_rule_structure'],
            "id_service_order": row['id_service_order'],
            "service_order_code": row['service_order_code'],
            "id_season": row['id_season'],
            "gps_position": row['gps_position'],
            "altitude": row['altitude'],
            "direction": row['direction'],
            "gps_satellites_quantity": row['gps_satellites_quantity'],
            "gps_sync": row['gps_sync'],
            "signal_rtk": row['signal_rtk'],
            "signal_strength_3g": row['signal_strength_3g'],
            "signal_strength_wifi": row['signal_strength_wifi'],
            "snapshot_id": row['snapshot_id'],
            "speed": row['speed'],
            "productive_hectar": row['productive_hectar'],
            "area_sensor": row['area_sensor'],
            "engine_on_sensor": row['engine_on_sensor'],
            "torque_sensor": row['torque_sensor'],
            "speed_avg_sensor": row['speed_avg_sensor']
        }
        collection.upsert(document)

    # Consultar dados
    results = collection.query(filter={"gps_sync": False})

    # Exibir resultados
    for result in results:
        print(result)

    # Fechar a conexão
    db.close()
