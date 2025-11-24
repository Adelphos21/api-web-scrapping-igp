import requests
import boto3
import uuid

def lambda_handler(event, context):
    # Año actual o el que desees consultar
    year = 2025

    # Endpoint oficial del IGP (retorna lista completa)
    url = f"https://ultimosismo.igp.gob.pe/api/ultimo-sismo/ajaxb/{year}"

    r = requests.get(url)
    if r.status_code != 200:
        return {
            "statusCode": r.status_code,
            "body": "Error al obtener datos del IGP"
        }

    data = r.json()
    if not isinstance(data, list):
        return {
            "statusCode": 500,
            "body": "Formato inesperado recibido del IGP"
        }

    # Tomar los 10 más recientes
    ultimos_10 = data[:10]

    # Transformar a formato DynamoDB
    items = []
    for s in ultimos_10:
        items.append({
            "id": str(uuid.uuid4()),
            "codigo": s.get("codigo"),
            "fecha_local": s.get("fecha_local"),
            "hora_local": s.get("hora_local"),
            "magnitud": str(s.get("magnitud")),
            "profundidad": s.get("profundidad"),
            "referencia": s.get("referencia"),
            "latitud": s.get("latitud"),
            "longitud": s.get("longitud"),
            "intensidad": s.get("intensidad", ""),
            "pdf_reporte": s.get("reporte_acelerometrico_pdf")
        })

    # Guardar en DynamoDB
    dynamo = boto3.resource("dynamodb")
    table = dynamo.Table("TablaSismosIGP")

    # Eliminar datos anteriores
    scan = table.scan()
    with table.batch_writer() as batch:
        for item in scan.get("Items", []):
            batch.delete_item(Key={"id": item["id"]})

    # Insertar los nuevos 10
    with table.batch_writer() as batch:
        for item in items:
            batch.put_item(Item=item)

    return {
        "statusCode": 200,
        "body": items
    }
