import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import pandas as pd
from db_connectors import create_connection
from create_schema_tables import create_schema_and_tables

# Función para escribir registros en MySQL
def write_to_mysql(row):
    connection = create_connection()
    try:
        cursor = connection.cursor()
        # Insertar usuario
        cursor.execute("""
            INSERT INTO usuarios (correo_contacto)
            VALUES (%s)
            ON DUPLICATE KEY UPDATE correo_contacto = VALUES(correo_contacto);
        """, (row['mail_contact'],))
        # Insertar propiedad
        cursor.execute("""
            INSERT INTO propiedades (
                estado, ciudad, colonia, calle, numero_exterior, tipo_inmueble, transaccion,
                precio, codigo_proveedor, correo_contacto, telefono_contacto
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                precio = VALUES(precio),
                telefono_contacto = VALUES(telefono_contacto);
        """, (
            row['state'], row['city'], row['colony'], row['street'], row['external_num'],
            row['type'], row['purpose'], row['price'], row['code'], row['mail_contact'], row['phone_contact']
        ))
        connection.commit()
    except Exception as e:
        print(f"Error escribiendo en MySQL: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# Función para normalizar registros
def normalize_row(row):
    # Manejar valores faltantes y normalizar datos
    row['external_num'] = row.get('external_num', 'N/A')
    row['price'] = float(row.get('price', 0))
    row['mail_contact'] = row.get('mail_contact', '').strip().lower()
    row['phone_contact'] = row.get('phone_contact', 'N/A')
    row['state'] = row.get('state', 'N/A')
    row['city'] = row.get('city', 'N/A')
    row['colony'] = row.get('colony', 'N/A')
    row['street'] = row.get('street', 'N/A')
    row['type'] = row.get('type', 'N/A')
    row['purpose'] = row.get('purpose', 'N/A')
    row['code'] = row.get('code', 'N/A')

    # Validar correo electrónico
    if '@' not in row['mail_contact'] or '.' not in row['mail_contact']:
        row['mail_contact'] = None

    return row

# Ejecutar pipeline
def run_pipeline(input_file):
    connection = create_connection()
    if connection:
        # Crear esquema y tablas si no existen
        create_schema_and_tables()
        connection.close()

    with beam.Pipeline(options=PipelineOptions()) as pipeline:
        (
            pipeline
            | 'Leer archivo XML' >> beam.Create([input_file])
            | 'Cargar en DataFrame' >> beam.FlatMap(lambda file: pd.read_xml(file).to_dict(orient='records'))
            | 'Normalizar registros' >> beam.Map(normalize_row)
            | 'Filtrar correos válidos' >> beam.Filter(lambda row: row['mail_contact'] is not None)
            | 'Escribir en MySQL' >> beam.Map(write_to_mysql)
        )

if __name__ == "__main__":
    run_pipeline('sources/feed.xml')