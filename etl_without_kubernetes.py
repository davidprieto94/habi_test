# Importar bibliotecas necesarias
import pandas as pd
import re
from db_connectors import create_connection
from create_schema_tables import create_schema_and_tables

# Ruta del archivo XML
XML_FILE_PATH = 'sources/feed.xml'

# Función para validar y normalizar correos electrónicos
def normalize_email(email):
    if email is None:
        return None
    email = email.strip().lower()  # Eliminar espacios y convertir a minúsculas
    email_regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if re.match(email_regex, email):
        return email
    return None

# Función para procesar y cargar datos
def process_and_load_data(xml_file, connection):
    try:
        # Leer el archivo XML usando pandas
        data = pd.read_xml(xml_file)

        # Transformaciones adecuadas
        data['external_num'] = data['external_num'].fillna('N/A')
        data['price'] = data['price'].fillna(0).astype(float)
        data['mail_contact'] = data['mail_contact'].apply(normalize_email)
        data['phone_contact'] = data['phone_contact'].fillna('N/A')
        data['state'] = data['state'].fillna('N/A')
        data['city'] = data['city'].fillna('N/A')
        data['colony'] = data['colony'].fillna('N/A')
        data['street'] = data['street'].fillna('N/A')
        data['type'] = data['type'].fillna('N/A')
        data['purpose'] = data['purpose'].fillna('N/A')
        data['code'] = data['code'].fillna('N/A')

        # Eliminar filas con correos no válidos
        data = data.dropna(subset=['mail_contact'])

        cursor = connection.cursor()

        # Cargar usuarios primero
        usuarios = data[['mail_contact']].drop_duplicates()
        for _, row in usuarios.iterrows():
            correo_contacto = row['mail_contact']
            cursor.execute("""
            INSERT INTO usuarios (correo_contacto)
            VALUES (%s)
            ON DUPLICATE KEY UPDATE correo_contacto = VALUES(correo_contacto);
            """, (correo_contacto,))

        # Cargar propiedades después
        for _, row in data.iterrows():
            correo_contacto = row['mail_contact']
            telefono_contacto = row['phone_contact']
            estado = row['state']
            ciudad = row['city']
            colonia = row['colony']
            calle = row['street']
            numero_exterior = row['external_num']
            tipo_inmueble = row['type']
            transaccion = row['purpose']
            precio = row['price']
            codigo_proveedor = row['code']

            # Verificar si el registro ya existe evaluando todos los campos
            cursor.execute("""
            SELECT COUNT(*) FROM propiedades
            WHERE estado = %s AND ciudad = %s AND colonia = %s AND calle = %s AND
                  numero_exterior = %s AND tipo_inmueble = %s AND transaccion = %s AND
                  precio = %s AND codigo_proveedor = %s AND correo_contacto = %s AND
                  telefono_contacto = %s;
            """, (estado, ciudad, colonia, calle, numero_exterior, tipo_inmueble, transaccion,
                  precio, codigo_proveedor, correo_contacto, telefono_contacto))

            exists = cursor.fetchone()[0] > 0

            if not exists:
                # Insertar o actualizar en propiedades
                cursor.execute("""
                INSERT INTO propiedades (
                    estado, ciudad, colonia, calle, numero_exterior, tipo_inmueble, transaccion,
                    precio, codigo_proveedor, correo_contacto, telefono_contacto
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """, (estado, ciudad, colonia, calle, numero_exterior, tipo_inmueble, transaccion,
                      precio, codigo_proveedor, correo_contacto, telefono_contacto))

        connection.commit()
        print("Datos cargados exitosamente")
    except Exception as e:
        print(f"Error al procesar datos: {e}")

# Script principal
def main():
    connection = create_connection()
    if connection:
        create_schema_and_tables()
        process_and_load_data(XML_FILE_PATH, connection)
        connection.close()

if __name__ == "__main__":
    main()
