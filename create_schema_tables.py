from db_connectors import create_connection

# Script para crear esquema y tablas
def create_schema_and_tables():
    try:
        # Conexión inicial utilizando la función de conexión
        connection = create_connection()

        if connection and connection.is_connected():
            cursor = connection.cursor()

            # Crear el esquema si no existe
            cursor.execute("CREATE DATABASE IF NOT EXISTS habi_test;")
            print("Esquema 'habi_test' creado o ya existente.")

            # Seleccionar el esquema
            cursor.execute("USE habi_test;")

            # Crear la tabla de usuarios
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS usuarios (
                id INT AUTO_INCREMENT PRIMARY KEY,
                correo_contacto VARCHAR(255) UNIQUE
            );
            """)

            # Crear la tabla de propiedades
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS propiedades (
                id INT AUTO_INCREMENT PRIMARY KEY,
                estado VARCHAR(100),
                ciudad VARCHAR(100),
                colonia VARCHAR(100),
                calle VARCHAR(255),
                numero_exterior VARCHAR(50),
                tipo_inmueble VARCHAR(50),
                transaccion VARCHAR(50),
                precio DECIMAL(15, 2),
                codigo_proveedor BIGINT,
                correo_contacto VARCHAR(255),
                telefono_contacto BIGINT,
                FOREIGN KEY (correo_contacto) REFERENCES usuarios(correo_contacto)
            );
            """)

            # Crear índices adicionales (verificando si no existen previamente)
            def create_index_if_not_exists(index_name, table_name, columns):
                cursor.execute(f"""
                SELECT COUNT(1) 
                FROM information_schema.STATISTICS 
                WHERE table_schema = 'habi_test' AND table_name = '{table_name}' AND index_name = '{index_name}';
                """)
                if cursor.fetchone()[0] == 0:
                    cursor.execute(f"CREATE INDEX {index_name} ON {table_name} ({columns});")
                    print(f"Índice '{index_name}' creado en la tabla '{table_name}'.")
                else:
                    print(f"Índice '{index_name}' ya existe en la tabla '{table_name}'.")

            create_index_if_not_exists('idx_ubicacion', 'propiedades', 'estado, ciudad, colonia')
            create_index_if_not_exists('idx_correo_contacto', 'propiedades', 'correo_contacto')
            create_index_if_not_exists('idx_tipo_transaccion', 'propiedades', 'tipo_inmueble, transaccion')
            create_index_if_not_exists('idx_precio', 'propiedades', 'precio')

            print("Tablas e índices creados exitosamente en el esquema 'habi_test'.")
            connection.commit()

    except Exception as e:
        print(f"Error al crear esquema o tablas: {e}")

if __name__ == "__main__":
    create_schema_and_tables()