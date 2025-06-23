# Proyecto ETL con Apache Airflow + PostgreSQL

Este proyecto implementa un pipeline ETL usando [Apache Airflow](https://airflow.apache.org/) para orquestar la extracción, transformación y carga de datos desde una API mock (Mockaroo) hacia una base de datos PostgreSQL.

## ⚙️ Tecnologías
- Apache Airflow 3.0
- PostgreSQL
- Python (pandas, requests)

## 📂 Estructura del DAG
1. **Extracción**: Obtiene datos desde la API de Mockaroo usando Python y Bash en formato csv.
2. **Transformación**: Limpia, agrupa, modifica fechas (YYYYMMDD) y renombra columnas con pandas.
3. **Carga**: Inserta los datos en PostgreSQL utilizando `bulk_load`.

## ▶️ Cómo ejecutar

1. Clona este repositorio.
2. Crea un entorno virtual.
3. Instala las dependencias.
4. Exporta la variable `AIRFLOW_HOME`.
5. Ejecuta Airflow (`airflow standalone` o `airflow scheduler` + `airflow webserver`)
6. Configura la conexión a PostgreSQL en la UI de Airflow.

## :books: Screenshots
- Datos ingresados a la tabla sales_db
![Postgre](https://github.com/user-attachments/assets/fc6d4989-0258-4716-8159-9578f75f023a)

- Airflow ejecutado correctamente

![Airflow1](https://github.com/user-attachments/assets/fbe1c9b6-e739-4e2d-9442-8b47cfbfe8a5)

![Airflow2](https://github.com/user-attachments/assets/98334c91-2c40-4096-ac38-87ee35823428)




