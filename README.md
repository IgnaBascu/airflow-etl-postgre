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

