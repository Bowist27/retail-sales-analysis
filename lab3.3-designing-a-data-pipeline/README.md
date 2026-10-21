# Lab 3.3: Pipeline ETL con Apache Airflow

Pipeline ETL para la empresa ficticia **ShopSmart**, orquestado con Apache Airflow 2.10 y SQLite.

## Flujo del DAG `etl_pipeline`

```
extract_data  →  transform_data  →  load_data  →  validate_data
```

| Tarea | Qué hace |
|---|---|
| `extract_data` | Crea un CSV con 4 productos de ejemplo, uno de ellos sin nombre. |
| `transform_data` | Rellena el nombre vacío con `Unknown Product` y agrega `discounted_price` (precio con 10 % de descuento). |
| `load_data` | Carga los datos en la tabla `products` de `demo.db` (SQLite). |
| `validate_data` | Cuenta las filas de `products` mediante la conexión de Airflow `my_sqlite_conn`. |

## Cómo ejecutarlo

1. Copiar `dags/etl_pipeline.py` a la carpeta de DAGs de Airflow.
2. Crear la base vacía `/app/data/demo.db`.
3. Crear la conexión:
   ```bash
   airflow connections add my_sqlite_conn --conn-type sqlite --conn-host /app/data/demo.db
   ```
4. Activar el DAG en la interfaz de Airflow y lanzarlo.

## Resultado

La ejecución termina con las cuatro tareas en estado `success`. La tabla `products` queda así:

| product_id | name | price | discounted_price |
|---|---|---|---|
| 1 | Product A | 10 | 9.0 |
| 2 | Product B | 20 | 18.0 |
| 3 | Unknown Product | 15 | 13.5 |
| 4 | Product D | 25 | 22.5 |
