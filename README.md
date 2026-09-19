# lavd-airflow-dags

DAGs compartidos para el **único Airflow** del VPS (`data-flight-pipeline` / https://airflow.luisandresvelazquez.com).

## Estructura

```
dags/
  andalucia_drought_monitor/   # ELT diario + mensual (sequía Andalucía)
  data_flight_pipeline/        # Pipeline de vuelos (+ utils)
```

El código de producto (dbt, scripts, spark_jobs, include) **sigue en cada repo**:

- https://github.com/luiveldel/andalucia-drought-monitor
- https://github.com/luiveldel/data-flight-pipeline

## Montaje en el VPS

- `./dags` → `/opt/airflow/dags`
- Drought: montar también `scripts` y `dbt_project` del repo agro en `/opt/airflow/scripts` y `/opt/airflow/dbt_project`
- Flights: seguir montando `include`, `spark_jobs`, `dbt_transform`, `data` desde `data-flight-pipeline`
- Red: Airflow debe poder resolver `agro-postgres` (p. ej. red `docker_agro-network`). **No** unir el servicio Compose `postgres` del stack agro a la red de flights con el alias `postgres` (colisión DNS con la metadatabase de Airflow).

## Variables de entorno (DAGs de sequía)

En el scheduler / workers de Airflow:

| Variable | Ejemplo | Uso |
|----------|---------|-----|
| `POSTGRES_DWH_HOST` | `agro-postgres` | Host del DWH agro |
| `POSTGRES_DWH_PORT` | `5432` | Puerto |
| `POSTGRES_DWH_DB` | `agro_sequia` | Base |
| `POSTGRES_DWH_USER` | `dwh_user` | Rol dbt / ingest |
| `POSTGRES_DWH_PASSWORD` | *(secret)* | Password |
| `POSTGRES_DWH_SCHEMA` | `marts` | Schema por defecto en `profiles.yml` |

El usuario `dwh_user` necesita `USAGE`/`CREATE` y ownership (o grants de escritura) en `raw`, `staging`, `intermediate` y `marts` (ver `docker/grant-dwh-write.sql` en el repo agro).

## Dependencias Python (sequía)

Ver [`requirements-drought.txt`](requirements-drought.txt). Sin ellas fallan:

- `elt_daily_pipeline` → `ModuleNotFoundError: polars`
- `elt_monthly_pipeline` → `ModuleNotFoundError: geopandas` (y luego `geoalchemy2` para PostGIS)

Además en la imagen: `git` (dbt debug) y libs GDAL (`gdal-bin`, `libgdal-dev`, …).

Los bash de dbt exportan `PATH="/opt/airflow/.venv/bin:$PATH"` para encontrar el `dbt` del venv.

## Actualizar

```bash
cd ~/lavd-airflow-dags && git pull
# Airflow recarga DAGs solo; reinicio solo si cambias mounts
```
