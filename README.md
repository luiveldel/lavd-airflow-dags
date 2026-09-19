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

## Actualizar

```bash
cd ~/lavd-airflow-dags && git pull
# Airflow recarga DAGs solo; reinicio solo si cambias mounts
```
