# finance-analytics

## Transacciones de Insiders y precios de activos - Data Pipeline Project

Este proyecto implementa un pipeline de datos en **Databricks**, diseñado para procesar y modelar datos relacionados con transacciones de insiders y precios de activos financieros. La solución sigue una arquitectura **Medallion** y utiliza **dbt** para la modelación en capas, finalizando con la visualización de los datos en un dashboard en **Power BI**.

![Dashboard de transacciones y cotizaciones de activos](https://imgur.com/MwP0ve5)

---

## 🛠️ Tecnologías

- **Databricks**
- **Python**
- **DBT**
- **Power BI**
- **Delta Lake**

---

## 📊 Descripción del Flujo

### A. 'medallion_layer' (Carga días laborales)

![Orquestación de la carga diaria](https://imgur.com/Hzx4PRz)

#### 1. 'trg_holidays.py' (Trigger inicial)
Verifica si el día actual es festivo:
- Si **es festivo**, el pipeline no se ejecuta.
- Si **no es festivo**, se activan los notebooks de ingesta.

#### 2. 'sources/' (Ingesta de datos crudos)
Contiene notebooks de Databricks que conectan con diversas fuentes para obtener:
- Transacciones de insiders
- Precios históricos de activos
- Días festivos
- Códigos SIC
- Listas de índices
- Activos registrados en la SEC

Los datos se almacenan en **db_landing** (zona cruda).

#### 3. 'dbt/' (Modelado con dbt)

##### 3.1 'staging/' (db_bronze)
- Limpieza básica y normalización de datos crudos.
- Preparación para análisis.

##### 3.2 'intermediate/' (db_silver)
- Tablas intermedias con lógica aplicada.
- Uniones, deduplicaciones, y enriquecimiento de datos.

##### 3.3 'marts/' (db_gold)
- Tablas finales para análisis y explotación en Power BI:
  - Transacciones de insiders
  - Precios de seguridad
  - Componentes de índices

### B. 'time_layer' (Carga anual)

![Orquestación de la capa tiempo anual](https://imgur.com/knJjoPy)

Este flujo se ejecuta **una vez al año**:
- Inserta los **días festivos del nuevo año**.
- Registra todos los días del año para facilitar análisis temporales.

### 5. Visualización
- Los datos modelados se consumen en un dashboard de **Power BI**, ofreciendo visualizaciones sobre la actividad de insiders y el comportamiento del mercado.

---

## 📁 Estructura del Repositorio

## 📁 Estructura del Repositorio

```text
.
- dbt/
  - dbt_project.yml
  - macros/
    - tests/
      - unique_combination.sql
  - models/
    - intermediate/
      - int_silver_index_components.sql
      - schema.yml
    - marts/
      - dim_gold_index_components.sql
      - dim_gold_industries.sql
      - dim_gold_securities.sql
      - dim_gold_time.sql
      - fct_gold_insiders_trx.sql
      - fct_gold_security_prices.sql
      - schema.yml
    - staging/
      - schema.yml
      - stg_bronze_djia_index.sql
      - stg_bronze_holidays.sql
      - stg_bronze_index_keys.sql
      - stg_bronze_insiders_trx.sql
      - stg_bronze_nasdaq_index.sql
      - stg_bronze_russell_index.sql
      - stg_bronze_sic_codes.sql
      - stg_bronze_sp_index.sql
      - stg_bronze_stock_prices.sql
      - stg_bronze_time.sql
  - tests/
- jobs/
  - medallion_layer.py
  - time_layer.py
- sources/
  - src_ingest_raw_holidays.ipynb
  - src_ingest_raw_index_keys.ipynb
  - src_ingest_raw_index_lists.ipynb
  - src_ingest_raw_insiders_trx.ipynb
  - src_ingest_raw_prices_01.ipynb
  - src_ingest_raw_prices_02.ipynb
  - src_ingest_raw_sic_codes.ipynb
  - src_ingest_raw_time.ipynb
- utils/
  - trg_holidays.py
- packages.yml
- package-lock.yml
- README.md
