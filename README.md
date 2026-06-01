# Data Engineering Lakehouse Pipeline: NYC Taxi ETL

Este repositorio contiene una solución empresarial de extremo a extremo (End-to-End) para la ingesta, transformación y modelado de datos de los viajes de taxi de Nueva York (NYC Taxi Trips). El proyecto implementa una arquitectura **Medallion Lakehouse** utilizando **Databricks**, **Delta Lake** y **Azure Data Lake Storage Gen2 (ADLS)**.

Toda la infraestructura de datos está desacoplada, securizada mediante una **Service Principal (App Registration)** y orquestada mediante código parametrizado por archivos JSON, evitando malas prácticas de configuraciones manuales (*ClickOps*).

---

## 🏗️ Arquitectura del Sistema

El flujo de datos sigue el ciclo completo de procesamiento desde el origen web hasta los Data Marts analíticos listos para Negocio:

1. **Ingesta Local (Landing):** Un componente local en Python descarga los archivos brutos de la web oficial de NYC Taxi y los sube a la zona de `Landing` en ADLS Gen2 utilizando credenciales seguras de una Service Principal.
2. **Capa Bronze (Raw Data):** Carga los datos crudos desde Landing hacia tablas Delta conservando el esquema original e histórico.
3. **Capa Silver (Enriched & Standardized):** Limpia tipos de datos, unifica fuentes (Yellow y Green taxi), estandariza nombres de columnas a *snake_case* y procesa las tablas de dimensiones maestros.
4. **Capa Gold (Business Analytics):** Genera claves subrogadas de negocio (`trip_id`), realiza cruces dimensionales y actualiza de manera **incremental (MERGE)** las tablas de hechos finales y los Data Marts agregados.

---

## 📂 Estructura del Proyecto

```text
DATABRICKS_NYTAXI_ETL/
├── config/                      # Archivos de configuración declarativos por entorno
│   ├── bronze_config.json
│   ├── silver_config.json
│   └── gold_config.json
├── data/                        # Componente local de ingesta y carga
│   ├── adapters/
│   ├── orchestrator/
│   ├── upload_manager.py        # Gestor de subida hacia ADLS Gen2 Landing
│   └── ...
├── pipelines/                   # Scripts orquestadores ejecutados en Databricks Jobs
│   ├── run_bronze.py
│   ├── run_silver.py
│   └── run_gold.py
├── sql/                         # Definiciones de Estructuras de Datos (DDLs)
│   ├── bronze/
│   ├── silver/
│   │   └── tables.sql
│   └── gold/
├── src/                         # Módulos de lógica central (Core Engine)
│   ├── bronze/
│   ├── common/
│   ├── silver/
│   │   ├── base_transformer.py  # Estandarizador de esquemas y nombres
│   │   ├── dimensions.py        # Procesador de dimensiones (Zones/Descriptions)
│   │   ├── int_trips.py         # Cruces intermedios y limpieza final
│   │   └── trips_unioned.py     # Unificación de fuentes Yellow/Green
│   └── gold/
│       ├── dim_vendors.py
│       ├── fct_trips.py         # Hechos incremental con lógica Delta MERGE
│       └── fct_monthly_zone_revenue.py
├── tests/                       # Framework de pruebas automatizadas (CI/CD)
│   ├── integration/             # Pruebas de conectividad local a ADLS Gen2
│   ├── unit/                    # Pruebas de transformaciones unitarias
│   └── conftest.py              # Fixtures globales de testing
├── .env.example                 # Plantilla de variables de entorno seguras
├── .gitignore
├── app_ingestion.log            # Logs de auditoría de ingesta local
└── LICENSE

🔐 Seguridad y Conectividad (Azure ADLS Gen2 Integration)
Para garantizar la seguridad de nivel empresarial, el acceso a los datos no utiliza claves de acceso directas ni tokens maestros. Se configuró un flujo de identidad federada:

Azure App Registration / Service Principal: Se creó un registro de aplicación en Azure Active Directory para actuar como la identidad del pipeline.

Control de Acceso (RBAC): Se le asignó el rol de Storage Blob Data Contributor a la Service Principal de manera granular únicamente en los contenedores correspondientes de ADLS Gen2.

Aislamiento por Capas: Cada contenedor de almacenamiento (landing, bronze, silver, gold) se maneja de manera independiente, permitiendo políticas de acceso desacopladas dentro del clúster de Databricks.

🚀 Componentes del Pipeline
1. Ingesta Local a ADLS (data/upload_manager.py)
Un script modular en Python nativo encargado de conectarse con los endpoints del gobierno de NYC, descargar los datos mensuales, inicializar el cliente de Azure Blob Storage mediante el Service Principal y transferir los flujos a la zona de Landing.

2. Capa Silver Estructurada (src/silver/)
Implementa herencia de lógica mediante SilverBaseTransformer para procesar de forma automatizada las columnas. Las dimensiones maestros de zonas y descripciones se procesan en paralelo. Corrige inconsistencias críticas de nombres de columnas heredadas de los sistemas origen (por ejemplo, alineando esquemas a rate_code_id y pickup_location_id).

3. Capa Gold Incremental (src/gold/fct_trips.py)
La tabla de hechos de viajes implementa una estrategia de carga incremental eficiente. En lugar de reescribir Terabytes de datos históricos, utiliza la potencia de las transacciones ACID de Delta Lake ejecutando una instrucción MERGE INTO controlada por claves subrogadas generadas mediante hashing SHA-256:

delta_target.alias("target") \
    .merge(df_enriched.alias("source"), "target.trip_id = source.trip_id") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

⚙️ Configuración Dinámica (JSON-Driven)
Los pipelines no contienen rutas (hardcoded) hacia catálogos o entornos. Todo el comportamiento se parametriza dinámicamente inyectando variables a strings estructurados en JSON.

{
  "environments": {
    "dev": { "catalog": "dev" }
  },
  "dimensions": [
    {
      "name": "dim_zones",
      "type": "zone",
      "source_table": "{catalog}.bronze.raw_taxi_trip_zone",
      "target_table": "{catalog}.silver.dim_taxi_trip_zone"
    }
  ],
  "trips_pipeline": {
    "source_yellow": "{catalog}.bronze.raw_taxi_trip_yellow",
    "target_table": "{catalog}.silver.int_trips"
  }
}

Al ejecutar el pipeline en Databricks, se pasa el parámetro catalog (ej. dev, qa, prod) a través de la UI del Job o CLI, autocompletando dinámicamente las rutas de Unity Catalog. Los scripts cuentan con tolerancia a entornos interactivos mediante inspección de stack (sys.argv[0]).

🧪 Calidad de Software y Testing (tests/)
El repositorio adopta prácticas de Data Ops incluyendo un suite completo de pruebas automatizadas con pytest:

Pruebas Unitarias (tests/unit/): Verificación de las funciones de transformación matemática, limpieza de strings y formateo de nombres de columnas sin necesidad de levantar servicios cloud.

Pruebas de Integración (tests/integration/): Validan de manera exhaustiva el handshake de seguridad, garantizando que el upload_manager.py local autentique correctamente con la Service Principal y posea los permisos de escritura requeridos en el contenedor destino de Azure.

pip install -r requirements-dev.txt
pytest tests/

🛠️ Instrucciones de Despliegue Rápido
Prerrequisitos Locales
Clonar el repositorio y configurar el entorno virtual:

python -m venv .venv
source .venv/bin/activate  # En Windows: .venv\Scripts\activate

Crear un archivo .env basado en .env.example con las credenciales de Azure:
AZURE_TENANT_ID=tu_tenant_id
AZURE_CLIENT_ID=tu_client_id
AZURE_CLIENT_SECRET=tu_client_secret

Ingesta Local Manual, se maneja un  archivo de checkpoint para guardar el estado de la carga
python3 -m data.upload_manager