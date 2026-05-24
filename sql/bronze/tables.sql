-- ============================================================================
-- CREACIÓN DE LA TABLA BRONZE: FIDELIDAD ABSOLUTA DEL ORIGEN
-- ============================================================================
CREATE TABLE IF NOT EXISTS dev.bronze.raw_taxi_trip_yellow (
    vendorid INT COMMENT 'Taxi technology provider (1 = Creative Mobile Technologies, 2 = VeriFone Inc.)',
    ratecodeid DOUBLE COMMENT 'Rate code at end of trip (1=Standard, 2=JFK, 3=Newark, 4=Nassau/Westchester, 5=Negotiated, 6=Group)',
    pulocationid INT COMMENT 'TLC Taxi Zone where the meter was engaged',
    dolocationid INT COMMENT 'TLC Taxi Zone where the meter was disengaged',
    tpep_pickup_datetime TIMESTAMP COMMENT 'Date and time when the meter was engaged (TPEP = Yellow Taxis)',
    tpep_dropoff_datetime TIMESTAMP COMMENT 'Date and time when the meter was disengaged',
    store_and_fwd_flag STRING COMMENT 'Flag indicating if trip record was held in vehicle memory (Y/N)',
    passenger_count DOUBLE COMMENT 'Number of passengers in the vehicle (driver-entered value)',
    trip_distance DOUBLE COMMENT 'Trip distance in miles reported by the taximeter',
    fare_amount DOUBLE COMMENT 'Time and distance fare calculated by the meter',
    extra DOUBLE COMMENT 'Miscellaneous extras and surcharges (rush hour, overnight)',
    mta_tax DOUBLE COMMENT ' $0.50 MTA tax automatically triggered based on meter rate',
    tip_amount DOUBLE COMMENT 'Tip amount (credit card tips only, cash tips not included)',
    tolls_amount DOUBLE COMMENT 'Total amount of all tolls paid during the trip',
    improvement_surcharge DOUBLE COMMENT 'Improvement surcharge assessed on hailed trips',
    total_amount DOUBLE COMMENT 'Total amount charged to passengers (does not include cash tips)',
    payment_type DOUBLE COMMENT 'Payment method code (1=Credit card, 2=Cash, 3=No charge, 4=Dispute, 5=Unknown, 6=Voided)',
    -- Columnas de Auditoría Obligatorias (Control del Lago)
    ingested_at TIMESTAMP COMMENT 'Métrica de auditoría: Fecha y hora en la que Spark procesó el registro',
    source_file STRING COMMENT 'Métrica de auditoría: Ruta y nombre del archivo original inmutable en la Landing Zone'
)
USING DELTA
-- LOCATION 'abfss://bronze@databricksadlsproject01.dfs.core.windows.net/dev/'
COMMENT 'Datos históricos en formato Delta replicados desde la Landing para auditoría.';

-- ============================================================================
-- CREACIÓN DE LA TABLA BRONZE: ESPEJO NATIVO DE GREEN TAXI TRIPS
-- ============================================================================
CREATE TABLE IF NOT EXISTS dev.bronze.raw_taxi_trip_green (
    VendorID INT COMMENT 'Taxi technology provider (1 = Creative Mobile Technologies, 2 = VeriFone Inc.)',
    RatecodeID DOUBLE COMMENT 'Rate code at end of trip (1=Standard, 2=JFK, 3=Newark, 4=Nassau/Westchester, 5=Negotiated, 6=Group)',
    PULocationID INT COMMENT 'TLC Taxi Zone where the meter was engaged',
    DOLocationID INT COMMENT 'TLC Taxi Zone where the meter was disengaged',
    lpep_pickup_datetime TIMESTAMP COMMENT 'Date and time when the meter was engaged (LPEP = Green Taxis)',
    lpep_dropoff_datetime TIMESTAMP COMMENT 'Date and time when the meter was disengaged',
    store_and_fwd_flag STRING COMMENT 'Flag indicating if trip record was held in vehicle memory (Y/N)',
    passenger_count DOUBLE COMMENT 'Number of passengers in the vehicle (driver-entered value)',
    trip_distance DOUBLE COMMENT 'Trip distance in miles reported by the taximeter',
    trip_type DOUBLE COMMENT 'Code for trip type (1=Street-hail, 2=Dispatch)',
    fare_amount DOUBLE COMMENT 'Time and distance fare calculated by the meter',
    extra DOUBLE COMMENT 'Miscellaneous extras and surcharges (rush hour, overnight)',
    mta_tax DOUBLE COMMENT '$0.50 MTA tax automatically triggered based on meter rate',
    tip_amount DOUBLE COMMENT 'Tip amount (credit card tips only, cash tips not included)',
    tolls_amount DOUBLE COMMENT 'Total amount of all tolls paid during the trip',
    ehail_fee DOUBLE COMMENT 'E-hail service fee (Specific to Green Taxis)',
    improvement_surcharge DOUBLE COMMENT 'Improvement surcharge assessed on hailed trips',
    total_amount DOUBLE COMMENT 'Total amount charged to passengers (does not include cash tips)',
    payment_type DOUBLE COMMENT 'Payment method code (1=Credit card, 2=Cash, 3=No charge, 4=Dispute, 5=Unknown, 6=Voided)',
    -- Columnas de Auditoría Obligatorias (Control del Lago)
    ingested_at TIMESTAMP COMMENT 'Métrica de auditoría: Fecha y hora en la que Spark procesó el registro',
    source_file STRING COMMENT 'Métrica de auditoría: Ruta y nombre del archivo original inmutable en la Landing Zone'
)
USING DELTA
-- LOCATION 'abfss://bronze@databricksadlsproject01.dfs.core.windows.net/dev/'
COMMENT 'Capa Bronze: Espejo Delta inmutable de los archivos Parquet de Green Taxi depositados en Landing.';

-- ============================================================================
-- CREACIÓN DE LA TABLA BRONZE: MAESTRO DE ZONAS (Origen JSON Anidado)
-- ============================================================================
-- Usamos STRUCT para mapear el objeto nativo "location" que creaste en Python
CREATE TABLE IF NOT EXISTS dev.bronze.raw_taxi_trip_zone (
    locationid INT COMMENT 'ID único de la zona de taxi (TLC Taxi Zone ID)',
    location STRUCT<
        borough: STRING,
        zone: STRING,
        service_zone: STRING
    > COMMENT 'Objeto anidado que contiene los detalles geográficos de la zona',   
    -- Columnas de Auditoría Obligatorias
    ingested_at TIMESTAMP COMMENT 'Fecha y hora de procesamiento en Spark',
    source_file STRING COMMENT 'Ruta del archivo JSON inmutable en Landing'
)
USING DELTA
-- LOCATION 'abfss://bronze@databricksadlsproject01.dfs.core.windows.net/dev/'
COMMENT 'Capa Bronze: Catálogo maestro de zonas TLC mapeado desde el JSON estructurado original.';

-- ============================================================================
-- CREACIÓN DE LA TABLA BRONZE: DESCRIPCIONES DE CÓDIGOS (Origen CSV Plano)
-- ============================================================================
CREATE TABLE IF NOT EXISTS dev.bronze.raw_taxi_trip_description (
    id INT COMMENT 'ID secuencial del registro',
    group_code STRING COMMENT 'Código único del grupo mapeado (ej: 001001)',
    group_description STRING COMMENT 'Nombre de la variable original (RatecodeID, payment_type)',
    code INT COMMENT 'Código numérico nativo presente en las tablas de viajes',
    description STRING COMMENT 'Traducción o descripción textual del código',   
    -- Columnas de Auditoría Obligatorias
    ingested_at TIMESTAMP COMMENT 'Fecha y hora de procesamiento en Spark',
    source_file STRING COMMENT 'Ruta del archivo CSV/Parquet inmutable en Landing'
)
USING DELTA
-- LOCATION 'abfss://bronze@databricksadlsproject01.dfs.core.windows.net/dev/'
COMMENT 'Capa Bronze: Tabla de referencia corporativa para decodificar RatecodeID y payment_type en las capas siguientes.';