-- ============================================================================
-- CAPA SILVER: MAESTRO DE ZONAS APLANADO (Flattened)
-- ============================================================================
CREATE TABLE IF NOT EXISTS dev.silver.dim_taxi_trip_zone (
    location_id INT COMMENT 'ID único de la zona de taxi (TLC Taxi Zone ID)',
    borough STRING COMMENT 'Distrito de Nueva York al que pertenece la zona',
    zone STRING COMMENT 'Nombre de la zona específica de recogida/destino',
    service_zone STRING COMMENT 'Zona de servicio interna de la TLC',    
    -- Control de Auditoría Silver
    processed_at TIMESTAMP COMMENT 'Fecha y hora en la que se aplanó el registro en Silver'
)
USING DELTA
COMMENT 'Capa Silver: Catálogo maestro de zonas TLC estructurado y aplanado para dimensiones.';

-- ============================================================================
-- CAPA SILVER: UNIÓN NORMALIZADA DE VIAJES (Intermediate)
-- ============================================================================
CREATE TABLE IF NOT EXISTS dev.silver.int_trips_unioned (
    -- Identificadores estandarizados
    vendor_id INT COMMENT 'Taxi technology provider ID (1=CMT, 2=VeriFone)',
    rate_code_id INT COMMENT 'Rate code at end of trip (1=Standard, 2=JFK, 3=Newark, etc.)',
    pickup_location_id INT COMMENT 'TLC Taxi Zone where trip started',
    dropoff_location_id INT COMMENT 'TLC Taxi Zone where trip ended',
    -- Fechas y Horas normalizadas
    pickup_datetime TIMESTAMP COMMENT 'Timestamp when meter was engaged',
    dropoff_datetime TIMESTAMP COMMENT 'Timestamp when meter was disengaged',
    -- Detalles del viaje
    store_and_fwd_flag STRING COMMENT 'Trip record stored in vehicle memory (Y/N)',
    passenger_count INT COMMENT 'Number of passengers in the vehicle',
    trip_distance DECIMAL(10, 2) COMMENT 'Trip distance in miles',
    trip_type INT COMMENT 'Trip type (1=Street-hail, 2=Dispatch)',
    -- Desglose Financiero Estandarizado
    fare_amount DECIMAL(10, 2) COMMENT 'Time and distance fare',
    extra DECIMAL(10, 2) COMMENT 'Miscellaneous extras and surcharges',
    mta_tax DECIMAL(10, 2) COMMENT 'MTA tax',
    tip_amount DECIMAL(10, 2) COMMENT 'Tip amount (credit card only)',
    tolls_amount DECIMAL(10, 2) COMMENT 'Total tolls paid',
    ehail_fee DECIMAL(10, 2) COMMENT 'E-hail service fee',
    improvement_surcharge DECIMAL(10, 2) COMMENT 'Improvement surcharge',
    total_amount DECIMAL(10, 2) COMMENT 'Total amount charged to passenger',
    payment_type INT COMMENT 'Payment method code',    
    -- Origen corporativo
    service_type STRING COMMENT 'Type of taxi service (Green or Yellow)'
)
USING DELTA
CLUSTER BY (pickup_datetime, service_type) -- ⚡ Optimización Liquid Clustering nativa para acelerar las consultas de Marts
COMMENT 'Capa Silver: Unión intermedia indexada de viajes verdes y amarillos con esquema homologado.';

-- ============================================================================
-- CAPA SILVER: VIAJES LIMPIOS, DEDUPLICADOS Y ENRIQUECIDOS
-- ============================================================================
CREATE TABLE IF NOT EXISTS dev.silver.int_trips (
    -- Clave Primaria Analítica
    trip_id STRING COMMENT 'Unique trip identifier generated via MD5 hash of natural keys',    
    vendor_id INT COMMENT 'Taxi technology provider ID',
    service_type STRING COMMENT 'Type of taxi service (Green or Yellow)',
    rate_code_id INT COMMENT 'Rate code at end of trip',
    pickup_location_id INT COMMENT 'TLC Taxi Zone where trip started',
    dropoff_location_id INT COMMENT 'TLC Taxi Zone where trip ended',    
    pickup_datetime TIMESTAMP COMMENT 'Timestamp when meter was engaged',
    dropoff_datetime TIMESTAMP COMMENT 'Timestamp when meter was disengaged',    
    store_and_fwd_flag STRING COMMENT 'Trip record stored in vehicle memory (Y/N)',
    passenger_count INT COMMENT 'Number of passengers in the vehicle',
    trip_distance DECIMAL(10, 2) COMMENT 'Trip distance in miles',
    trip_type INT COMMENT 'Trip type (1=Street-hail, 2=Dispatch)',    
    -- Datos Financieros
    fare_amount DECIMAL(10, 2) COMMENT 'Time and distance fare',
    extra DECIMAL(10, 2) COMMENT 'Miscellaneous extras and surcharges',
    mta_tax DECIMAL(10, 2) COMMENT 'MTA tax',
    tip_amount DECIMAL(10, 2) COMMENT 'Tip amount (credit card only)',
    tolls_amount DECIMAL(10, 2) COMMENT 'Total tolls paid',
    ehail_fee DECIMAL(10, 2) COMMENT 'E-hail service fee',
    improvement_surcharge DECIMAL(10, 2) COMMENT 'Improvement surcharge',
    total_amount DECIMAL(10, 2) COMMENT 'Total amount charged to passenger',    
    -- Métricas enriquecidas mediante JOINs en Silver
    payment_type INT COMMENT 'Payment method code',
    payment_type_description STRING COMMENT 'Human-readable payment method description (Enriched via Lookup)'
)
USING DELTA
CLUSTER BY (pickup_datetime, service_type, pickup_location_id) -- ⚡ Triple cluster para acelerar los queries de Marts
COMMENT 'Capa Silver: Modelo integrado de viajes limpio y listo para consumo de modelos dimensionales (Marts).';

-- ============================================================================
-- CAPA SILVER: MAESTRO DE DESCRIPCIONES DE CÓDIGOS (Dimension Table)
-- ============================================================================
CREATE TABLE IF NOT EXISTS dev.silver.dim_taxi_trip_description (
    id INT COMMENT 'ID secuencial del registro',
    group_code STRING COMMENT 'Código único del grupo (ej: 001001)',
    group_variable_name STRING COMMENT 'Nombre estandarizado de la variable origen (rate_code_id, payment_type)',
    code INT COMMENT 'Código numérico que coincide con las tablas de hechos',
    code_description STRING COMMENT 'Traducción o descripción textual legible para el negocio',    
    -- Control de Auditoría Silver
    processed_at TIMESTAMP COMMENT 'Fecha y hora en la que se procesó y estandarizó en Silver'
)
USING DELTA
COMMENT 'Capa Silver: Tabla de referencia limpia para decodificar códigos de tarifas y métodos de pago.';