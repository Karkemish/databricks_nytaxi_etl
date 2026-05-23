-- ============================================================================
-- 1. CREACIÓN DE SCHEMAS
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS dev_taxi.bronze 
COMMENT 'Capa Bronze: Almacenamiento histórico optimizado en formato Delta puro.';

CREATE SCHEMA IF NOT EXISTS dev_taxi.silver 
COMMENT 'Capa Silver: Datos limpios, tipados y enriquecidos.';

CREATE SCHEMA IF NOT EXISTS dev_taxi.gold 
COMMENT 'Capa Gold: Agregaciones de negocio para Power BI.';