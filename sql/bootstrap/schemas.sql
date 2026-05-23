-- ============================================================================
-- 1. CREACIÓN DE SCHEMAS
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS dev.bronze 
MANAGED LOCATION 'abfss://bronze@databricksadlsproject01.dfs.core.windows.net/dev/'
COMMENT 'Capa Bronze: Almacenamiento histórico optimizado en formato Delta puro.';

CREATE SCHEMA IF NOT EXISTS dev.silver 
MANAGED LOCATION 'abfss://silver@databricksadlsproject01.dfs.core.windows.net/dev/'
COMMENT 'Capa Silver: Datos limpios, tipados y enriquecidos.';

CREATE SCHEMA IF NOT EXISTS dev.gold 
MANAGED LOCATION 'abfss://gold@databricksadlsproject01.dfs.core.windows.net/dev/'
COMMENT 'Capa Gold: Agregaciones de negocio para Power BI.';