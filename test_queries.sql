-- test_queries.sql
-- Consultas para verificar que los cambios funcionan correctamente

-- 1. Verificar tipos almacenados en la tabla
SELECT 
    Contrato, typeof(Contrato) AS t_contrato,
    Instalacion, typeof(Instalacion) AS t_inst
FROM FicheroChile_test
WHERE Contrato IN (1592237, 0)
LIMIT 20;

-- 2. Buscar fechas fantasma (debería devolver 0 o muy pocas filas)
SELECT COUNT(*) as fechas_fantasma
FROM FicheroChile_test
WHERE Fecha_Inicio_Cuota = '1970-01-01';

-- 3. Verificar distribución de tipos en columnas numéricas
SELECT 
    typeof(Contrato) as tipo_contrato, COUNT(*) as cantidad
FROM FicheroChile_test
GROUP BY typeof(Contrato);

-- 4. Verificar que los valores 0 están como integer, no como fecha
SELECT 
    Contrato, typeof(Contrato), Instalacion, typeof(Instalacion)
FROM FicheroChile_test
WHERE Contrato = 0 OR Instalacion = 0
LIMIT 10;

-- 5. Verificar fechas válidas
SELECT 
    Fecha_Inicio_Cuota, typeof(Fecha_Inicio_Cuota), COUNT(*)
FROM FicheroChile_test
WHERE Fecha_Inicio_Cuota IS NOT NULL 
    AND Fecha_Inicio_Cuota != ''
    AND Fecha_Inicio_Cuota != '1970-01-01'
GROUP BY Fecha_Inicio_Cuota, typeof(Fecha_Inicio_Cuota)
ORDER BY COUNT(*) DESC
LIMIT 10;
