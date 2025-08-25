-- verification_queries.sql
-- Consultas para verificar que el fix de decimales funciona correctamente

-- 1. Verificar valores específicos de CycleFee conservando decimales originales
SELECT 
    Contrato, 
    Desde, 
    Hasta, 
    Codigo, 
    CycleFee, 
    typeof(CycleFee) AS t_cyclefee
FROM Fichero_Item
WHERE Contrato = 1592237
LIMIT 10;

-- Esperado: CycleFee = -42.37 (NO -4237.0), t_cyclefee = 'real'

-- 2. Verificar rango de valores CycleFee para detectar escalas incorrectas
SELECT 
    MIN(CycleFee) as min_cyclefee,
    MAX(CycleFee) as max_cyclefee,
    AVG(CycleFee) as avg_cyclefee,
    COUNT(DISTINCT CycleFee) as distinct_values
FROM Fichero_Item
WHERE CycleFee IS NOT NULL;

-- Si antes había valores como -4237.0, ahora deberían ser -42.37

-- 3. Verificar casos específicos de formatos US y EU
SELECT 
    CycleFee,
    COUNT(*) as frecuencia,
    typeof(CycleFee) as tipo
FROM Fichero_Item 
WHERE CycleFee IS NOT NULL
GROUP BY CycleFee, typeof(CycleFee)
ORDER BY ABS(CycleFee) DESC
LIMIT 20;

-- Verificar que valores como 42.37, 123.45, etc. aparecen correctamente

-- 4. Buscar valores que podrían indicar error de escala (muy grandes)
SELECT 
    Contrato,
    CycleFee,
    typeof(CycleFee)
FROM Fichero_Item
WHERE ABS(CycleFee) > 10000  -- Valores sospechosamente grandes
ORDER BY ABS(CycleFee) DESC
LIMIT 10;

-- Esto debería devolver pocas o ninguna fila si el fix funciona

-- 5. Verificar distribución de decimales
SELECT 
    CASE 
        WHEN CycleFee = CAST(CycleFee AS INTEGER) THEN 'Entero'
        WHEN ABS(CycleFee - CAST(CycleFee AS INTEGER)) < 1 THEN 'Con decimales'
        ELSE 'Otro'
    END as tipo_valor,
    COUNT(*) as cantidad
FROM Fichero_Item
WHERE CycleFee IS NOT NULL
GROUP BY 
    CASE 
        WHEN CycleFee = CAST(CycleFee AS INTEGER) THEN 'Entero'
        WHEN ABS(CycleFee - CAST(CycleFee AS INTEGER)) < 1 THEN 'Con decimales'
        ELSE 'Otro'
    END;

-- Verificar que hay valores con decimales (no solo enteros grandes)
