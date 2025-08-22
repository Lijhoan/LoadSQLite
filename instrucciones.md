# Objetivo
Corregir el flujo de procesamiento para **evitar falsos positivos de fechas (p. ej., `0 → 1970‑01‑01`)** y **aplicar el esquema final a los valores antes de insertarlos**. Mantener lo ya funcional; **no duplicar pasos** ni introducir cambios innecesarios.

---

## Resumen del problema (diagnóstico)
1. **Detección de fechas demasiado laxa**: se intenta `pd.to_datetime` sobre columnas numéricas y eso promueve enteros (p. ej. `0`) a epoch (`1970-01-01`).
2. **Aplicación de esquema sólo al DDL**: tras corregir tipos en UI/ventana, **se crea la tabla con esos tipos**, pero **se insertan valores provenientes de `df_final`** (ya normalizado con el detector anterior) **sin reconvertir** desde el `df_original` conforme al esquema final.

---

## Resultado esperado
- Columnas puramente numéricas **no** deben considerarse fecha **a menos que el nombre/patrón lo sugiera**.
- Tras decidir el esquema (automático o corregido por el usuario), **reconstruir el DataFrame desde `df_original` aplicando conversiones por tipo** y **sólo entonces insertar**.
- Desaparecen valores fantasma `'1970-01-01'` nacidos de `0` en columnas que no son fecha.

---

## Alcance (sin cambios colaterales)
- **No** modificar nombres de funciones públicas, firmas ni rutas ya usadas por otros módulos.
- **No** duplicar normalizaciones existentes; reemplazar puntos específicos.
- **Sí**: endurecer `detectar_columnas_fecha` y **agregar** `aplicar_esquema_a_df`, usándola **en lugar** de `df_final` para inserts.

---

## Cambios a implementar (paso a paso)

### 1) Endurecer la detección de fechas
**Archivo**: `processor.py` (o donde resida la lógica actual de detección)

**Acción**: sustituir la implementación de `detectar_columnas_fecha` por una que:
- Filtre columnas **numéricas** salvo que el **nombre** sugiera fecha.
- Evalúe una **muestra de strings** y, si no hay indicadores `-` o `/`, **sólo** la trate como fecha si el nombre lo sugiere.

```python
# processor.py
import re
import pandas as pd

def detectar_columnas_fecha(self, df):
    columnas_fecha = []
    for col in df.columns:
        col_lower = str(col).lower()
        es_fecha_por_nombre = any(re.search(p, col_lower) for p in self.fecha_patterns)

        # 🔒 Evitar tratar números como fecha salvo por nombre/patrón
        if df[col].dtype.kind in 'iuf' and not es_fecha_por_nombre:
            continue

        # Trabajar con strings; usar indicadores visuales de fecha
        muestra = df[col].dropna().astype(str).head(20)
        candidatos = muestra[muestra.str.contains(r'[-/]', regex=True)]

        if len(candidatos) == 0 and not es_fecha_por_nombre:
            continue

        try:
            pd.to_datetime(candidatos if len(candidatos) else muestra, errors='raise')
            columnas_fecha.append(col)
        except Exception:
            continue
    return columnas_fecha
```

> **Nota**: `self.fecha_patterns` debe incluir patrones tipo: `r"\bfecha\b|\bdate\b|\bfec\b|\bfech[a|.]|_dt|_date|_fecha"` (ajustar según el proyecto, sin romper compatibilidad).

---

### 2) Aplicar el **esquema final** a los **valores** antes de insertar
**Archivo**: `processor.py`

**Acción**: crear utilitario `aplicar_esquema_a_df(df_src, esquema)` y usarlo **en ambos flujos**: con ventana de corrección y modo automático.

```python
# processor.py
import pandas as pd
import re

def aplicar_esquema_a_df(self, df_src, esquema):
    df2 = df_src.copy()

    def to_int_series(s):
        # Quita separadores de miles y deja dígitos/signo
        return pd.to_numeric(
            s.astype(str).str.replace(r'[^\d\-]+', '', regex=True),
            errors='coerce'
        ).astype('Int64')

    def to_real_series(s):
        # Normaliza coma decimal -> punto, quita miles (.)
        txt = s.astype(str).str.replace('.', '', regex=False)
        txt = txt.str.replace(',', '.', regex=False)
        return pd.to_numeric(txt, errors='coerce')

    def to_bool_series(s):
        m = s.astype(str).str.strip().str.lower()
        return m.map({
            '1':1,'true':1,'t':1,'yes':1,'si':1,'sí':1,'y':1,
            '0':0,'false':0,'f':0,'no':0,'n':0
        }).astype('Int64')

    for col_limpio, info in esquema.items():
        col = info.get('columna_original', col_limpio)
        tipo = str(info['tipo']).upper()

        if col not in df2.columns:
            continue

        if tipo == 'INTEGER':
            df2[col] = to_int_series(df2[col])
        elif tipo in ('REAL', 'NUMERIC', 'DECIMAL', 'FLOAT', 'DOUBLE'):
            df2[col] = to_real_series(df2[col])
        elif tipo in ('DATE', 'DATETIME') or info.get('es_fecha'):
            dt = pd.to_datetime(df2[col], errors='coerce')
            if tipo == 'DATETIME':
                df2[col] = dt.dt.strftime('%Y-%m-%d %H:%M:%S')
            else:
                df2[col] = dt.dt.strftime('%Y-%m-%d')
            df2[col] = df2[col].replace('NaT', None)
        elif tipo == 'BOOLEAN':
            df2[col] = to_bool_series(df2[col])
        else:
            # TEXT/BLOB: aseguramos None donde aplique
            df2[col] = df2[col].where(pd.notna(df2[col]), None)

    # Normalizar nulos con helper existente
    return self.normalizar_nulos(df2)
```

---

### 3) Integración en el flujo **con ventana de corrección**
**Archivo**: `processor.py` (método `_continuar_despues_correccion`)

**Reemplazar** el uso de `df_final` para inserts por `df_convertido` reconstruido desde `df_original` y el **esquema corregido**.

```diff
 def _continuar_despues_correccion(self, df_original, esquema_personalizado, ...):
-    # Se usaba df_final (ya normalizado antes de corregir) para insertar
-    df_para_insert = df_final
+    # ✅ Reconstruir valores según el esquema elegido
+    df_para_insert = self.aplicar_esquema_a_df(df_original, esquema_personalizado)

     # Mantener el resto del flujo (renombrar columnas a nombres limpios, to_sql, etc.)
```

Asegurarse de que el **renombrado de columnas** utilice las claves `col_limpio` del `esquema_personalizado` y mapee desde `columna_original`.

---

### 4) Integración en el flujo **automático (sin ventana)**
**Archivo**: `processor.py` (método `procesar_archivo` o equivalente)

Tras obtener el esquema automático desde `df_original`:

```diff
- esquema_personalizado = self.obtener_esquema_tabla(df_original)
- df_para_insert = df_final  # provenía de normalizaciones previas
+ esquema_personalizado = self.obtener_esquema_tabla(df_original)
+ df_para_insert = self.aplicar_esquema_a_df(df_original, esquema_personalizado)

 # Continuar con renombrado -> to_sql usando df_para_insert
```

---

## Consideraciones de compatibilidad
- Conservar las claves de `esquema_personalizado`:
  - `col_limpio` (key del dict), `columna_original`, `tipo`, `es_fecha` (opcional).
- **No** alterar contratos de `normalizar_nulos`, `obtener_esquema_tabla`, UI de corrección.

---

## Pruebas (checklist)
1. **Tipos almacenados**
   ```sql
   SELECT
     Contrato, typeof(Contrato) AS t_contrato,
     Instalacion, typeof(Instalacion) AS t_inst
   FROM FicheroChile_test
   WHERE Contrato IN (1592237)
   LIMIT 20;
   ```
   - Esperado: `t_contrato = 'integer'`, `t_inst = 'integer'`. Valores coherentes/NULLs válidos.

2. **Fechas fantasma**
   ```sql
   SELECT *
   FROM FicheroChile_test
   WHERE Fecha_Inicio_Cuota = '1970-01-01';
   ```
   - Esperado: drástica reducción o 0 filas si la columna no era fecha real.

3. **Paridad de flujos**
   - Con UI de corrección y en modo automático, ambos deben producir los mismos tipos finales para las mismas columnas.

4. **Backward compatibility**
   - Archivos que ya funcionaban **siguen funcionando**; no hay columnas legítimas de fecha que se pierdan (si el nombre sugiere fecha, se siguen detectando).

---

## Criterios de aceptación
- [ ] Ninguna columna puramente numérica vuelve a ser clasificada como fecha por defecto.
- [ ] Los inserts usan **valores** transformados conforme al **esquema final** (no valores heredados de la detección previa).
- [ ] Se mantiene el rendimiento previo (±5%) en datasets medianos.
- [ ] Pruebas SQL anteriores pasan.

---

## Plan de rollback
- Mantener cambios detrás de commits aislados.
- Si aparece regresión, revertir sólo el commit de `detectar_columnas_fecha` o `aplicar_esquema_a_df` según corresponda.

---

## Notas para el agente (Claude Sonnet 4 en VS Code, modo agente)
- Crear branch `fix/date-detection-and-schema-apply`.
- Aplicar los parches arriba **sin renombrar** funciones públicas.
- Ejecutar pruebas de SQL indicadas tras carga de datos.
- Producir un breve diff/summary en el PR:
  - Archivos tocados
  - Funciones nuevas/modificadas
  - Evidencia de queries SQL

> Si el proyecto tiene pruebas automáticas, agregar tests para: (1) columna numérica con `0` no es fecha; (2) esquema corregido en UI convierte datos y no inserta `'1970-01-01'` en columnas INTEGER.

