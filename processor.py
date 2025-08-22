# processor.py
"""
⚡ PROCESADOR DE DATOS ETL LIMPIO
Maneja la carga y procesamiento de archivos Excel/CSV con formato uniforme
"""
import pandas as pd
import sqlite3
import time
import os
import numpy as np
from datetime import datetime
import re

class DataProcessor:
    """Procesador de datos para ETL con manejo uniforme de fechas y nulos"""
    
    def __init__(self):
        self.cancelado = False
        self.callback_progreso = None
        self.callback_completado = None
        self.callback_correccion_tipos = None  # Para ventana gráfica de corrección
        
        # Patrones para detectar columnas de fecha
        self.fecha_patterns = [
            r'fecha', r'date', r'datetime', r'timestamp', 
            r'creado', r'actualizado', r'modificado',
            r'registro', r'ingreso', r'alta', r'baja'
        ]
    
    def detectar_columnas_fecha(self, df):
        """Detecta automáticamente columnas que contienen fechas"""
        columnas_fecha = []
        
        for col in df.columns:
            # Verificar por nombre de columna
            col_lower = str(col).lower()
            es_fecha_por_nombre = any(re.search(pattern, col_lower) for pattern in self.fecha_patterns)
            
            # Verificar por tipo de datos
            es_fecha_por_tipo = df[col].dtype in ['datetime64[ns]', 'datetime64', 'object']
            
            # Si parece fecha, intentar conversión
            if es_fecha_por_nombre or es_fecha_por_tipo:
                try:
                    # Tomar muestra pequeña para verificar
                    muestra = df[col].dropna().head(10)
                    if len(muestra) > 0:
                        pd.to_datetime(muestra, errors='raise')
                        columnas_fecha.append(col)
                except:
                    continue
        
        return columnas_fecha
    
    def normalizar_fechas(self, df):
        """Normaliza todas las fechas a formato ISO YYYY-MM-DD"""
        df_copy = df.copy()
        columnas_fecha = self.detectar_columnas_fecha(df_copy)
        
        for col in columnas_fecha:
            try:
                # Convertir a datetime con manejo de errores
                df_copy[col] = pd.to_datetime(df_copy[col], errors='coerce')
                
                # Convertir a formato ISO (solo fecha, sin hora)
                df_copy[col] = df_copy[col].dt.strftime('%Y-%m-%d')
                
                # Los valores que no se pudieron convertir quedan como None
                df_copy[col] = df_copy[col].replace('NaT', None)
                
            except Exception:
                continue
        
        return df_copy, columnas_fecha
    
    def normalizar_nulos(self, df):
        """Normaliza todos los valores nulos a None estándar"""
        df_copy = df.copy()
        
        # Reemplazar diferentes tipos de nulos con None
        df_copy = df_copy.replace({
            np.nan: None,
            pd.NaT: None,
            'NaN': None,
            'NaT': None,
            'nan': None,
            'null': None,
            'NULL': None,
            '': None,
            ' ': None
        })
        
        # Convertir objetos NaN a None
        for col in df_copy.columns:
            if df_copy[col].dtype == 'object':
                df_copy[col] = df_copy[col].where(pd.notna(df_copy[col]), None)
        
        return df_copy
    
    def obtener_esquema_tabla(self, df):
        """Genera esquema de tabla con tipos SQLite apropiados"""
        esquema = {}
        df_normalizado, columnas_fecha = self.normalizar_fechas(df)
        
        for col in df_normalizado.columns:
            # Limpiar nombre de columna
            col_limpio = str(col).replace(' ', '_').replace('-', '_')
            col_limpio = re.sub(r'[^\w]', '_', col_limpio)
            
            # Determinar tipo
            if col in columnas_fecha:
                tipo_sql = 'TEXT'  # Fechas como TEXT en formato ISO
                ejemplo = df_normalizado[col].dropna().iloc[0] if len(df_normalizado[col].dropna()) > 0 else None
            else:
                # Inferir tipo para otros campos
                muestra_sin_nulos = df_normalizado[col].dropna()
                
                if len(muestra_sin_nulos) == 0:
                    tipo_sql = 'TEXT'
                    ejemplo = None
                elif muestra_sin_nulos.dtype in ['int64', 'int32']:
                    tipo_sql = 'INTEGER'
                    ejemplo = int(muestra_sin_nulos.iloc[0])
                elif muestra_sin_nulos.dtype in ['float64', 'float32']:
                    tipo_sql = 'REAL'
                    ejemplo = float(muestra_sin_nulos.iloc[0])
                else:
                    tipo_sql = 'TEXT'
                    ejemplo = str(muestra_sin_nulos.iloc[0])
            
            esquema[col_limpio] = {
                'tipo': tipo_sql,
                'columna_original': col,
                'es_fecha': col in columnas_fecha,
                'ejemplo': ejemplo
            }
        
        return esquema
    
    def detectar_problemas_tipos(self, df, esquema):
        """Detecta problemas potenciales en los tipos de datos detectados automáticamente"""
        problemas = []
        
        for col_limpio, info in esquema.items():
            col_original = info['columna_original']
            tipo_detectado = info['tipo']
            
            # Obtener muestra de datos sin nulos
            muestra = df[col_original].dropna()
            if len(muestra) == 0:
                continue
            
            # PROBLEMA 1: Números grandes interpretados como texto
            if tipo_detectado == 'TEXT':
                # Verificar si contiene números que podrían ser enteros
                muestra_str = muestra.astype(str)
                numeros_detectados = 0
                for valor in muestra_str.head(20):  # Revisar primeros 20
                    if valor.replace('.', '').replace('-', '').isdigit():
                        numeros_detectados += 1
                
                if numeros_detectados > len(muestra_str.head(20)) * 0.7:  # 70% son números
                    problemas.append({
                        'columna': col_original,
                        'problema': 'Números detectados como TEXT - posible INTEGER/REAL',
                        'sugerencia': 'INTEGER',  # Cambié tipo_sugerido por sugerencia
                        'categoria': 'numeros_como_texto'
                    })
            
            # PROBLEMA 2: Fechas no detectadas (campos de texto que parecen fechas)
            if tipo_detectado == 'TEXT' and not info['es_fecha']:
                # Buscar patrones de fecha en el nombre de columna
                col_lower = col_original.lower()
                if any(patron in col_lower for patron in ['fecha', 'date', 'time', 'created', 'updated']):
                    problemas.append({
                        'columna': col_original,
                        'problema': 'Posible fecha no detectada - revisar formato',
                        'sugerencia': 'DATE',  # Cambié tipo_sugerido por sugerencia
                        'categoria': 'fecha_no_detectada'
                    })
            
            # PROBLEMA 3: Campos mixtos (enteros y decimales mezclados)
            if tipo_detectado == 'REAL':
                # Verificar si todos los valores son enteros
                valores_enteros = 0
                for valor in muestra.head(20):
                    if pd.notna(valor) and float(valor).is_integer():
                        valores_enteros += 1
                
                if valores_enteros == len(muestra.head(20)):
                    problemas.append({
                        'columna': col_original,
                        'problema': 'Todos los valores son enteros - considerar INTEGER',
                        'sugerencia': 'INTEGER',  # Cambié tipo_sugerido por sugerencia
                        'categoria': 'real_podria_ser_integer'
                    })
            
            # PROBLEMA 4: Campos que podrían ser BOOLEAN
            if tipo_detectado in ['TEXT', 'INTEGER']:
                valores_unicos = set(muestra.astype(str).str.lower().unique()[:10])
                if valores_unicos.issubset({'0', '1', 'true', 'false', 'yes', 'no', 'si', 'no', 't', 'f'}):
                    problemas.append({
                        'columna': col_original,
                        'problema': 'Valores binarios detectados - considerar BOOLEAN',
                        'sugerencia': 'BOOLEAN',  # Cambié tipo_sugerido por sugerencia
                        'categoria': 'posible_boolean'
                    })
        
        return problemas
    
    def _continuar_despues_correccion(self, aplicar_cambios, esquema_resultado):
        """Continúa el procesamiento después de que el usuario termine la corrección de tipos"""
        try:
            # Recuperar datos guardados
            datos = self._datos_pendientes
            df_original = datos['df_original']
            df_final = datos['df_final']
            bd_destino = datos['bd_destino']
            nombre_tabla = datos['nombre_tabla']
            # NO usar la conexión anterior - crear nueva en este hilo
            esquema_inicial = datos['esquema_inicial']
            
            # CRÍTICO: Cerrar conexión anterior y crear nueva en este hilo
            try:
                datos['conn'].close()
            except:
                pass  # Ignorar errores al cerrar
            
            # Crear NUEVA conexión en el hilo correcto
            conn = sqlite3.connect(bd_destino)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA cache_size=10000")
            
            # Determinar esquema final
            if aplicar_cambios:
                esquema_personalizado = esquema_resultado
                self.callback_progreso(0.45, "✅ Aplicando correcciones de tipos...")
            else:
                esquema_personalizado = esquema_inicial
                self.callback_progreso(0.45, "🔄 Usando detección automática...")

            self.callback_progreso(0.5, "📋 Creando esquema de tabla...")
            
            # Crear tabla con esquema apropiado (usar esquema personalizado)
            esquema = esquema_personalizado
            sql_create = f"CREATE TABLE IF NOT EXISTS {nombre_tabla} (\n"
            columnas_sql = []
            for col_limpio, info in esquema.items():
                columnas_sql.append(f"    {col_limpio} {info['tipo']}")
            sql_create += ",\n".join(columnas_sql) + "\n)"
            
            conn.execute(f"DROP TABLE IF EXISTS {nombre_tabla}")
            conn.execute(sql_create)
            conn.commit()
            
            if self.cancelado:
                conn.close()
                return
            
            self.callback_progreso(0.6, "📊 Insertando datos...")
            
            # Preparar datos para inserción
            datos_insercion = []
            for _, fila in df_final.iterrows():
                fila_limpia = []
                for col_limpio, info in esquema.items():
                    valor_original = fila[info['columna_original']]
                    
                    if pd.isna(valor_original):
                        fila_limpia.append(None)
                    else:
                        fila_limpia.append(valor_original)
                
                datos_insercion.append(tuple(fila_limpia))
                
                if self.cancelado:
                    conn.close()
                    return
            
            # Insertar por lotes
            columnas_limpias = list(esquema.keys())
            placeholders = ", ".join(["?" for _ in columnas_limpias])
            sql_insert = f"INSERT INTO {nombre_tabla} ({', '.join(columnas_limpias)}) VALUES ({placeholders})"
            
            # Procesar en lotes de 1000
            chunk_size = 1000
            total_filas = len(datos_insercion)
            
            for i in range(0, total_filas, chunk_size):
                if self.cancelado:
                    conn.close()
                    return
                
                chunk = datos_insercion[i:i + chunk_size]
                conn.executemany(sql_insert, chunk)
                conn.commit()
                
                # Actualizar progreso
                progreso = 0.6 + (0.3 * (i + len(chunk)) / total_filas)
                self.callback_progreso(progreso, f"📊 Insertando: {i + len(chunk):,}/{total_filas:,} filas")
            
            conn.close()
            
            # Éxito
            self.callback_completado(True, "Carga completada exitosamente", total_filas)
            
        except Exception as e:
            if 'conn' in locals():
                conn.close()
            self.callback_completado(False, f"Error en carga: {str(e)}")
    
    def cargar_preview(self, archivo):
        """Carga una vista previa del archivo con formato normalizado"""
        try:
            if archivo.lower().endswith('.csv'):
                df = pd.read_csv(archivo, nrows=100)
            else:
                df = pd.read_excel(archivo, nrows=100)
            
            # Normalizar fechas y nulos
            df_normalizado, _ = self.normalizar_fechas(df)
            df_final = self.normalizar_nulos(df_normalizado)
            
            return df_final
        except Exception as e:
            raise Exception(f"Error al cargar vista previa: {str(e)}")
    
    def procesar_archivo(self, archivo, bd_destino, nombre_tabla, callback_progreso, callback_completado, correccion_modo=None):
        """Procesa el archivo completo y lo carga a SQLite con formato normalizado
        
        Args:
            correccion_modo: None/False=automático, "grafica"=ventana gráfica, "consola"=por consola
        """
        self.cancelado = False
        self.callback_progreso = callback_progreso
        self.callback_completado = callback_completado
        
        try:
            # Actualizar progreso
            self.callback_progreso(0.1, "📂 Leyendo archivo...")
            
            # Leer archivo completo
            if archivo.lower().endswith('.csv'):
                df_original = pd.read_csv(archivo)
            else:
                df_original = pd.read_excel(archivo)
            
            if self.cancelado:
                return
            
            self.callback_progreso(0.2, "🔧 Normalizando fechas y valores...")
            
            # Normalizar fechas y nulos
            df_normalizado, columnas_fecha = self.normalizar_fechas(df_original)
            df_final = self.normalizar_nulos(df_normalizado)
            
            if self.cancelado:
                return
            
            self.callback_progreso(0.3, "🗃️ Preparando base de datos...")
            
            # Conectar a SQLite
            conn = sqlite3.connect(bd_destino)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA cache_size=10000")
            
            if self.cancelado:
                conn.close()
                return
            
            # NUEVO: Corrección de tipos gráfica
            if correccion_modo == "grafica":
                self.callback_progreso(0.35, "🎨 Abriendo ventana de corrección de tipos...")
                
                # Generar esquema inicial para corrección
                esquema_inicial = self.obtener_esquema_tabla(df_original)
                
                # Importar y abrir ventana de corrección
                try:
                    # La ventana se abrirá usando el callback configurado desde interface.py
                    if hasattr(self, 'callback_correccion_tipos') and self.callback_correccion_tipos:
                        # CAMBIO CRÍTICO: No continuar aquí, delegar a la ventana
                        self.callback_progreso(0.4, "⏸️ Esperando corrección de tipos...")
                        
                        # Preparar datos para continuar después
                        self._datos_pendientes = {
                            'df_original': df_original,
                            'df_final': df_final,
                            'bd_destino': bd_destino,
                            'nombre_tabla': nombre_tabla,
                            # NO incluir conn - se creará nueva en el otro hilo
                            'esquema_inicial': esquema_inicial
                        }
                        
                        # Abrir ventana Y PARAR AQUÍ
                        self.callback_correccion_tipos(df_original, esquema_inicial, self._continuar_despues_correccion)
                        
                        # Cerrar conexión original ya que se creará nueva en el callback
                        conn.close()
                        return  # ← CRÍTICO: Parar aquí y esperar
                    else:
                        esquema_personalizado = self.obtener_esquema_tabla(df_original)
                        
                except ImportError:
                    print("⚠️ No se pudo cargar ventana gráfica, usando esquema automático")
                    esquema_personalizado = self.obtener_esquema_tabla(df_original)
            else:
                esquema_personalizado = self.obtener_esquema_tabla(df_original)

            self.callback_progreso(0.4, "📋 Creando esquema de tabla...")
            
            # Crear tabla con esquema apropiado (usar esquema personalizado si existe)
            esquema = esquema_personalizado
            sql_create = f"CREATE TABLE IF NOT EXISTS {nombre_tabla} (\n"
            columnas_sql = []
            for col_limpio, info in esquema.items():
                columnas_sql.append(f"    {col_limpio} {info['tipo']}")
            sql_create += ",\n".join(columnas_sql) + "\n)"
            
            conn.execute(f"DROP TABLE IF EXISTS {nombre_tabla}")
            conn.execute(sql_create)
            
            if self.cancelado:
                conn.close()
                return
            
            self.callback_progreso(0.5, f"📊 Cargando {len(df_final):,} filas...")
            
            # Renombrar columnas para que coincidan con el esquema
            df_para_cargar = df_final.copy()
            mapeo_columnas = {info['columna_original']: col_limpio 
                            for col_limpio, info in esquema.items()}
            df_para_cargar = df_para_cargar.rename(columns=mapeo_columnas)
            
            # CARGAR DATOS CON MANEJO SEGURO DE MUCHAS COLUMNAS
            num_columnas = len(df_para_cargar.columns)
            
            # Ajustar chunk_size según número de columnas para evitar "too many SQL variables"
            if num_columnas > 50:
                chunk_size = 100  # Muy pocas filas para muchas columnas
            elif num_columnas > 20:
                chunk_size = 250  # Pocas filas para bastantes columnas
            else:
                chunk_size = 500  # Filas normales para pocas columnas
            
            total_chunks = len(df_para_cargar) // chunk_size + (1 if len(df_para_cargar) % chunk_size else 0)
            
            for i, chunk_start in enumerate(range(0, len(df_para_cargar), chunk_size)):
                if self.cancelado:
                    conn.close()
                    return
                
                chunk_end = min(chunk_start + chunk_size, len(df_para_cargar))
                chunk_df = df_para_cargar.iloc[chunk_start:chunk_end]
                
                # Cargar chunk de forma segura
                try:
                    chunk_df.to_sql(nombre_tabla, conn, if_exists='append', index=False)
                except Exception as e:
                    if "too many SQL variables" in str(e).lower():
                        # Si aún hay error, usar mini-chunks
                        for mini_start in range(0, len(chunk_df), 50):
                            mini_end = min(mini_start + 50, len(chunk_df))
                            mini_chunk = chunk_df.iloc[mini_start:mini_end]
                            mini_chunk.to_sql(nombre_tabla, conn, if_exists='append', index=False)
                    else:
                        raise e
                
                # Actualizar progreso
                progreso = 0.5 + (0.4 * (i + 1) / total_chunks)
                self.callback_progreso(progreso, f"📊 Procesando lote {i+1:,} de {total_chunks:,}")
                
                time.sleep(0.01)  # Pequeña pausa para no bloquear UI
            
            conn.close()
            
            if not self.cancelado:
                self.callback_progreso(1.0, "✅ ¡Carga completada!")
                
                # Información detallada del resultado
                info_fechas = f"\n📅 Columnas de fecha normalizadas: {len(columnas_fecha)}" if columnas_fecha else ""
                mensaje_detalle = f"Datos normalizados correctamente:{info_fechas}\n🔧 Valores nulos estandarizados\n📊 {len(df_final):,} filas procesadas"
                
                self.callback_completado(True, mensaje_detalle, len(df_final))
                
        except Exception as e:
            if hasattr(self, 'callback_completado') and self.callback_completado:
                self.callback_completado(False, str(e))
    
    def obtener_tablas_bd(self, bd_path):
        """Obtiene la lista de tablas en una base de datos"""
        try:
            conn = sqlite3.connect(bd_path)
            cursor = conn.cursor()
            
            # Consultar tablas
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            tablas = [row[0] for row in cursor.fetchall()]
            
            conn.close()
            return tablas
            
        except Exception as e:
            raise Exception(f"Error al consultar base de datos: {str(e)}")
    
    def cancelar(self):
        """Cancela el proceso en curso"""
        self.cancelado = True
