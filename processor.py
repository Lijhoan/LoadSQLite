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
                self.callback_progreso(0.35, "🎨 Preparando corrección gráfica de tipos...")
                
                # Generar esquema inicial para corrección
                esquema_inicial = self.obtener_esquema_tabla(df_original)
                
                # Importar y abrir ventana de corrección
                try:
                    from correccion_tipos import VentanaCorreccionTipos
                    
                    # Variable para resultado
                    resultado_correccion = {'completado': False, 'esquema': esquema_inicial}
                    
                    def callback_correccion(aplicar_cambios, esquema_resultado):
                        resultado_correccion['completado'] = True
                        resultado_correccion['aplicar_cambios'] = aplicar_cambios
                        resultado_correccion['esquema'] = esquema_resultado
                    
                    # Abrir ventana (esto debe ejecutarse en el hilo principal)
                    # La ventana se abrirá usando el callback configurado desde interface.py
                    if hasattr(self, 'callback_correccion_tipos') and self.callback_correccion_tipos:
                        self.callback_correccion_tipos(df_original, esquema_inicial, callback_correccion)
                        
                        # Esperar resultado (simplificado)
                        import time
                        timeout = 300  # 5 minutos máximo
                        elapsed = 0
                        while not resultado_correccion['completado'] and elapsed < timeout:
                            time.sleep(0.1)
                            elapsed += 0.1
                            if self.cancelado:
                                conn.close()
                                return
                        
                        # Usar esquema corregido si se aplicaron cambios
                        if resultado_correccion.get('aplicar_cambios', False):
                            esquema_personalizado = resultado_correccion['esquema']
                        else:
                            esquema_personalizado = esquema_inicial
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
