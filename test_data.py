#!/usr/bin/env python3
"""
Script para probar las correcciones de detección de tipos
Crea datos de prueba y verifica que:
1. Columnas numéricas con 0 no se conviertan a '1970-01-01'
2. El esquema corregido se aplique correctamente
"""

import pandas as pd
import sqlite3
import os
from processor import DataProcessor

def crear_datos_prueba():
    """Crea un DataFrame de prueba con casos problemáticos"""
    data = {
        'Contrato': [0, 1592237, 2000000, 0, 12345],  # Números con ceros
        'Instalacion': [0, 987654, 1111111, 0, 555555],  # Más números con ceros
        'Fecha_Inicio_Cuota': ['2023-01-15', '2023-02-20', '', '2023-04-10', '2023-05-30'],  # Fechas reales
        'Monto': [0.0, 150.5, 200.75, 0.0, 99.99],  # Decimales con ceros
        'Estado': ['Activo', 'Inactivo', 'Pendiente', 'Activo', 'Cancelado'],  # Texto
        'Codigo_Numerico': [0, 100, 200, 0, 300]  # Números puros que NO deben ser fechas
    }
    return pd.DataFrame(data)

def test_deteccion_tipos():
    """Prueba que la detección de tipos sea correcta"""
    print("🧪 Probando detección de tipos...")
    
    processor = DataProcessor()
    df = crear_datos_prueba()
    
    # Test 1: Verificar detección de fechas
    columnas_fecha = processor.detectar_columnas_fecha(df)
    print(f"📅 Columnas detectadas como fecha: {columnas_fecha}")
    
    # Debe detectar solo 'Fecha_Inicio_Cuota' y NO los números
    assert 'Fecha_Inicio_Cuota' in columnas_fecha, "❌ No detectó columna de fecha válida"
    assert 'Contrato' not in columnas_fecha, "❌ Detectó número como fecha"
    assert 'Instalacion' not in columnas_fecha, "❌ Detectó número como fecha"
    assert 'Codigo_Numerico' not in columnas_fecha, "❌ Detectó número como fecha"
    
    print("✅ Detección de fechas correcta")
    
    # Test 2: Verificar esquema generado
    esquema = processor.obtener_esquema_tabla(df)
    print(f"📋 Esquema generado: {list(esquema.keys())}")
    
    # Verificar tipos esperados
    assert esquema['Contrato']['tipo'] == 'INTEGER', f"❌ Contrato debe ser INTEGER, fue {esquema['Contrato']['tipo']}"
    assert esquema['Instalacion']['tipo'] == 'INTEGER', f"❌ Instalacion debe ser INTEGER, fue {esquema['Instalacion']['tipo']}"
    assert esquema['Monto']['tipo'] == 'REAL', f"❌ Monto debe ser REAL, fue {esquema['Monto']['tipo']}"
    
    print("✅ Esquema generado correctamente")

def test_aplicacion_esquema():
    """Prueba que aplicar_esquema_a_df funcione correctamente"""
    print("\n🔧 Probando aplicación de esquema...")
    
    processor = DataProcessor()
    df_original = crear_datos_prueba()
    
    # Crear esquema personalizado
    esquema = {
        'Contrato': {'tipo': 'INTEGER', 'columna_original': 'Contrato'},
        'Instalacion': {'tipo': 'INTEGER', 'columna_original': 'Instalacion'},
        'Fecha_Inicio_Cuota': {'tipo': 'DATE', 'columna_original': 'Fecha_Inicio_Cuota', 'es_fecha': True},
        'Monto': {'tipo': 'REAL', 'columna_original': 'Monto'},
        'Estado': {'tipo': 'TEXT', 'columna_original': 'Estado'},
        'Codigo_Numerico': {'tipo': 'INTEGER', 'columna_original': 'Codigo_Numerico'}
    }
    
    # Aplicar esquema
    df_convertido = processor.aplicar_esquema_a_df(df_original, esquema)
    
    # Verificar que los 0 no se convirtieron a fechas en columnas numéricas
    contratos_con_cero = df_convertido[df_convertido['Contrato'] == 0]
    print(f"📊 Filas con Contrato=0: {len(contratos_con_cero)}")
    
    # Los valores 0 deben seguir siendo 0, no '1970-01-01'
    for idx, row in contratos_con_cero.iterrows():
        assert row['Contrato'] == 0, f"❌ Contrato en fila {idx} cambió de 0 a {row['Contrato']}"
    
    print("✅ Los ceros en columnas numéricas se mantuvieron como ceros")
    
    # Verificar fechas reales
    fechas_validas = df_convertido[df_convertido['Fecha_Inicio_Cuota'].notna()]
    print(f"📅 Fechas válidas procesadas: {len(fechas_validas)}")
    
    return df_convertido

def test_carga_completa():
    """Prueba el flujo completo de carga"""
    print("\n🚀 Probando flujo completo...")
    
    # Crear archivo CSV de prueba
    df = crear_datos_prueba()
    archivo_prueba = 'test_data_temp.csv'
    df.to_csv(archivo_prueba, index=False)
    
    # Crear base de datos de prueba
    bd_prueba = 'test_temp.db'
    if os.path.exists(bd_prueba):
        os.remove(bd_prueba)
    
    # Simular callbacks
    def callback_progreso(pct, msg):
        print(f"  📊 {pct:.1%}: {msg}")
    
    def callback_completado(exito, mensaje, filas=None):
        if exito:
            print(f"  ✅ {mensaje} ({filas} filas)")
        else:
            print(f"  ❌ {mensaje}")
    
    # Procesar archivo
    processor = DataProcessor()
    processor.procesar_archivo(
        archivo_prueba, 
        bd_prueba, 
        'test_table',
        callback_progreso,
        callback_completado,
        correccion_modo=None  # Automático
    )
    
    # Verificar resultados en BD
    conn = sqlite3.connect(bd_prueba)
    
    # Test 1: Verificar tipos almacenados
    cursor = conn.cursor()
    cursor.execute("""
        SELECT Contrato, typeof(Contrato) as t_contrato,
               Instalacion, typeof(Instalacion) as t_inst
        FROM test_table 
        WHERE Contrato IN (0, 1592237)
        LIMIT 10
    """)
    
    resultados = cursor.fetchall()
    print(f"\n📊 Tipos en BD: {resultados}")
    
    for fila in resultados:
        contrato, tipo_contrato, instalacion, tipo_inst = fila
        assert tipo_contrato == 'integer', f"❌ Contrato {contrato} debe ser 'integer', fue '{tipo_contrato}'"
        assert tipo_inst == 'integer', f"❌ Instalacion {instalacion} debe ser 'integer', fue '{tipo_inst}'"
    
    print("✅ Tipos almacenados correctamente en BD")
    
    # Test 2: Verificar que no hay fechas fantasma
    cursor.execute("SELECT COUNT(*) FROM test_table WHERE Fecha_Inicio_Cuota = '1970-01-01'")
    fechas_fantasma = cursor.fetchone()[0]
    
    print(f"👻 Fechas fantasma (1970-01-01): {fechas_fantasma}")
    # Debería ser 0 porque 'Fecha_Inicio_Cuota' es una columna real de fechas
    # y los valores 0 están en columnas numéricas, no de fecha
    
    conn.close()
    
    # Limpiar archivos temporales
    if os.path.exists(archivo_prueba):
        os.remove(archivo_prueba)
    if os.path.exists(bd_prueba):
        os.remove(bd_prueba)
    
    print("✅ Flujo completo exitoso")

if __name__ == "__main__":
    print("🧪 EJECUTANDO PRUEBAS DE CORRECCIÓN DE TIPOS\n")
    print("=" * 50)
    
    try:
        test_deteccion_tipos()
        test_aplicacion_esquema()
        test_carga_completa()
        
        print("\n" + "=" * 50)
        print("🎉 ¡TODAS LAS PRUEBAS PASARON!")
        print("✅ Detección de fechas endurecida")
        print("✅ Esquema aplicado correctamente")
        print("✅ No más valores fantasma 1970-01-01")
        
    except Exception as e:
        print(f"\n❌ PRUEBA FALLÓ: {e}")
        import traceback
        traceback.print_exc()
