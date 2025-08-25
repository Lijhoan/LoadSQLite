#!/usr/bin/env python3
"""Prueba simple de las correcciones"""

import pandas as pd
import sys
import os

# Agregar el directorio actual al path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from processor import DataProcessor
    
    print("🧪 Probando correcciones de tipos...")
    
    # Crear datos de prueba
    data = {
        'Contrato': [0, 1592237, 2000000],
        'Fecha_Inicio': ['2023-01-15', '2023-02-20', '2023-03-25'],
        'Numero_Simple': [0, 100, 200]
    }
    df = pd.DataFrame(data)
    
    print("📊 Datos de prueba:")
    print(df)
    print()
    
    # Instanciar procesador
    processor = DataProcessor()
    
    # Test 1: Detección de fechas
    print("📅 Probando detección de fechas...")
    fechas_detectadas = processor.detectar_columnas_fecha(df)
    print(f"Fechas detectadas: {fechas_detectadas}")
    
    # Debe detectar solo 'Fecha_Inicio'
    if 'Fecha_Inicio' in fechas_detectadas:
        print("✅ Detectó columna de fecha correcta")
    else:
        print("❌ No detectó columna de fecha")
    
    if 'Contrato' not in fechas_detectadas and 'Numero_Simple' not in fechas_detectadas:
        print("✅ NO detectó números como fechas")
    else:
        print("❌ Detectó números como fechas")
    
    print()
    
    # Test 2: Esquema
    print("📋 Probando generación de esquema...")
    esquema = processor.obtener_esquema_tabla(df)
    
    for col, info in esquema.items():
        print(f"  {col}: {info['tipo']} (original: {info['columna_original']})")
    
    print()
    
    # Test 3: Aplicación de esquema
    print("🔧 Probando aplicación de esquema...")
    df_convertido = processor.aplicar_esquema_a_df(df, esquema)
    
    print("Datos después de aplicar esquema:")
    print(df_convertido)
    print()
    
    # Verificar que los 0 se mantienen como 0
    contratos_cero = df_convertido[df_convertido['Contrato'] == 0]
    if len(contratos_cero) > 0:
        print("✅ Los valores 0 se mantuvieron como 0 (no se convirtieron a 1970-01-01)")
    else:
        print("❓ No se encontraron valores 0 para verificar")
    
    print("\n🎉 Pruebas básicas completadas!")
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
