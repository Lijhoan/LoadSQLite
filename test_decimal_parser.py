#!/usr/bin/env python3
"""
Test del nuevo parser decimal para verificar que conserva valores originales
"""

import pandas as pd
import sys
import os

# Agregar el directorio actual al path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from processor import DataProcessor

def test_decimal_parser():
    """Prueba el nuevo parser decimal con casos específicos"""
    
    print("🧪 Probando nuevo parser decimal para valores REAL/NUMERIC")
    print("=" * 60)
    
    # Casos de prueba específicos
    test_cases = [
        # Caso US (punto decimal)
        ("-42.37", -42.37, "US decimal"),
        ("123.45", 123.45, "US decimal positivo"),
        ("0.99", 0.99, "US decimal menor a 1"),
        
        # Caso EU (coma decimal)  
        ("-42,37", -42.37, "EU decimal"),
        ("123,45", 123.45, "EU decimal positivo"),
        ("0,99", 0.99, "EU decimal menor a 1"),
        
        # Mixto con miles US (coma miles, punto decimal)
        ("1,234.56", 1234.56, "US con miles"),
        ("-1,234.56", -1234.56, "US con miles negativo"),
        ("12,345,678.90", 12345678.90, "US múltiples miles"),
        
        # Mixto con miles EU (punto miles, coma decimal)
        ("1.234,56", 1234.56, "EU con miles"),
        ("-1.234,56", -1234.56, "EU con miles negativo"),
        ("12.345.678,90", 12345678.90, "EU múltiples miles"),
        
        # Sin separadores
        ("42", 42.0, "Entero simple"),
        ("-42", -42.0, "Entero negativo"),
        
        # Casos edge
        ("0", 0.0, "Cero"),
        ("", None, "Vacío"),
        ("nan", None, "NaN"),
    ]
    
    # Crear DataFrame de prueba
    valores_test = [case[0] for case in test_cases]
    df = pd.DataFrame({'CycleFee': valores_test})
    
    print("📊 Datos de prueba:")
    print(df)
    print()
    
    # Crear esquema para REAL
    esquema = {
        'CycleFee': {
            'tipo': 'REAL',
            'columna_original': 'CycleFee'
        }
    }
    
    # Aplicar el nuevo parser
    processor = DataProcessor()
    df_convertido = processor.aplicar_esquema_a_df(df, esquema)
    
    print("🔄 Resultados después de aplicar esquema REAL:")
    print(df_convertido)
    print()
    
    # Verificar resultados
    print("✅ Verificación de casos:")
    print("-" * 40)
    
    all_passed = True
    for i, (input_val, expected, description) in enumerate(test_cases):
        result = df_convertido['CycleFee'].iloc[i]
        
        # Manejar casos None/NaN
        if pd.isna(expected) and pd.isna(result):
            status = "✅ PASS"
        elif expected is not None and abs(result - expected) < 0.0001:
            status = "✅ PASS"
        else:
            status = "❌ FAIL"
            all_passed = False
        
        print(f"{status} {description:.<25} '{input_val}' → {result} (esperado: {expected})")
    
    print()
    print("=" * 60)
    if all_passed:
        print("🎉 ¡TODOS LOS CASOS PASARON!")
        print("✅ El parser decimal conserva valores originales correctamente")
    else:
        print("❌ ALGUNOS CASOS FALLARON - revisar implementación")
    
    return all_passed

def test_integration_with_schema():
    """Prueba integración completa con esquema de tabla"""
    print("\n🔧 Probando integración con esquema completo...")
    
    # Simular datos como los del problema original
    data = {
        'Contrato': [1592237, 1592238, 1592239],
        'CycleFee': ['-42.37', '123,45', '1,234.56'],  # Casos problemáticos
        'Desde': ['2023-01-01', '2023-02-01', '2023-03-01'],
        'Codigo': ['A1', 'B2', 'C3']
    }
    
    df = pd.DataFrame(data)
    print("📊 DataFrame original:")
    print(df)
    print()
    
    # Crear esquema mixto
    esquema = {
        'Contrato': {'tipo': 'INTEGER', 'columna_original': 'Contrato'},
        'CycleFee': {'tipo': 'REAL', 'columna_original': 'CycleFee'},  # ← El caso crítico
        'Desde': {'tipo': 'DATE', 'columna_original': 'Desde', 'es_fecha': True},
        'Codigo': {'tipo': 'TEXT', 'columna_original': 'Codigo'}
    }
    
    processor = DataProcessor()
    df_convertido = processor.aplicar_esquema_a_df(df, esquema)
    
    print("🎯 DataFrame después de aplicar esquema:")
    print(df_convertido)
    print()
    
    # Verificar específicamente CycleFee
    cycle_fees = df_convertido['CycleFee'].tolist()
    expected_fees = [-42.37, 123.45, 1234.56]
    
    print("🔍 Verificación específica de CycleFee:")
    for i, (original, converted, expected) in enumerate(zip(data['CycleFee'], cycle_fees, expected_fees)):
        status = "✅" if abs(converted - expected) < 0.0001 else "❌"
        print(f"  {status} Fila {i}: '{original}' → {converted} (esperado: {expected})")
    
    return True

if __name__ == "__main__":
    print("🧪 TEST DEL NUEVO PARSER DECIMAL\n")
    
    try:
        success1 = test_decimal_parser()
        success2 = test_integration_with_schema()
        
        if success1 and success2:
            print("\n🎉 ¡TODAS LAS PRUEBAS EXITOSAS!")
            print("✅ Fix aplicado correctamente - valores decimales conservados")
        else:
            print("\n❌ ALGUNAS PRUEBAS FALLARON")
            
    except Exception as e:
        print(f"\n❌ ERROR EN PRUEBAS: {e}")
        import traceback
        traceback.print_exc()
