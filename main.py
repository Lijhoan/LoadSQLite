# main.py
"""
🚀 ETL VISUAL - PUNTO DE ENTRADA ÚNICO
Aplicación para cargar datos Excel/CSV a SQLite
"""
import customtkinter as ctk
from interface import ETLInterface

def main():
    """Función principal que inicia la aplicación"""
    print("🚀 Iniciando ETL Visual...")
    
    # Configurar CustomTkinter
    ctk.set_appearance_mode("System")
    ctk.set_default_color_theme("blue")
    
    # Crear y ejecutar aplicación
    app = ETLInterface()
    app.mainloop()

if __name__ == "__main__":
    main()
