import customtkinter as ctk
from tkinter import messagebox
import pandas as pd

class VentanaCorreccionTipos:
    def __init__(self, parent, df, esquema_inicial, callback_resultado):
        self.parent = parent
        self.df = df
        self.esquema_inicial = esquema_inicial
        self.callback_resultado = callback_resultado
        self.tipos_corregidos = None
        
        # Tipos SQLite disponibles
        self.tipos_sqlite_disponibles = [
            'INTEGER',
            'REAL', 
            'TEXT',
            'BLOB',
            'NUMERIC',
            'BOOLEAN',
            'DATE',
            'DATETIME'
        ]
        
        # Crear ventana
        self.ventana = ctk.CTkToplevel(parent)
        self.ventana.title("🔧 Corrección de Tipos de Datos SQLite")
        self.ventana.geometry("1200x700")
        self.ventana.transient(parent)
        self.ventana.grab_set()
        
        # Variables para dropdowns
        self.dropdown_vars = {}
        self.setup_ui()
        
    def setup_ui(self):
        # Header
        header_frame = ctk.CTkFrame(self.ventana)
        header_frame.pack(fill="x", padx=20, pady=10)
        
        title_label = ctk.CTkLabel(
            header_frame, 
            text="🔍 Revisión y Corrección de Tipos de Datos",
            font=ctk.CTkFont(size=18, weight="bold")
        )
        title_label.pack(pady=10)
        
        info_label = ctk.CTkLabel(
            header_frame,
            text=f"📊 {len(self.esquema_inicial)} columnas detectadas - Selecciona el tipo correcto para cada una",
            font=ctk.CTkFont(size=12)
        )
        info_label.pack()
        
        # Frame principal con scroll
        main_frame = ctk.CTkFrame(self.ventana)
        main_frame.pack(fill="both", expand=True, padx=20, pady=10)
        
        # Scrollable frame
        self.scrollable_frame = ctk.CTkScrollableFrame(main_frame)
        self.scrollable_frame.pack(fill="both", expand=True, padx=10, pady=10)
        
        # Headers de la tabla
        headers_frame = ctk.CTkFrame(self.scrollable_frame)
        headers_frame.pack(fill="x", pady=(0, 10))
        
        ctk.CTkLabel(headers_frame, text="📋 Columna", font=ctk.CTkFont(weight="bold"), width=200).pack(side="left", padx=5)
        ctk.CTkLabel(headers_frame, text="🔍 Detectado", font=ctk.CTkFont(weight="bold"), width=100).pack(side="left", padx=5)
        ctk.CTkLabel(headers_frame, text="📑 Ejemplos", font=ctk.CTkFont(weight="bold"), width=250).pack(side="left", padx=5)
        ctk.CTkLabel(headers_frame, text="⚙️ Tipo Correcto", font=ctk.CTkFont(weight="bold"), width=150).pack(side="left", padx=5)
        ctk.CTkLabel(headers_frame, text="🚨 Problema", font=ctk.CTkFont(weight="bold"), width=200).pack(side="left", padx=5)
        
        # Detectar problemas automáticamente
        from processor import DataProcessor
        temp_processor = DataProcessor()
        problemas = temp_processor.detectar_problemas_tipos(self.df, self.esquema_inicial)
        problemas_dict = {p['columna']: p for p in problemas}
        
        # Crear fila para cada columna
        for col_limpio, info in self.esquema_inicial.items():
            self.crear_fila_columna(col_limpio, info, problemas_dict)
        
        # Botones
        buttons_frame = ctk.CTkFrame(self.ventana)
        buttons_frame.pack(fill="x", padx=20, pady=10)
        
        ctk.CTkButton(
            buttons_frame,
            text="✅ Aplicar Correcciones",
            command=self.aplicar_correcciones,
            font=ctk.CTkFont(size=14, weight="bold"),
            width=200,
            height=40,
            fg_color="#28A745",
            hover_color="#218838"
        ).pack(side="left", padx=10)
        
        ctk.CTkButton(
            buttons_frame,
            text="🔄 Usar Detección Automática",
            command=self.usar_deteccion_automatica,
            font=ctk.CTkFont(size=14, weight="bold"),
            width=250,
            height=40,
            fg_color="#17A2B8",
            hover_color="#138496"
        ).pack(side="left", padx=10)
        
        ctk.CTkButton(
            buttons_frame,
            text="❌ Cancelar",
            command=self.cancelar,
            font=ctk.CTkFont(size=14, weight="bold"),
            width=150,
            height=40,
            fg_color="#DC3545",
            hover_color="#C82333"
        ).pack(side="right", padx=10)
        
    def crear_fila_columna(self, col_limpio, info, problemas_dict):
        col_original = info['columna_original']
        tipo_detectado = info['tipo']
        
        # Frame para la fila
        row_frame = ctk.CTkFrame(self.scrollable_frame)
        row_frame.pack(fill="x", pady=2, padx=5)
        
        # Nombre de columna
        col_label = ctk.CTkLabel(
            row_frame, 
            text=col_original[:25] + "..." if len(col_original) > 25 else col_original,
            font=ctk.CTkFont(weight="bold"),
            width=200,
            anchor="w"
        )
        col_label.pack(side="left", padx=5)
        
        # Tipo detectado
        tipo_label = ctk.CTkLabel(
            row_frame,
            text=tipo_detectado,
            width=100,
            fg_color="#E8F4FD" if tipo_detectado != "TEXT" else "#FFF3CD",
            corner_radius=5
        )
        tipo_label.pack(side="left", padx=5)
        
        # Ejemplos de datos
        muestra = self.df[col_original].dropna().head(3).tolist()
        ejemplos_text = str(muestra)[:35] + "..." if len(str(muestra)) > 35 else str(muestra)
        ejemplos_label = ctk.CTkLabel(
            row_frame,
            text=ejemplos_text,
            width=250,
            font=ctk.CTkFont(size=10),
            anchor="w"
        )
        ejemplos_label.pack(side="left", padx=5)
        
        # Dropdown para seleccionar tipo
        self.dropdown_vars[col_limpio] = ctk.StringVar(value=tipo_detectado)
        
        # Determinar color según si hay problema
        dropdown_color = "#DC3545" if col_original in problemas_dict else "#28A745"
        
        dropdown = ctk.CTkComboBox(
            row_frame,
            values=self.tipos_sqlite_disponibles,
            variable=self.dropdown_vars[col_limpio],
            width=150,
            button_color=dropdown_color,
            border_color=dropdown_color
        )
        dropdown.pack(side="left", padx=5)
        
        # Indicador de problema
        if col_original in problemas_dict:
            problema = problemas_dict[col_original]
            problema_label = ctk.CTkLabel(
                row_frame,
                text=f"⚠️ {problema['problema'][:30]}...",
                width=200,
                font=ctk.CTkFont(size=9),
                text_color="#DC3545",
                anchor="w"
            )
            problema_label.pack(side="left", padx=5)
            
            # Sugerir tipo automáticamente si hay problema
            if problema['sugerencia']:
                self.dropdown_vars[col_limpio].set(problema['sugerencia'])
                dropdown.configure(button_color="#FD7E14", border_color="#FD7E14")
        else:
            problema_label = ctk.CTkLabel(
                row_frame,
                text="✅ OK",
                width=200,
                font=ctk.CTkFont(size=9),
                text_color="#28A745",
                anchor="w"
            )
            problema_label.pack(side="left", padx=5)
    
    def aplicar_correcciones(self):
        """Aplica las correcciones seleccionadas por el usuario"""
        tipos_corregidos = {}
        cambios_realizados = 0
        
        for col_limpio, info in self.esquema_inicial.items():
            nuevo_tipo = self.dropdown_vars[col_limpio].get()
            
            # Crear info corregida
            info_corregida = info.copy()
            info_corregida['tipo'] = nuevo_tipo
            info_corregida['es_fecha'] = nuevo_tipo in ['DATE', 'DATETIME']
            
            tipos_corregidos[col_limpio] = info_corregida
            
            # Contar cambios
            if nuevo_tipo != info['tipo']:
                cambios_realizados += 1
        
        self.tipos_corregidos = tipos_corregidos
        
        # Mostrar resumen
        mensaje = f"Correcciones aplicadas:\n\n"
        mensaje += f"📊 Total columnas: {len(self.esquema_inicial)}\n"
        mensaje += f"🔄 Cambios realizados: {cambios_realizados}\n\n"
        
        if cambios_realizados > 0:
            mensaje += "Cambios específicos:\n"
            for col_limpio, info_nueva in tipos_corregidos.items():
                info_original = self.esquema_inicial[col_limpio]
                if info_nueva['tipo'] != info_original['tipo']:
                    mensaje += f"• {info_nueva['columna_original']}: {info_original['tipo']} → {info_nueva['tipo']}\n"
        else:
            mensaje += "No se realizaron cambios."
        
        messagebox.showinfo("Correcciones Aplicadas", mensaje)
        
        # Cerrar ventana y devolver resultado
        self.ventana.destroy()
        self.callback_resultado(True, tipos_corregidos)
    
    def usar_deteccion_automatica(self):
        """Usa la detección automática para todos los problemas encontrados"""
        from processor import DataProcessor
        temp_processor = DataProcessor()
        problemas = temp_processor.detectar_problemas_tipos(self.df, self.esquema_inicial)
        
        cambios_automaticos = 0
        for problema in problemas:
            # Buscar la columna correspondiente
            for col_limpio, info in self.esquema_inicial.items():
                if info['columna_original'] == problema['columna']:
                    if problema['sugerencia']:
                        self.dropdown_vars[col_limpio].set(problema['sugerencia'])
                        cambios_automaticos += 1
                    break
        
        if cambios_automaticos > 0:
            messagebox.showinfo(
                "Detección Automática", 
                f"Se aplicaron {cambios_automaticos} correcciones automáticas.\n\n"
                "Revisa los cambios y haz clic en 'Aplicar Correcciones'."
            )
        else:
            messagebox.showinfo(
                "Detección Automática", 
                "No se detectaron problemas que requieran corrección automática."
            )
    
    def cancelar(self):
        """Cancela sin aplicar cambios"""
        respuesta = messagebox.askyesno(
            "Cancelar", 
            "¿Estás seguro de cancelar sin aplicar correcciones?\n\n"
            "Se usarán los tipos detectados automáticamente."
        )
        if respuesta:
            self.ventana.destroy()
            self.callback_resultado(False, self.esquema_inicial)
    
    def on_closing(self):
        """Maneja el evento de cerrar ventana con X"""
        respuesta = messagebox.askyesno(
            "Cerrar Ventana", 
            "¿Deseas cerrar la ventana de corrección de tipos?\n\n"
            "• SÍ: Continuar con tipos detectados automáticamente\n"
            "• NO: Mantener ventana abierta para hacer correcciones"
        )
        if respuesta:
            self.ventana.destroy()
            # Continuar con esquema automático (sin correcciones)
            self.callback_resultado(False, self.esquema_inicial)
