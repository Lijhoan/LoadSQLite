# interface_simple_fixed.py
"""
🎨 INTERFAZ DE USUARIO ETL - VERSION SIMPLIFICADA
Ventana principal con interfaz gráfica SOLO para corrección de tipos
"""
import customtkinter as ctk
from tkinter import filedialog, messagebox, ttk
import threading
import pandas as pd
from processor import DataProcessor

class ETLInterface(ctk.CTk):
    """Interfaz principal de la aplicación ETL con corrección gráfica"""
    
    def __init__(self):
        super().__init__()
        
        # Configuración ventana
        self.title("🚀 ETL Visual - Carga de Datos a SQLite")
        self.geometry("1400x900")
        
        # Variables
        self.archivo_actual = None
        self.cargando = False
        self.preview_expandida = False
        
        # Processor
        self.processor = DataProcessor()
        
        # UI
        self.setup_ui()
        
    def setup_ui(self):
        # Header
        header_frame = ctk.CTkFrame(self)
        header_frame.pack(fill="x", padx=20, pady=10)
        
        title_label = ctk.CTkLabel(
            header_frame, 
            text="🚀 ETL Visual - Carga Excel/CSV a SQLite", 
            font=ctk.CTkFont(size=20, weight="bold")
        )
        title_label.pack(pady=10)
        
        # 1. ARCHIVO
        archivo_frame = ctk.CTkFrame(self)
        archivo_frame.pack(fill="x", padx=20, pady=10)
        
        ctk.CTkLabel(archivo_frame, text="📂 SELECCIÓN DE ARCHIVO", font=("Segoe UI", 14, "bold")).pack(anchor="w", padx=10, pady=5)
        
        seleccion_frame = ctk.CTkFrame(archivo_frame)
        seleccion_frame.pack(fill="x", padx=10, pady=5)
        
        self.lbl_archivo = ctk.CTkLabel(seleccion_frame, text="No se ha seleccionado archivo", font=("Segoe UI", 11))
        self.lbl_archivo.pack(side="left", padx=10, pady=10)
        
        ctk.CTkButton(seleccion_frame, text="📁 Seleccionar Archivo", command=self.seleccionar_archivo, width=200).pack(side="right", padx=10, pady=10)
        
        # 2. OPCIONES
        opciones_frame = ctk.CTkFrame(self)
        opciones_frame.pack(fill="x", padx=20, pady=10)
        
        ctk.CTkLabel(opciones_frame, text="⚙️ CONFIGURACIÓN", font=("Segoe UI", 14, "bold")).pack(anchor="w", padx=10, pady=5)
        
        config_frame = ctk.CTkFrame(opciones_frame)
        config_frame.pack(fill="x", padx=10, pady=5)
        config_frame.grid_columnconfigure(1, weight=1)
        config_frame.grid_columnconfigure(3, weight=1)
        
        # BD y Tabla
        ctk.CTkLabel(config_frame, text="Base datos:", font=("Segoe UI", 11)).grid(row=1, column=0, sticky="w", padx=10, pady=10)
        self.entry_bd = ctk.CTkEntry(config_frame, placeholder_text="mi_base (sin .db)", width=250)
        self.entry_bd.grid(row=1, column=1, sticky="ew", padx=10, pady=10)
        
        ctk.CTkLabel(config_frame, text="Tabla:", font=("Segoe UI", 11)).grid(row=1, column=2, sticky="w", padx=10, pady=10)
        self.entry_tabla = ctk.CTkEntry(config_frame, placeholder_text="Auto-generado", width=200)
        self.entry_tabla.grid(row=1, column=3, sticky="ew", padx=10, pady=10)
        
        # 3. PREVIEW
        preview_frame = ctk.CTkFrame(self)
        preview_frame.pack(fill="both", expand=True, padx=20, pady=10)
        
        preview_header = ctk.CTkFrame(preview_frame)
        preview_header.pack(fill="x", padx=10, pady=5)
        
        ctk.CTkLabel(preview_header, text="👀 VISTA PREVIA", font=("Segoe UI", 14, "bold")).pack(side="left", padx=10, pady=5)
        self.lbl_preview_info = ctk.CTkLabel(preview_header, text="Seleccione un archivo para ver los datos", font=("Segoe UI", 10))
        self.lbl_preview_info.pack(side="right", padx=10, pady=5)
        
        # Tabla preview
        self.tabla_preview = ttk.Treeview(preview_frame, show="headings", height=8)
        self.tabla_preview.pack(fill="both", expand=True, padx=10, pady=5)
        
        # Scrollbars preview
        scroll_y_preview = ttk.Scrollbar(preview_frame, orient="vertical", command=self.tabla_preview.yview)
        scroll_x_preview = ttk.Scrollbar(preview_frame, orient="horizontal", command=self.tabla_preview.xview)
        self.tabla_preview.configure(yscrollcommand=scroll_y_preview.set, xscrollcommand=scroll_x_preview.set)
        
        # 4. ACCIONES
        acciones_frame = ctk.CTkFrame(self)
        acciones_frame.pack(fill="x", padx=20, pady=10)
        
        ctk.CTkLabel(acciones_frame, text="🚀 CARGA DE DATOS", font=("Segoe UI", 14, "bold")).pack(anchor="w", padx=10, pady=5)
        
        botones_frame = ctk.CTkFrame(acciones_frame)
        botones_frame.pack(fill="x", padx=10, pady=5)
        
        self.btn_cargar = ctk.CTkButton(botones_frame, text="🚀 Iniciar Carga", command=self.iniciar_carga, width=200, height=40, 
                                       font=("Segoe UI", 14, "bold"), state="disabled")
        self.btn_cargar.pack(side="left", padx=10, pady=10)
        
        self.btn_cancelar = ctk.CTkButton(botones_frame, text="⏹ Cancelar", command=self.cancelar_carga, width=150, height=40, 
                                         state="disabled", fg_color="#DC3545", hover_color="#C82333")
        self.btn_cancelar.pack(side="left", padx=10, pady=10)
        
        # 5. PROGRESO
        progreso_frame = ctk.CTkFrame(acciones_frame)
        progreso_frame.pack(fill="x", padx=10, pady=5)
        
        self.progreso = ctk.CTkProgressBar(progreso_frame, width=400, height=20)
        self.progreso.pack(side="left", padx=10, pady=10)
        self.progreso.set(0)
        
        self.lbl_estado = ctk.CTkLabel(progreso_frame, text="Seleccione un archivo para comenzar", font=("Segoe UI", 11))
        self.lbl_estado.pack(side="left", padx=10, pady=10)
        
    def seleccionar_archivo(self):
        """Selecciona archivo Excel/CSV"""
        archivo = filedialog.askopenfilename(
            title="Seleccionar archivo Excel/CSV",
            filetypes=[("Archivos Excel", "*.xlsx *.xls"), ("Archivos CSV", "*.csv"), ("Todos", "*.*")]
        )
        
        if archivo:
            self.archivo_actual = archivo
            self.lbl_archivo.configure(text=f"📄 {archivo.split('/')[-1]}")
            
            # Auto-generar nombre de tabla
            nombre_tabla = archivo.split('/')[-1].split('.')[0].lower().replace(' ', '_')
            self.entry_tabla.delete(0, "end")
            self.entry_tabla.insert(0, nombre_tabla)
            
            # Cargar preview
            self.cargar_preview()
            self.btn_cargar.configure(state="normal")
            
    def cargar_preview(self):
        """Carga vista previa del archivo"""
        try:
            df_preview = self.processor.cargar_preview(self.archivo_actual)
            
            # Limpiar tabla
            for item in self.tabla_preview.get_children():
                self.tabla_preview.delete(item)
            
            # Configurar columnas
            columnas = list(df_preview.columns)
            self.tabla_preview["columns"] = columnas
            
            for col in columnas:
                self.tabla_preview.heading(col, text=col)
                self.tabla_preview.column(col, width=120, minwidth=80)
            
            # Insertar datos
            for index, row in df_preview.iterrows():
                valores = [str(val) if val is not None else "" for val in row]
                self.tabla_preview.insert("", "end", values=valores)
            
            self.lbl_preview_info.configure(text=f"Vista previa: {len(df_preview)} filas x {len(columnas)} columnas")
            
        except Exception as e:
            messagebox.showerror("Error", f"Error al cargar vista previa:\n{str(e)}")
    
    def iniciar_carga(self):
        """Inicia el proceso de carga con interfaz gráfica SIEMPRE"""
        bd = self.entry_bd.get().strip() or "datos"
        if not bd.lower().endswith(('.db', '.sqlite', '.sqlite3')):
            bd += '.db'
        tabla = self.entry_tabla.get().strip()
        
        if not tabla:
            messagebox.showwarning("Advertencia", "Debe ingresar un nombre de tabla")
            return
        
        # NUEVO: Solo preguntar si quiere corregir tipos o usar automático
        usar_interfaz_grafica = messagebox.askyesno(
            "Corrección de Tipos de Datos", 
            "¿Deseas revisar y corregir los tipos de datos?\n\n"
            "✅ SÍ: Se abrirá ventana gráfica para revisar todas las columnas\n"
            "❌ NO: Usar tipos detectados automáticamente\n\n"
            "🎯 La ventana gráfica te permitirá:\n"
            "• Ver todas las columnas y sus ejemplos\n"
            "• Cambiar tipos con listas desplegables\n"
            "• Ver problemas detectados automáticamente\n"
            "• Aplicar correcciones antes de cargar\n\n"
            "💡 RECOMENDADO: SÍ para control total"
        )
        
        self.cargando = True
        self.btn_cargar.configure(state="disabled", text="⏳ Cargando...")
        self.btn_cancelar.configure(state="normal")
        self.progreso.set(0)
        self.lbl_estado.configure(text="Iniciando carga...")
        
        # Usar interfaz gráfica si el usuario dijo que sí
        if usar_interfaz_grafica:
            self.abrir_correccion_tipos_antes_de_cargar(bd, tabla)
        else:
            # Cargar directamente sin corrección
            self.ejecutar_carga_final(bd, tabla, None)
    
    def abrir_correccion_tipos_antes_de_cargar(self, bd, tabla):
        """Abre ventana de corrección de tipos ANTES de la carga"""
        try:
            self.lbl_estado.configure(text="🔍 Analizando tipos de datos...")
            
            # Cargar datos completos para análisis
            if self.archivo_actual.lower().endswith('.csv'):
                df_completo = pd.read_csv(self.archivo_actual)
            else:
                df_completo = pd.read_excel(self.archivo_actual)
            
            # Generar esquema inicial
            esquema_inicial = self.processor.obtener_esquema_tabla(df_completo)
            
            # Función callback para cuando el usuario termine la corrección
            def callback_correccion(aplicar_cambios, esquema_resultado):
                if aplicar_cambios:
                    # Aplicar correcciones y cargar
                    self.ejecutar_carga_final(bd, tabla, esquema_resultado)
                else:
                    # Usar esquema original
                    self.ejecutar_carga_final(bd, tabla, esquema_inicial)
            
            # Abrir ventana de corrección
            self.lbl_estado.configure(text="🖥️ Abriendo ventana de corrección...")
            from correccion_tipos import VentanaCorreccionTipos
            VentanaCorreccionTipos(self, df_completo, esquema_inicial, callback_correccion)
            
        except Exception as e:
            messagebox.showerror("Error", f"Error al abrir corrección de tipos:\n{str(e)}")
            self.restablecer_ui()
    
    def ejecutar_carga_final(self, bd, tabla, esquema_corregido):
        """Ejecuta la carga final con o sin correcciones"""
        thread = threading.Thread(
            target=self.processor.procesar_archivo,
            args=(
                self.archivo_actual, 
                bd, 
                tabla, 
                self.callback_progreso, 
                self.callback_completado,
                esquema_corregido  # Pasar esquema corregido o None
            )
        )
        thread.daemon = True
        thread.start()
    
    def cancelar_carga(self):
        """Cancela la carga en proceso"""
        if self.cargando:
            self.processor.cancelar()
            self.restablecer_ui()
            messagebox.showinfo("Cancelado", "Carga cancelada por el usuario")
    
    def callback_progreso(self, valor, mensaje):
        """Callback para actualizar progreso"""
        self.after(0, lambda: self.progreso.set(valor))
        self.after(0, lambda: self.lbl_estado.configure(text=mensaje))
    
    def callback_completado(self, exito, mensaje, total_filas=0):
        """Callback completado"""
        self.cargando = False
        self.after(0, lambda: self.restablecer_ui())
        
        if exito:
            self.after(0, lambda: self.progreso.set(1.0))
            self.after(0, lambda: self.lbl_estado.configure(text=f"¡Completado! {total_filas:,} filas"))
            # Mostrar el nombre de BD con extensión correcta
            bd_nombre = self.entry_bd.get().strip() or "datos"
            if not bd_nombre.lower().endswith(('.db', '.sqlite', '.sqlite3')):
                bd_nombre += '.db'
            mensaje_exito = f"Carga exitosa!\n\nFilas: {total_filas:,}\nTabla: {self.entry_tabla.get()}\nBD: {bd_nombre}"
            self.after(0, lambda: messagebox.showinfo("Éxito", mensaje_exito))
        else:
            self.after(0, lambda: self.lbl_estado.configure(text=f"Error: {mensaje}"))
            self.after(0, lambda: messagebox.showerror("Error", f"Error:\n{mensaje}"))
    
    def restablecer_ui(self):
        """Restablece UI después de carga"""
        self.btn_cargar.configure(state="normal" if self.archivo_actual else "disabled", text="🚀 Iniciar Carga")
        self.btn_cancelar.configure(state="disabled")
        self.progreso.set(0)
