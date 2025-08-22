# interface.py
"""
🎨 INTERFAZ DE USUARIO ETL
Ventana principal con control dinámico de expansión
"""
import customtkinter as ctk
from tkinter import filedialog, messagebox, ttk
import threading
import pandas as pd
from processor import DataProcessor

class ETLInterface(ctk.CTk):
    """Interfaz principal de la aplicación ETL con control dinámico"""
    
    def __init__(self):
        super().__init__()
        
        # Configuración ventana
        self.title("🚀 ETL Visual - SQLite")
        self.geometry("1200x800")
        self.minsize(1000, 700)
        
        # Variables de estado
        self.processor = DataProcessor()
        self.archivo_actual = None
        self.cargando = False
        self.preview_expandida = False  # Control de expansión
        
        # Crear interfaz con scroll
        self.crear_interfaz_con_scroll()
    
    def crear_interfaz_con_scroll(self):
        """Crea la interfaz con barras de desplazamiento"""
        
        # Canvas principal con scrollbars
        self.canvas = ctk.CTkCanvas(self, highlightthickness=0)
        self.scrollbar_y = ttk.Scrollbar(self, orient="vertical", command=self.canvas.yview)
        self.scrollbar_x = ttk.Scrollbar(self, orient="horizontal", command=self.canvas.xview)
        
        # Frame principal que contendrá todo
        self.main_frame = ctk.CTkFrame(self.canvas)
        
        # Configurar canvas
        self.canvas.configure(
            yscrollcommand=self.scrollbar_y.set,
            xscrollcommand=self.scrollbar_x.set
        )
        
        # Layout con scrollbars
        self.scrollbar_y.pack(side="right", fill="y")
        self.scrollbar_x.pack(side="bottom", fill="x")
        self.canvas.pack(side="left", fill="both", expand=True)
        
        # Crear ventana en el canvas
        self.canvas_window = self.canvas.create_window((0, 0), window=self.main_frame, anchor="nw")
        
        # Bind eventos para scroll
        self.main_frame.bind("<Configure>", self.on_frame_configure)
        self.canvas.bind("<Configure>", self.on_canvas_configure)
        self.bind_mousewheel(self.canvas)
        
        # Configurar grid para distribución dinámica
        self.main_frame.grid_rowconfigure(0, weight=0)  # Título
        self.main_frame.grid_rowconfigure(1, weight=0)  # Selección archivo
        self.main_frame.grid_rowconfigure(2, weight=2)  # Vista previa (dinámico)
        self.main_frame.grid_rowconfigure(3, weight=1)  # Esquema detectado
        self.main_frame.grid_rowconfigure(4, weight=1)  # Configuración
        self.main_frame.grid_rowconfigure(5, weight=0)  # Botones carga
        self.main_frame.grid_rowconfigure(6, weight=0)  # Controles adicionales
        self.main_frame.grid_columnconfigure(0, weight=1)
        
        # Crear secciones
        self.crear_contenido()
    
    def on_frame_configure(self, event=None):
        """Actualiza la región scrollable"""
        self.canvas.configure(scrollregion=self.canvas.bbox("all"))
        
    def on_canvas_configure(self, event):
        """Actualiza el tamaño de la ventana del canvas"""
        width = max(self.main_frame.winfo_reqwidth(), event.width)
        self.canvas.itemconfig(self.canvas_window, width=width)
        
    def bind_mousewheel(self, widget):
        """Vincula eventos de la rueda del mouse"""
        widget.bind("<MouseWheel>", self.on_mousewheel)
        widget.bind("<Shift-MouseWheel>", self.on_shift_mousewheel)
        
    def on_mousewheel(self, event):
        """Scroll vertical"""
        self.canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
        
    def on_shift_mousewheel(self, event):
        """Scroll horizontal"""
        self.canvas.xview_scroll(int(-1 * (event.delta / 120)), "units")
    
    def crear_contenido(self):
        """Crea todo el contenido de la interfaz"""
        
        # TÍTULO PRINCIPAL
        titulo_frame = ctk.CTkFrame(self.main_frame)
        titulo_frame.grid(row=0, column=0, sticky="ew", padx=10, pady=(10, 5))
        
        titulo = ctk.CTkLabel(
            titulo_frame,
            text="🚀 ETL Visual - Carga de Datos a SQLite",
            font=("Segoe UI", 24, "bold"),
            text_color="#2C3E50"
        )
        titulo.pack(pady=20)
        
        # SECCIONES
        self.crear_seccion_archivo()
        self.crear_seccion_preview()
        self.crear_seccion_esquema()
        self.crear_seccion_config()
        self.crear_seccion_carga()
        self.crear_seccion_controles()
    
    def crear_seccion_archivo(self):
        """Crea la sección de selección de archivo"""
        frame_seccion = ctk.CTkFrame(self.main_frame)
        frame_seccion.grid(row=1, column=0, sticky="ew", padx=10, pady=5)
        frame_seccion.grid_columnconfigure(0, weight=1)
        
        titulo = ctk.CTkLabel(
            frame_seccion,
            text="📁 Selección de Archivo",
            font=("Segoe UI", 16, "bold"),
            text_color="#2C3E50"
        )
        titulo.pack(anchor="w", padx=15, pady=(15, 5))
        
        controles_frame = ctk.CTkFrame(frame_seccion)
        controles_frame.pack(fill="x", padx=15, pady=(0, 15))
        controles_frame.grid_columnconfigure(1, weight=1)
        
        self.btn_archivo = ctk.CTkButton(
            controles_frame,
            text="Seleccionar archivo",
            command=self.seleccionar_archivo,
            fg_color="#3A7CA5",
            hover_color="#2C3E50",
            width=150,
            font=("Segoe UI", 12, "bold")
        )
        self.btn_archivo.grid(row=0, column=0, padx=(10, 20), pady=10)
        
        self.lbl_archivo = ctk.CTkLabel(
            controles_frame,
            text="No se ha seleccionado archivo",
            font=("Segoe UI", 11),
            anchor="w"
        )
        self.lbl_archivo.grid(row=0, column=1, sticky="w", padx=5, pady=10)
    
    def crear_seccion_preview(self):
        """Crea la sección de vista previa CON CONTROL DINÁMICO"""
        self.frame_preview = ctk.CTkFrame(self.main_frame)
        self.frame_preview.grid(row=2, column=0, sticky="nsew", padx=10, pady=5)
        self.frame_preview.grid_columnconfigure(0, weight=1)
        self.frame_preview.grid_rowconfigure(2, weight=1)
        
        # Header con controles dinámicos
        header_frame = ctk.CTkFrame(self.frame_preview, fg_color="transparent")
        header_frame.grid(row=0, column=0, sticky="ew", padx=15, pady=(15, 5))
        header_frame.grid_columnconfigure(0, weight=1)
        
        # Título a la izquierda
        titulo = ctk.CTkLabel(
            header_frame,
            text="👁️ Vista Previa de Datos",
            font=("Segoe UI", 16, "bold"),
            text_color="#2C3E50"
        )
        titulo.grid(row=0, column=0, sticky="w")
        
        # Controles dinámicos a la derecha
        controles_frame = ctk.CTkFrame(header_frame, fg_color="transparent")
        controles_frame.grid(row=0, column=1, sticky="e")
        
        # Botón expandir/contraer
        self.btn_expandir = ctk.CTkButton(
            controles_frame,
            text="🔽 Expandir",
            command=self.toggle_preview_size,
            width=100,
            height=28,
            font=("Segoe UI", 10),
            fg_color="#17A2B8",
            hover_color="#138496"
        )
        self.btn_expandir.grid(row=0, column=0, padx=(0, 5))
        
        # Botón ajustar columnas
        self.btn_ajustar = ctk.CTkButton(
            controles_frame,
            text="📐 Ajustar",
            command=self.ajustar_columnas,
            width=80,
            height=28,
            font=("Segoe UI", 10),
            fg_color="#6C757D",
            hover_color="#5A6268"
        )
        self.btn_ajustar.grid(row=0, column=1)
        
        # Info de datos
        info_frame = ctk.CTkFrame(self.frame_preview, fg_color="transparent")
        info_frame.grid(row=1, column=0, sticky="ew", padx=15, pady=(0, 5))
        
        self.lbl_preview_info = ctk.CTkLabel(
            info_frame,
            text="Seleccione un archivo para ver los datos",
            font=("Segoe UI", 10),
            text_color="#666666"
        )
        self.lbl_preview_info.pack()
        
        # Contenedor de tabla CON ALTURA DINÁMICA
        self.tabla_container = ctk.CTkFrame(self.frame_preview, fg_color="transparent")
        self.tabla_container.grid(row=2, column=0, sticky="nsew", padx=15, pady=(0, 15))
        self.tabla_container.grid_rowconfigure(0, weight=1)
        self.tabla_container.grid_columnconfigure(0, weight=1)
        
        # ALTURAS DINÁMICAS
        self.altura_contraida = 250
        self.altura_expandida = 450
        self.tabla_container.configure(height=self.altura_contraida)
        self.tabla_container.grid_propagate(False)  # Altura fija controlada
        
        # Estilos
        self.style = ttk.Style()
        self.style.theme_use("default")
        
        self.style.configure("Treeview",
            font=("Segoe UI", 9),
            rowheight=25,
            background="#FFFFFF",
            fieldbackground="#FFFFFF",
            foreground="#2C3E50",
            borderwidth=1,
            relief="solid"
        )
        
        self.style.configure("Treeview.Heading",
            font=("Segoe UI", 10, "bold"),
            background="#3A7CA5",
            foreground="#FFFFFF",
            borderwidth=0,
            relief="flat",
            padding=(8, 6)
        )
        
        # Tabla principal
        self.tabla = ttk.Treeview(
            self.tabla_container,
            show="headings",
            selectmode="browse",
            style="Treeview"
        )
        self.tabla.grid(row=0, column=0, sticky="nsew")
        
        # Scrollbars
        scroll_v = ttk.Scrollbar(self.tabla_container, orient="vertical", command=self.tabla.yview)
        scroll_v.grid(row=0, column=1, sticky="ns")
        
        scroll_h = ttk.Scrollbar(self.tabla_container, orient="horizontal", command=self.tabla.xview)
        scroll_h.grid(row=1, column=0, sticky="ew")
        
        self.tabla.configure(
            yscrollcommand=scroll_v.set,
            xscrollcommand=scroll_h.set
        )
    
    def toggle_preview_size(self):
        """🔄 FUNCIÓN CLAVE: Controla expansión dinámica"""
        if self.preview_expandida:
            # CONTRAER
            self.tabla_container.configure(height=self.altura_contraida)
            self.btn_expandir.configure(text="🔽 Expandir")
            self.preview_expandida = False
            
            # Ajustar pesos del grid principal
            self.main_frame.grid_rowconfigure(2, weight=2)  # Vista previa normal
            self.main_frame.grid_rowconfigure(3, weight=1)  # Esquema visible
            
            info_text = f"Vista contraída - {self.altura_contraida}px"
            
        else:
            # EXPANDIR
            self.tabla_container.configure(height=self.altura_expandida)
            self.btn_expandir.configure(text="🔼 Contraer")
            self.preview_expandida = True
            
            # Dar más espacio a la vista previa
            self.main_frame.grid_rowconfigure(2, weight=4)  # Vista previa grande
            self.main_frame.grid_rowconfigure(3, weight=0)  # Esquema pequeño
            
            info_text = f"Vista expandida - {self.altura_expandida}px"
        
        # Actualizar interfaz
        self.update_idletasks()
        self.lbl_preview_info.configure(text=f"{info_text} - Use botón 'Ajustar' para columnas")
    
    def ajustar_columnas(self):
        """📐 FUNCIÓN CLAVE: Ajuste inteligente de columnas"""
        if not hasattr(self, 'tabla') or not self.tabla['columns']:
            messagebox.showwarning("Aviso", "No hay datos cargados para ajustar")
            return
        
        try:
            self.tabla_container.update_idletasks()
            ancho_disponible = self.tabla_container.winfo_width() - 40
            
            columnas = self.tabla['columns']
            num_columnas = len(columnas)
            
            # ESTRATEGIAS INTELIGENTES
            if num_columnas <= 4:
                ancho_col = max(150, ancho_disponible // num_columnas)
                estrategia = "Distribución completa"
            elif num_columnas <= 8:
                ancho_col = 160
                estrategia = "Ancho cómodo"
            else:
                ancho_col = 120
                estrategia = "Ancho compacto"
            
            # Aplicar ajuste
            for col in columnas:
                self.tabla.column(col, width=ancho_col, minwidth=80)
            
            # Información actualizada
            ancho_total = ancho_col * num_columnas
            self.lbl_preview_info.configure(
                text=f"📐 {estrategia}: {num_columnas} columnas × {ancho_col}px = {ancho_total:,}px"
            )
            
            messagebox.showinfo(
                "Columnas Ajustadas", 
                f"✅ {estrategia}\n📊 {num_columnas} columnas × {ancho_col}px\n💡 Use scroll horizontal para navegar"
            )
            
        except Exception as e:
            messagebox.showerror("Error", f"Error al ajustar:\n{str(e)}")
    
    def crear_seccion_esquema(self):
        """Crea la sección que muestra el esquema detectado"""
        frame_seccion = ctk.CTkFrame(self.main_frame)
        frame_seccion.grid(row=3, column=0, sticky="nsew", padx=10, pady=5)
        frame_seccion.grid_columnconfigure(0, weight=1)
        frame_seccion.grid_rowconfigure(1, weight=1)
        
        titulo = ctk.CTkLabel(
            frame_seccion,
            text="🔍 Esquema Detectado",
            font=("Segoe UI", 16, "bold"),
            text_color="#2C3E50"
        )
        titulo.grid(row=0, column=0, sticky="w", padx=15, pady=(15, 5))
        
        esquema_container = ctk.CTkFrame(frame_seccion, fg_color="transparent")
        esquema_container.grid(row=1, column=0, sticky="nsew", padx=15, pady=(0, 15))
        esquema_container.grid_rowconfigure(0, weight=1)
        esquema_container.grid_columnconfigure(0, weight=1)
        
        self.tabla_esquema = ttk.Treeview(
            esquema_container,
            columns=("tipo", "ejemplo", "formato"),
            show="tree headings",
            selectmode="browse",
            height=6
        )
        self.tabla_esquema.grid(row=0, column=0, sticky="nsew")
        
        self.tabla_esquema.heading("#0", text="Columna", anchor="w")
        self.tabla_esquema.heading("tipo", text="Tipo SQLite", anchor="w")
        self.tabla_esquema.heading("ejemplo", text="Ejemplo", anchor="w")
        self.tabla_esquema.heading("formato", text="Formato", anchor="w")
        
        self.tabla_esquema.column("#0", width=200, minwidth=150)
        self.tabla_esquema.column("tipo", width=120, minwidth=100)
        self.tabla_esquema.column("ejemplo", width=200, minwidth=150)
        self.tabla_esquema.column("formato", width=150, minwidth=120)
        
        scroll_esquema = ttk.Scrollbar(esquema_container, orient="vertical", command=self.tabla_esquema.yview)
        scroll_esquema.grid(row=0, column=1, sticky="ns")
        self.tabla_esquema.configure(yscrollcommand=scroll_esquema.set)
    
    def crear_seccion_config(self):
        """Crea la sección de configuración"""
        frame_seccion = ctk.CTkFrame(self.main_frame)
        frame_seccion.grid(row=4, column=0, sticky="ew", padx=10, pady=5)
        frame_seccion.grid_columnconfigure(0, weight=1)
        
        titulo = ctk.CTkLabel(
            frame_seccion,
            text="⚙️ Configuración de Carga",
            font=("Segoe UI", 16, "bold"),
            text_color="#2C3E50"
        )
        titulo.pack(anchor="w", padx=15, pady=(15, 5))
        
        opciones_frame = ctk.CTkFrame(frame_seccion)
        opciones_frame.pack(fill="x", padx=15, pady=(0, 15))
        
        opciones_frame.grid_columnconfigure(0, weight=0)
        opciones_frame.grid_columnconfigure(1, weight=1)
        opciones_frame.grid_columnconfigure(2, weight=0)
        opciones_frame.grid_columnconfigure(3, weight=1)
        
        # Modo BD
        ctk.CTkLabel(opciones_frame, text="Modo BD:", font=("Segoe UI", 11, "bold")).grid(
            row=0, column=0, sticky="w", padx=10, pady=10
        )
        
        radio_frame = ctk.CTkFrame(opciones_frame)
        radio_frame.grid(row=0, column=1, columnspan=2, sticky="w", padx=10, pady=10)
        
        self.radio_var_bd = ctk.StringVar(value="nueva")
        
        self.radio_nueva_bd = ctk.CTkRadioButton(
            radio_frame,
            text="Crear nueva",
            variable=self.radio_var_bd,
            value="nueva",
            command=self.toggle_bd_mode
        )
        self.radio_nueva_bd.pack(side="left", padx=(10, 20))
        
        self.radio_existente_bd = ctk.CTkRadioButton(
            radio_frame,
            text="Usar existente",
            variable=self.radio_var_bd,
            value="existente",
            command=self.toggle_bd_mode
        )
        self.radio_existente_bd.pack(side="left")
        
        self.btn_seleccionar_bd = ctk.CTkButton(
            opciones_frame,
            text="📂 Seleccionar",
            command=self.seleccionar_bd_existente,
            state="disabled",
            width=100
        )
        self.btn_seleccionar_bd.grid(row=0, column=3, sticky="w", padx=10, pady=10)
        
        # BD y Tabla
        ctk.CTkLabel(opciones_frame, text="Base datos:", font=("Segoe UI", 11)).grid(
            row=1, column=0, sticky="w", padx=10, pady=10
        )
        self.entry_bd = ctk.CTkEntry(opciones_frame, placeholder_text="mi_base (sin .db)", width=250)
        self.entry_bd.grid(row=1, column=1, sticky="ew", padx=10, pady=10)
        
        ctk.CTkLabel(opciones_frame, text="Tabla:", font=("Segoe UI", 11)).grid(
            row=1, column=2, sticky="w", padx=10, pady=10
        )
        self.entry_tabla = ctk.CTkEntry(opciones_frame, placeholder_text="Auto-generado", width=200)
        self.entry_tabla.grid(row=1, column=3, sticky="ew", padx=10, pady=10)
        
        # Lote
        ctk.CTkLabel(opciones_frame, text="Lote:", font=("Segoe UI", 11)).grid(
            row=2, column=0, sticky="w", padx=10, pady=10
        )
        self.combo_chunk = ctk.CTkComboBox(opciones_frame, values=["1000", "2500", "5000", "10000"], width=120)
        self.combo_chunk.set("1000")
        self.combo_chunk.grid(row=2, column=1, sticky="w", padx=10, pady=10)
    
    def crear_seccion_carga(self):
        """Crea la sección de carga"""
        frame_seccion = ctk.CTkFrame(self.main_frame)
        frame_seccion.grid(row=5, column=0, sticky="ew", padx=10, pady=5)
        frame_seccion.grid_columnconfigure(0, weight=1)
        
        titulo = ctk.CTkLabel(
            frame_seccion,
            text="🚀 Carga de Datos",
            font=("Segoe UI", 16, "bold"),
            text_color="#2C3E50"
        )
        titulo.pack(anchor="w", padx=15, pady=(15, 5))
        
        progreso_container = ctk.CTkFrame(frame_seccion)
        progreso_container.pack(fill="x", padx=15, pady=(0, 10))
        
        self.progreso = ctk.CTkProgressBar(progreso_container, width=500, height=20)
        self.progreso.pack(fill="x", padx=10, pady=10)
        self.progreso.set(0)
        
        self.lbl_estado = ctk.CTkLabel(progreso_container, text="Listo para cargar", font=("Segoe UI", 11))
        self.lbl_estado.pack(padx=10, pady=(0, 10))
        
        botones_frame = ctk.CTkFrame(frame_seccion)
        botones_frame.pack(fill="x", padx=15, pady=(0, 15))
        botones_frame.grid_columnconfigure((0, 1), weight=1)
        
        self.btn_cargar = ctk.CTkButton(
            botones_frame,
            text="🚀 Iniciar Carga",
            command=self.iniciar_carga,
            state="disabled",
            width=200,
            height=45,
            font=("Segoe UI", 14, "bold"),
            fg_color="#28A745",
            hover_color="#218838"
        )
        self.btn_cargar.grid(row=0, column=0, padx=10, pady=10)
        
        self.btn_cancelar = ctk.CTkButton(
            botones_frame,
            text="❌ Cancelar",
            command=self.cancelar_carga,
            state="disabled",
            width=150,
            height=45,
            font=("Segoe UI", 14, "bold"),
            fg_color="#DC3545",
            hover_color="#C82333"
        )
        self.btn_cancelar.grid(row=0, column=1, padx=10, pady=10)
    
    def crear_seccion_controles(self):
        """Crea la sección de controles adicionales"""
        frame_seccion = ctk.CTkFrame(self.main_frame)
        frame_seccion.grid(row=6, column=0, sticky="ew", padx=10, pady=(5, 15))
        frame_seccion.grid_columnconfigure(0, weight=1)
        
        titulo = ctk.CTkLabel(
            frame_seccion,
            text="🔄 Controles Adicionales",
            font=("Segoe UI", 16, "bold"),
            text_color="#2C3E50"
        )
        titulo.pack(anchor="w", padx=15, pady=(15, 5))
        
        botones_frame = ctk.CTkFrame(frame_seccion)
        botones_frame.pack(fill="x", padx=15, pady=(0, 15))
        botones_frame.grid_columnconfigure((0, 1, 2), weight=1)
        
        self.btn_limpiar = ctk.CTkButton(
            botones_frame,
            text="🧹 Limpiar",
            command=self.limpiar_programa,
            width=150,
            height=40,
            fg_color="#17A2B8",
            hover_color="#138496"
        )
        self.btn_limpiar.grid(row=0, column=0, padx=10, pady=10)
        
        self.btn_ver_tablas = ctk.CTkButton(
            botones_frame,
            text="📊 Ver Tablas",
            command=self.ver_tablas_bd,
            width=150,
            height=40,
            fg_color="#6F42C1",
            hover_color="#5A2D91"
        )
        self.btn_ver_tablas.grid(row=0, column=1, padx=10, pady=10)
        
        self.btn_abrir_carpeta = ctk.CTkButton(
            botones_frame,
            text="📂 Abrir Carpeta",
            command=self.abrir_carpeta_bd,
            width=150,
            height=40,
            fg_color="#FD7E14",
            hover_color="#E8640F"
        )
        self.btn_abrir_carpeta.grid(row=0, column=2, padx=10, pady=10)
    
    # FUNCIONES DE CONTROL
    def toggle_bd_mode(self):
        """Cambia entre modo nueva BD y BD existente"""
        if self.radio_var_bd.get() == "existente":
            self.btn_seleccionar_bd.configure(state="normal")
            self.entry_bd.configure(state="disabled")
        else:
            self.btn_seleccionar_bd.configure(state="disabled")
            self.entry_bd.configure(state="normal")
    
    def seleccionar_bd_existente(self):
        """Selecciona una base de datos existente"""
        archivo_bd = filedialog.askopenfilename(
            title="Seleccionar base de datos SQLite",
            filetypes=[("Archivos SQLite", "*.db *.sqlite *.sqlite3"), ("Todos", "*.*")]
        )
        if archivo_bd:
            self.entry_bd.configure(state="normal")
            self.entry_bd.delete(0, "end")
            self.entry_bd.insert(0, archivo_bd)
            self.entry_bd.configure(state="disabled")
    
    def seleccionar_archivo(self):
        """Maneja la selección de archivo"""
        archivo = filedialog.askopenfilename(
            title="Seleccionar archivo de datos",
            filetypes=[
                ("Archivos Excel", "*.xlsx *.xls"),
                ("Archivos CSV", "*.csv"),
                ("Todos", "*.*")
            ]
        )
        
        if archivo:
            try:
                self.archivo_actual = archivo
                nombre_archivo = archivo.split('/')[-1].split('\\')[-1]
                self.lbl_archivo.configure(text=f"Archivo: {nombre_archivo}")
                
                # Cargar vista previa Y esquema
                df = self.processor.cargar_preview(archivo)
                self.actualizar_tabla(df)
                self.actualizar_esquema(df)
                
                # Auto-completar nombre tabla
                nombre_tabla = nombre_archivo.split('.')[0].replace('-', '_').replace(' ', '_')
                self.entry_tabla.delete(0, "end")
                self.entry_tabla.insert(0, nombre_tabla)
                
                # Habilitar carga
                self.btn_cargar.configure(state="normal")
                
            except Exception as e:
                messagebox.showerror("Error", f"Error al cargar archivo:\n{str(e)}")
    
    def actualizar_tabla(self, df):
        """Actualiza la tabla con formato uniforme"""
        # Limpiar
        for item in self.tabla.get_children():
            self.tabla.delete(item)
        
        if df is not None and not df.empty:
            columnas = list(df.columns)
            self.tabla["columns"] = columnas
            self.tabla["show"] = "headings"
            
            # Configurar columnas inteligentemente
            num_cols = len(columnas)
            if num_cols <= 5:
                col_width = 150
            elif num_cols <= 10:
                col_width = 120
            else:
                col_width = 100
            
            for col in columnas:
                self.tabla.heading(col, text=str(col))
                self.tabla.column(col, width=col_width, minwidth=80, stretch=True, anchor='w')
            
            # Insertar datos con formato especial para nulos
            for i, (_, fila) in enumerate(df.iterrows()):
                valores_formateados = []
                for val in fila:
                    if val is None or pd.isna(val):
                        valores_formateados.append("(vacío)")
                    else:
                        valores_formateados.append(str(val))
                
                tag = 'evenrow' if i % 2 == 0 else 'oddrow'
                self.tabla.insert("", "end", values=valores_formateados, tags=(tag,))
            
            # Actualizar info
            self.lbl_preview_info.configure(
                text=f"📊 {len(df)} filas × {len(columnas)} columnas - Use 'Expandir' y 'Ajustar' para mejor vista"
            )
    
    def actualizar_esquema(self, df):
        """Actualiza la tabla de esquema"""
        for item in self.tabla_esquema.get_children():
            self.tabla_esquema.delete(item)
        
        if df is not None and not df.empty:
            try:
                esquema = self.processor.obtener_esquema_tabla(df)
                
                for col_limpio, info in esquema.items():
                    if info['es_fecha']:
                        formato = 'Fecha ISO (YYYY-MM-DD)'
                        tag = 'fecha'
                    elif info['tipo'] in ['INTEGER', 'REAL']:
                        formato = 'Número'
                        tag = 'numero'
                    else:
                        formato = 'Texto'
                        tag = 'texto'
                    
                    ejemplo = str(info['ejemplo']) if info['ejemplo'] is not None else "(vacío)"
                    
                    self.tabla_esquema.insert(
                        "", "end",
                        text=info['columna_original'],
                        values=(info['tipo'], ejemplo, formato)
                    )
            except Exception as e:
                self.tabla_esquema.insert("", "end", text="Error", values=("N/A", str(e)[:50], "N/A"))
    
    def iniciar_carga(self):
        """Inicia el proceso de carga"""
        bd = self.entry_bd.get().strip() or "datos"
        # Asegurar que la base de datos tenga extensión .db
        if not bd.lower().endswith(('.db', '.sqlite', '.sqlite3')):
            bd += '.db'
        tabla = self.entry_tabla.get().strip()
        
        if not tabla:
            messagebox.showwarning("Advertencia", "Debe ingresar un nombre de tabla")
            return
        
        self.cargando = True
        self.btn_cargar.configure(state="disabled", text="⏳ Cargando...")
        self.btn_cancelar.configure(state="normal")
        self.progreso.set(0)
        self.lbl_estado.configure(text="Iniciando carga...")
        
        thread = threading.Thread(
            target=self.processor.procesar_archivo,
            args=(self.archivo_actual, bd, tabla, self.callback_progreso, self.callback_completado)
        )
        thread.daemon = True
        thread.start()
    
    def callback_progreso(self, progreso, mensaje):
        """Callback progreso"""
        self.after(0, lambda: self.progreso.set(progreso))
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
    
    def cancelar_carga(self):
        """Cancela la carga"""
        self.cargando = False
        self.processor.cancelar()
        self.lbl_estado.configure(text="Cancelando...")
    
    def restablecer_ui(self):
        """Restablece la interfaz"""
        self.btn_cargar.configure(state="normal", text="🚀 Iniciar Carga")
        self.btn_cancelar.configure(state="disabled")
    
    def limpiar_programa(self):
        """Limpia todos los datos"""
        respuesta = messagebox.askyesno("Confirmar", "¿Limpiar todos los datos?")
        if respuesta:
            self.archivo_actual = None
            self.cargando = False
            
            self.lbl_archivo.configure(text="No se ha seleccionado archivo")
            self.entry_tabla.delete(0, "end")
            self.entry_bd.configure(state="normal")
            self.entry_bd.delete(0, "end")
            self.entry_bd.insert(0, "datos")
            
            self.radio_var_bd.set("nueva")
            self.toggle_bd_mode()
            
            for item in self.tabla.get_children():
                self.tabla.delete(item)
            for item in self.tabla_esquema.get_children():
                self.tabla_esquema.delete(item)
            
            self.progreso.set(0)
            self.lbl_estado.configure(text="Listo para cargar")
            self.lbl_preview_info.configure(text="Seleccione un archivo para ver los datos")
            
            self.btn_cargar.configure(state="disabled", text="🚀 Iniciar Carga")
            self.btn_cancelar.configure(state="disabled")
            
            messagebox.showinfo("Limpieza", "✅ Programa reiniciado")
    
    def ver_tablas_bd(self):
        """Muestra tablas en BD"""
        bd_path = self.entry_bd.get().strip() or "datos"
        # Asegurar que la base de datos tenga extensión .db
        if not bd_path.lower().endswith(('.db', '.sqlite', '.sqlite3')):
            bd_path += '.db'
        try:
            tablas = self.processor.obtener_tablas_bd(bd_path)
            if tablas:
                lista = "\n".join([f"• {tabla}" for tabla in tablas])
                messagebox.showinfo(f"Tablas en {bd_path}", f"📊 Tablas:\n\n{lista}")
            else:
                messagebox.showinfo(f"BD {bd_path}", "📭 No hay tablas")
        except Exception as e:
            messagebox.showerror("Error", f"Error:\n{str(e)}")
    
    def abrir_carpeta_bd(self):
        """Abre carpeta de BD"""
        import os, subprocess
        bd_path = self.entry_bd.get().strip() or "datos"
        # Asegurar que la base de datos tenga extensión .db
        if not bd_path.lower().endswith(('.db', '.sqlite', '.sqlite3')):
            bd_path += '.db'
        try:
            directorio = os.path.dirname(os.path.abspath(bd_path)) if not os.path.isabs(bd_path) else os.path.dirname(bd_path)
            subprocess.run(['explorer', directorio])
        except Exception as e:
            messagebox.showerror("Error", f"Error:\n{str(e)}")
