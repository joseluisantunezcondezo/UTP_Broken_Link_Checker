# 📚 UTP - Broken Link Checker

Aplicativo web en **Streamlit** para **detectar enlaces rotos** en documentos académicos y administrativos, reducir la revisión manual y generar un **reporte final en Excel** con estado **ACTIVO/ROTO**.

[![Python](https://img.shields.io/badge/Python-3.8%2B-blue.svg)](https://www.python.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-App-FF4B4B.svg)](https://streamlit.io/)
[![Estado](https://img.shields.io/badge/Estado-Producción-success.svg)]()
---

## Descripción general

**UTP - Broken Link Checker** automatiza un proceso que normalmente consume mucho tiempo:

1. Recibe un **Excel con URLs** de documentos.
2. Descarga automáticamente archivos compatibles.
3. Procesa documentos **ZIP, PDF, Word, PowerPoint, H5P y XLF/Rise**.
4. Extrae los enlaces detectados.
5. Valida si cada enlace sigue activo.
6. Entrega un **Excel final de status** para revisión y corrección.

Está pensado para equipos académicos, áreas de calidad, producción de contenidos y usuarios que necesitan verificar material digital antes de su publicación o distribución.

---

## ¿Qué problema resuelve?

En entornos académicos y documentales, los enlaces rotos generan:

- Mala experiencia del usuario final.
- Pérdida de tiempo en validaciones manuales.
- Materiales desactualizados.

---

El aplicativo permite:

- Cargar archivos desde la interfaz.
- Ejecutar el flujo sin programar.
- Visualizar progreso y estados.
- Descargar reportes finales listos para revisión.

## Módulos del aplicativo

La aplicación tiene **2 módulos principales**:

### 1. Home

Pantalla informativa que explica:

- Propósito del sistema.
- Cobertura de formatos.
- Funcionalidades principales.
- Flujo de trabajo.
- Lineamientos de seguridad y privacidad.

### 2. Report Broken Link

Módulo operativo principal. Aquí se ejecuta el pipeline completo de análisis.

---

## Flujo funcional del aplicativo

El flujo puede entenderse en **4 grandes fases**:

### Fase 1. Ingesta y descarga

- Se carga un **Excel** con columna `url`.
- El sistema identifica enlaces que terminan en formatos permitidos.
- Descarga automáticamente documentos **PDF, DOCX y PPTX**.
- Genera también un **CSV de fallidos** cuando alguna descarga no puede completarse.

### Fase 2. Carga documental

Además de la descarga masiva, el usuario puede cargar directamente:

- **PDF**
- **DOCX**
- **PPTX**
- **ZIP** con contenidos **H5P**
- **ZIP** con contenidos **XLF / Rise**

### Fase 3. Extracción y preparación

- Los **PDF** se transforman a **DOCX** para facilitar el análisis.
- Los **DOCX** y **PPTX** se recorren para detectar URLs visibles e hipervínculos incrustados.
- Los paquetes **H5P** y **Rise/XLF** se convierten a **TXT** mediante helpers especializados.
- Todos los resultados se consolidan en un **reporte de links detectados**.

### Fase 4. Validación y reporte

- Se normalizan enlaces.
- Se descartan formatos inválidos.
- Se consulta el estado HTTP de cada URL.
- Se aplican reglas de clasificación avanzada:
  - Enlaces activos
  - Rotos reales
  - Soft-404
  - Accesos restringidos
  - Errores transitorios
  - Dominios validados manualmente
- Finalmente se genera un **Excel Status** con el resultado final.

---

## Flujo 

```text
Excel con URLs
   ↓
Descarga automática de documentos
   ↓
Carga y procesamiento de archivos
   ↓
Extracción de links
   ↓
Validación automática de enlaces
   ↓
Excel final con estado ACTIVO / ROTO
```

---

## Arquitectura 

La arquitectura  en **5 capas**.

### 1. Capa de interfaz

Es la parte visible para el usuario.

Incluye:

- Sidebar, módulos, expanders, métricas, barras de progreso, chips de estado, botones de descarga, tablas de resultados.

### 2. Capa de orquestación

Controla el flujo del proceso usando `st.session_state`.

Esta capa decide:

- Qué pasos ya terminaron.
- Qué resultados deben reutilizarse.
- Cuándo reiniciar el pipeline.
- Qué archivos están disponibles en cada etapa.

### 3. Capa de ingesta y archivos

Se encarga de recibir archivos desde:

- Carga manual.
- Descargas masivas.
- ZIP con contenidos estructurados.

### 4. Capa de procesamiento documental

Transforma y analiza los documentos:

- PDF → DOCX.
- Lectura de Word.
- Lectura de PowerPoint.
- Extracción de texto desde H5P.
- Extracción de texto desde XLF / Rise.

### 5. Capa de validación y salida

Aplica la lógica de validación de enlaces y genera la salida final:

- Normalización de URLs.
- Validación estructural.
- Verificación HTTP.
- Heurísticas de soft-404.
- Whitelist institucional.
- Exportación del reporte en Excel.

---

## Arquitectura técnica

```mermaid
flowchart TD
    A[Interfaz Streamlit] --> B[Orquestación con session_state]
    B --> C[Ingesta de archivos]
    C --> D1[Descarga masiva desde Excel]
    C --> D2[Carga Directa
             (PDF-DOCX-PPTX-ZIP)]
    D1 --> E[Procesamiento documental]
    D2 --> E
    E --> F1[PDF a DOCX]
    E --> F2[Extracción de links DOCX]
    E --> F3[Extracción de links PPTX]
    E --> F4[Conversión H5P ZIP a TXT]
    E --> F5[Conversión Rise XLF ZIP a TXT]
    F1 --> G[Consolidación de links]
    F2 --> G
    F3 --> G
    F4 --> G
    F5 --> G
    G --> H[Validación HTTP y reglas de negocio]
    H --> I[Excel final Status]
```

---

## Archivos principales del proyecto

### `app.py`

Archivo principal del aplicativo en producción.

Responsabilidades:

- Configuración de Streamlit.
- Render de módulos.
- Control del flujo unificado.
- Persistencia de estado.
- Procesamiento de descargas.
- Extracción de links.
- Validación final.
- Exportación de reportes.


### `brokenCheck_h5p_helper.py`

Helper especializado para paquetes **H5P**.

Responsabilidades:

- Abrir ZIPs H5P.
- Detectar archivos `.h5p` y reportes Excel asociados.
- extraer texto útil desde JSON, HTML y TXT internos.
- Generar archivos `.txt` por contenido.
- Construir un **reporte H5P unificado**.
- Extraer links desde los TXT generados.

### `brokenCheck_rise_helper.py`

Helper especializado para paquetes **Rise / XLF / XLIFF / XML**.

Responsabilidades:

- Abrir ZIPs de Rise.
- Detectar archivos XLF/XLIFF/XML.
- Parsear contenido de etiquetas `source`, `target` y `seg-source`.
- Convertir contenido a TXT.
- Leer reportes Excel `reporte_rise_*`.
- Generar un **reporte Rise unificado**.
- Extraer links desde los TXT procesados.

---

## Componentes técnicos clave

### Gestión de estado

Funciones importantes:

- `init_session_state()`
- `reset_report_broken_pipeline()`

Su función es mantener el flujo estable y evitar reprocesos innecesarios entre pasos.

### Procesamiento de PDFs

Clase principal:

- `PDFBatchProcessor`

Permite:

- Procesar múltiples PDFs.
- Usar procesamiento paralelo por páginas.
- Convertir PDFs a DOCX.
- Preservar el texto para etapas posteriores.

### Extracción de links

Funciones relevantes:

- `_extract_links_from_docx_bytes()`
- `_extract_links_from_pptx_bytes()`
- `_extract_links_from_pdf_path()`
- `run_h5p_txt_link_report_streamlit()`
- `run_rise_txt_link_report_streamlit()`

### Validación de enlaces

Funciones relevantes:

- `_normalize_links()`
- `_check_one_url_robust_v5()`
- `_run_link_check_ultra_v5()`
- `_infer_tipo_problema()`
- `_standardize_status_column()`

### Exportación

Funciones relevantes:

- `_to_excel_report()`
- `_to_excel_reporte_links()`

---

## Formatos de entrada soportados

### Entrada principal

- Excel `.xlsx` / `.xls` con columna `url`

### Documentos

- `.pdf`
- `.docx`
- `.pptx`

### Paquetes comprimidos

- `.zip` con contenidos **H5P**.
- `.zip` con contenidos **Rise / XLF / XLIFF / XML**.
- `.zip` con colecciones de documentos compatibles.

---

## Archivos de salida

El sistema puede generar los siguientes artefactos:

- ZIP con documentos descargados.
- CSV de descargas fallidas.
- DOCX generados desde PDFs.
- TXT procesados desde H5P.
- TXT procesados desde Rise.
- Excel unificado H5P.
- Excel unificado Rise.
- Excel final **Status** con el estado de cada enlace.

---

## Estructura del reporte final

La hoja principal de salida consolida información como:

- `name`
- `Archivo`
- `Página/Diapositiva`
- `Link`
- `Status`
- `HTTP_Code`
- `Detalle`
- `Tipo_Problema`
- `link_class`
- `source_url`

Esto permite revisar no solo si el enlace está activo o roto, sino también **de dónde proviene** y **qué tipo de problema se detectó**.

---

## Lógica de validación destacada

El checker no se limita a revisar un código HTTP.

También incorpora lógica adicional para reducir falsos positivos:

- Validación de estructura de URL.
- Detección de enlaces truncados.
- Detección de **soft-404**.
- Tratamiento especial para dominios confiables.
- **lista blanca institucional** para URLs aprobadas manualmente.
- Tratamiento especial para `canvas.utp`, que se marca como roto por regla de negocio.

---

## Decisiones técnicas importantes del diseño

### Persistencia por sesión

La app usa `st.session_state` para:

- Evitar ejecuciones repetidas.
- Mantener archivos intermedios disponibles.
- Preservar resultados entre pasos.
- Soportar reinicio controlado del pipeline.

### Procesamiento por rutas en disco

En lugar de depender solo de objetos en memoria, la aplicación trabaja con **rutas temporales en disco** para mejorar estabilidad y manejo de archivos grandes.

## Requisitos técnicos

### Python

Se recomienda usar **Python 3.10 o superior**.

### Dependencias principales

- `streamlit`
- `pandas`
- `requests`
- `httpx`
- `pymupdf`
- `python-docx`
- `python-pptx`
- `openpyxl`

---

## Recomendaciones operativas

Para un uso más estable en producción:

- Procesar entre **500 y 700 URLs** por ejecución en Streamlit Cloud.
- Dividir archivos muy grandes en bloques cuando sea necesario.
- Validar dependencias instaladas antes de desplegar.

---


## Problemas frecuentes

### El Excel no procesa

Verifica que el archivo contenga la columna obligatoria `url`.

### No se descargan documentos

Revisa si las URLs terminan en formatos permitidos:

- `.pdf`
- `.doc`
- `.docx`
- `.ppt`
- `.pptx`

### Los ZIP H5P o Rise no generan resultados

Confirma que el ZIP contenga:

- Archivos válidos del paquete.

### El proceso consume mucha memoria en cloud

Divide el trabajo en lotes más pequeños o ejecuta el aplicativo en local.

### Un enlace parece válido pero figura como roto

Puede deberse a:

- Reglas institucionales.
- Contenido protegido por anti-bot.
- Soft-404.
- Redirecciones no válidas.
- Errores de contenido devuelto por el servidor.

---

## Autor

**José Luis Antúnez Condezo**

Proyecto orientado a automatización documental, validación de enlaces y mejora de calidad en contenidos académicos.
