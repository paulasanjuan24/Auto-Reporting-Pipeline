# Auto-Reporting Pipeline (Email → Google Sheets → Power BI)

Automatización completa de reportes desde Gmail a Google Sheets y Power BI, con validación de datos, idempotencia y notificaciones en Slack.

---

## Descripción

Este proyecto implementa un **pipeline ETL** end-to-end:

1. **Extract**  
   - Conexión a Gmail API.  
   - Descarga de adjuntos (.csv / .xlsx).  
   - Control de duplicados mediante **hash SHA-256** almacenados en SQLite.

2. **Transform**  
   - Normalización de nombres de columnas.  
   - Conversión de tipos (fechas, numéricos, strings).  
   - **Validación de calidad** con Pandera:  
     - Ventas, Leads, Inventario, Finanzas.  
     - Reglas de negocio (ej: `total = cantidad * precio_unitario`).  
   - Datos inválidos → se aíslan en pestaña `invalid`.

3. **Load**  
   - Exportación a `combined.csv` y `combined.xlsx`.  
   - Publicación en Google Sheets (pestañas `raw_clean` y `summary`).  
   - Integración con Power BI para dashboards.

4. **Operación**  
   - Logs estructurados con rotación.  
   - Notificaciones vía Slack (Éxito, Advertencia, Error).  
   - CLI (`python -m src.cli run-now`) y servidor HTTP (`python -m src.server`).  
   - Orquestación con Make/n8n a través de webhooks (expuesto con ngrok).  

---

## Arquitectura

```mermaid
flowchart LR
    Gmail[Gmail API] --> Fetch[fetch_gmail.py]
    Fetch --> Process[process_reports.py\nLimpieza + Validación]
    Process --> Files[combined.csv/xlsx]
    Process --> Sheets[Google Sheets]
    Sheets --> BI[Power BI]
    Process --> Notify[Slack/Telegram]
    CLI[CLI / Scheduler / Make] --> Fetch
