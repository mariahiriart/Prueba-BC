# Agente de Seguimiento Operativo Brightcell / JCR

Sistema de monitoreo en tiempo real de rutas de distribución. Escucha mensajes de WhatsApp a través de webhooks de **Whapi**, parsea reportes de drivers y respuestas de back-office (BO), y genera un dashboard HTML actualizado en vivo con KPIs por ruta.

---

## Arquitectura general

```
WhatsApp (drivers / BO)
        │
        ▼
   Whapi.cloud  ──webhook──►  main_api.py (FastAPI)
                                    │
                    ┌───────────────┼────────────────────┐
                    ▼               ▼                    ▼
             database.py     image_utils.py       dashboard_YYYY_MM_DD/
           (PostgreSQL/Neon) (compresión imágenes)      data.js
                                    │
                              Claude Vision API
                          (extrae asignaciones driver↔ruta
                           desde imagen enviada por Roberto)
```

---

## Scripts

### Agente principal

| Archivo | Descripción |
|---|---|
| `main_api.py` | Servidor FastAPI. Recibe webhooks de Whapi, procesa mensajes en tiempo real, regenera el dashboard en cada mensaje. Punto de entrada principal. |
| `database.py` | Capa de acceso a PostgreSQL (Neon). Guarda mensajes crudos, reportes de driver y cierres de BO. |
| `image_utils.py` | Utilidad de compresión de imágenes (PIL). Reduce fotos de celular antes de procesarlas. |
| `silent_listener_v1_1.py` | Versión v1 del listener. Clasifica y persiste mensajes en BD sin generar dashboard. Útil como módulo de referencia. |

### Scripts de procesamiento offline

| Archivo | Descripción |
|---|---|
| `procesar_whapi_json.py` | Procesa un JSON exportado de mensajes de Whapi y genera `data.js` del dashboard para un día específico. |
| `procesar_whatsapp.py` | Lee mensajes desde la BD (tabla `raw_messages`) para una fecha dada y genera `data.js`. Uso: `python procesar_whatsapp.py 2026-03-21` |
| `generar_data_combined.py` | Genera el archivo `data_combined.js` que agrupa varios días para el dashboard histórico (18 marzo). |
| `generar_desde_json.py` | Genera `data.js` y `data_combined.js` a partir de un JSON local de WhatsApp exportado (formato legacy del 18 marzo). |
| `parse_routes_excel.py` | Parsea el Excel de rutas del día (enviado por Jacob) y devuelve lista de paradas con ID BOP, cliente, dirección, etc. |

### Utilidades de inspección

| Archivo | Descripción |
|---|---|
| `inspect_schema.py` | Muestra la estructura de tablas de la BD. |
| `inspect_types.py` | Inspecciona tipos de datos de columnas. |
| `inspect_constraints.py` | Lista constraints (PK, FK, NOT NULL) de las tablas. |
| `inspect_lengths.py` | Verifica longitudes máximas de campos de texto en la BD. |
| `inspect_reports.py` | Consulta y muestra los reportes guardados en `driver_reports`. |
| `inspect_whatsapp_groups.py` | Lista los grupos de WhatsApp registrados en la tabla `whatsapp_groups`. |
| `list_tables.py` | Lista todas las tablas de la BD con conteo de filas. |
| `verify_db.py` | Verifica la conexión y estado general de la BD. |
| `test_process.py` | Script de prueba para validar el procesamiento de mensajes de ejemplo. |

---

## Flujo de operación diaria

```
1. Jacob envía el Excel de rutas del día por WhatsApp
      → main_api.py lo descarga automáticamente y carga los BOPs

2. Roberto envía imagen con la tabla de asignación driver↔ruta
      → Claude Vision extrae los nombres y los guarda en driver_names.json

3. Los drivers reportan por WhatsApp con formato:
      ID BOP: XXXXXXX
      Estatus: Exitoso / Fallido
      Observaciones: ...

4. El BO responde con formato:
      IdBop: [ #XXXXXXX ]
      Estatus: Entregado / No entregado
      Motivo: ...

5. Cada mensaje actualiza el dashboard en vivo:
      http://localhost:8000/dashboard → data.js (JSON del día)
```

---

## Instalación

```bash
# 1. Clonar
git clone https://github.com/mihiriart/Desktop.git
cd Desktop/documentacion

# 2. Instalar dependencias
pip install fastapi uvicorn openpyxl pillow anthropic psycopg2-binary python-dotenv

# 3. Configurar variables de entorno
cp .env.example .env
# Editar .env con los valores reales

# 4. Levantar el servidor
python main_api.py
```

El servidor queda escuchando en `http://0.0.0.0:8000`.
Para exponerlo a internet (webhook de Whapi) se usa **ngrok**:

```bash
ngrok http 8000
# Configurar la URL pública en Whapi como webhook
```

---

## Variables de entorno

Ver `.env.example`. Las requeridas son:

| Variable | Descripción |
|---|---|
| `WHAPI_TOKEN` | Token de autenticación de Whapi |
| `ANTHROPIC_API_KEY` | API key de Anthropic (para Claude Vision) |
| `DB_HOST` | Host de PostgreSQL (Neon) |
| `DB_PORT` | Puerto (default: 5432) |
| `DB_NAME` | Nombre de la base de datos |
| `DB_USER` | Usuario de la BD |
| `DB_PASSWORD` | Contraseña de la BD |

---

## Endpoints de la API

| Método | Ruta | Descripción |
|---|---|---|
| `GET` | `/` | Health check, estado del día en curso |
| `GET` | `/status` | KPIs actuales regenerados |
| `GET` | `/api/data` | Estado completo en JSON (sin leer disco) |
| `POST` | `/webhook/whatsapp` | Receptor de webhooks de Whapi |
| `GET` | `/dashboard` | Dashboard HTML del día en vivo |

---

## Estructura del dashboard generado

Por cada día de operación se genera un directorio `dashboard_YYYY_MM_DD/` con:

```
dashboard_2026_03_23/
├── index.html     # Dashboard HTML
└── data.js        # Datos JSON del día (const dashboardData = {...})
```

El `data.js` contiene:
- `kpis`: totales asignados, reportados, sin reporte, exitosos, fallidos, % éxito
- `rutas`: resumen por ruta con driver, BOPs asignados vs reportados
- `detalle_reportados`: detalle BOP a BOP con status del driver, respuesta del BO y multimedia

---

## Base de datos (PostgreSQL / Neon)

Tablas principales:

| Tabla | Descripción |
|---|---|
| `whatsapp_groups` | Grupos de WhatsApp autorizados |
| `raw_messages` | Todos los mensajes recibidos (crudos) |
| `driver_reports` | Reportes parseados de drivers |
| `bo_closures` | Cierres de BO parseados |
