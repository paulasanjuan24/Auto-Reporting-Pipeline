# src/process_reports.py
'''Leer, unificar y exportar
- Cargamos CSV/XLSX de la carpeta descargada.
- Añadimos columna __source_file (de dónde viene cada fila).
- Unimos todo en un solo DataFrame.
- Creamos un resumen básico y exportamos a combined.csv y combined.xlsx.

- Normalizamos nombres de columnas (minúsculas, sin acentos, _ en vez de espacios).
- Estandarizamos nombres comunes (sinónimos) a un nombre canónico.
- Detectamos el tipo de dataset (ventas, leads, inventario, finanzas, genérico).
- Coercemos tipos (fechas, numéricos, etc.) y validamos conesquemas Pandera (si aplica).
- Exportamos datos limpios y resumen a CSV y XLSX (varias pestañas).
- Mantenemos un df de inválidos (con errores de validación) para revisión.
- Usamos Pandera para validación avanzada (chequeos, reportes de errores).
- Documentamos cada paso y función.
- Logueamos errores y pasos importantes.
- Todo configurable y extensible (fácil añadir nuevos tipos o sinónimos).
- Buenas prácticas: funciones pequeñas, typing, docstrings, logging.
- Tests unitarios (ver tests/test_process_reports.py).
'''

from __future__ import annotations
import pathlib
import unicodedata
from typing import List, Tuple, Optional

import pandas as pd
import pandera.pandas as pa

from .config import settings
from .logger import get_logger

log = get_logger()

# Directorios para guardar outputs
DATA_DIR = settings.data_dir
PROCESSED_DIR = settings.processed_dir
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

# -----------------------------
# 1) Utilidades de normalización
# -----------------------------
'''
Limpia nombres de columnas para que sean consistentes:
sin acentos, minúsculas, _ en vez de espacios, etc.
Por qué: los reportes cambian “Fecha” / “FECHA” / “fecha_pedido”. Normalizar evita errores y simplifica reglas.
Ejemplo: "Fecha Pedido" → fechapedido → renombrado a canónico fecha.
'''
def _strip_accents(s: str) -> str:
    # Normaliza acentos y eñes -> ascii simple
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join(c for c in nfkd if not unicodedata.combining(c))

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Devuelve un df con nombres de columnas normalizados:
    - quita espacios laterales
    - pasa a minúsculas
    - reemplaza espacios/guiones por '_'
    - elimina acentos/ñ -> n
    """
    new_cols = []
    for c in df.columns:
        c2 = str(c).strip().lower()
        c2 = _strip_accents(c2)
        c2 = c2.replace(" ", "_").replace("-", "_")
        c2 = c2.replace("__", "_")
        new_cols.append(c2)
    df = df.copy()
    df.columns = new_cols
    return df

# ------------------------------------
# 2) Detección + estandarización por tipo
# ------------------------------------
'''
Por las columnas presentes, decide si el archivo parece de ventas, leads, inventario, finanzas o genérico.
Por qué: cada tipo tiene reglas distintas. Ventas no se valida igual que Leads.
Ejemplo: si ve fecha, producto, cantidad → lo marca como ventas.
'''
# Mapeos de sinónimos -> nombre canónico
SYNONYMS = {
    "fecha": {"fecha", "fechapedido", "fecha_pedido", "date"},
    "producto": {"producto", "item", "articulo"},
    "cantidad": {"cantidad", "qty", "unidades"},
    "precio_unitario": {"precio_unitario", "preciounitario", "precio", "precio_unit", "unit_price"},
    "total": {"total", "importe", "monto_total"},
    "campana": {"campana", "campana_marketing", "campaign"},
    "leads": {"leads", "prospectos"},
    "conversiones": {"conversiones", "conversions"},
    "id": {"id", "codigo", "code"},
    "stock": {"stock", "existencias"},
    "almacen": {"almacen", "almacn", "deposito", "bodega"},
    "pais": {"pais", "country"},
    "categoria": {"categoria", "category"},
    "tipo": {"tipo", "type"},
    "monto": {"monto", "importe", "amount"},
}

def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Renombra columnas conocidas a su forma canónica (sin acentos y con '_').
    """
    rename_map = {}
    cols = set(df.columns)
    for canonical, variants in SYNONYMS.items():
        for v in variants:
            if v in cols:
                rename_map[v] = canonical
    if rename_map:
        df = df.rename(columns=rename_map)
    return df

def detect_dataset_type(df: pd.DataFrame) -> str:
    """
    Heurística simple por columnas:
    - ventas: fecha + producto + cantidad (y/o total)
    - leads: fecha + campana + leads
    - inventario: producto + stock (y/o almacen)
    - finanzas: fecha + categoria + monto (+ tipo)
    Si nada cuadra -> 'generico'
    """
    c = set(df.columns)

    def has(*cols): return all(col in c for col in cols)

    if (("fecha" in c and "producto" in c and ("cantidad" in c or "total" in c)) or
        ("fechapedido" in c and "producto" in c)):
        return "ventas"
    if has("fecha", "campana") and ("leads" in c or "conversiones" in c):
        return "leads"
    if ("producto" in c and "stock" in c) or ("stock" in c and "almacen" in c):
        return "inventario"
    if "fecha" in c and "monto" in c:
        return "finanzas"
    return "generico"

# --------------------------------
# 3) Conversión de tipos por dataset
# --------------------------------
'''
Intenta convertir: fechas → datetime, numéricos → float/int, columnas especiales (p. ej., tipo de finanzas a ingreso/gasto en minúsculas)
Por qué: si llegan “30,00” o “30.00” o “30€”, Pandas necesita “entender” los números.
Ejemplo: si total no existe pero hay cantidad y precio_unitario, se calcula.
'''
def coerce_common(df: pd.DataFrame) -> pd.DataFrame:
    """Conversión genérica: intenta parsear 'fecha*' y números conocidos."""
    df = df.copy()
    # Fechas genéricas
    for col in [c for c in df.columns if c.startswith("fecha")]:
        df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)
    # Numéricos comunes
    numeric_candidates = {"cantidad", "precio_unitario", "total", "leads", "conversiones", "stock", "monto"}
    for col in df.columns:
        if col in numeric_candidates:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    # Normaliza 'tipo' en finanzas: ingreso/gasto (lower)
    if "tipo" in df.columns:
        df["tipo"] = df["tipo"].astype(str).str.strip().str.lower()
    return df

def coerce_by_type(df: pd.DataFrame, dtype: str) -> pd.DataFrame:
    """
    Ajustes más específicos por tipo de dataset.
    - ventas: si falta 'total' y hay 'cantidad' y 'precio_unitario' -> calcular
    """
    df = df.copy()
    if dtype == "ventas":
        # Homogeneiza nombre 'fecha'
        if "fechapedido" in df.columns and "fecha" not in df.columns:
            df["fecha"] = df["fechapedido"]
        if "total" not in df.columns and all(col in df.columns for col in ["cantidad", "precio_unitario"]):
            df["total"] = df["cantidad"] * df["precio_unitario"]
    return df

# -------------------------------
# 4) Esquemas de validación (Pandera)
# -------------------------------
'''
Define esquemas por tipo:
- Ventas: fecha válida, cantidad >= 0, total >= 0 y (si hay) total≈cantidad*precio_unitario.
- Leads: fecha, leads >= 0, etc.
- Inventario: stock >= 0.
- Finanzas: tipo ∈ {ingreso, gasto}, monto >= 0.
Por qué: evita que datos corruptos entren al dashboard.
Qué pasa si falla: ese archivo se aparta a invalid (no contamina el dataset limpio). El log lo indica.
'''
VentasSchema = pa.DataFrameSchema(
    {
        "fecha": pa.Column(pa.DateTime, nullable=False),
        "producto": pa.Column(pa.String, nullable=False),
        "cantidad": pa.Column(pa.Int, checks=pa.Check.ge(0), nullable=True),
        "precio_unitario": pa.Column(pa.Float, checks=pa.Check.ge(0), nullable=True),
        "total": pa.Column(pa.Float, checks=pa.Check.ge(0), nullable=True),
    },
    # Check de consistencia: si existen cantidad y precio_unitario, total ≈ cantidad*precio_unitario
    checks=[
        pa.Check(
            lambda d: (
                # condición válida cuando falte alguna de las columnas
                (("cantidad" not in d.columns) | ("precio_unitario" not in d.columns) | ("total" not in d.columns)) |
                # o cuando existen, que total sea cercano al producto
                ((d["cantidad"] * d["precio_unitario"] - d["total"]).abs() < 1e-6) |
                # permitir NaN en alguna de las tres
                (d["cantidad"].isna()) | (d["precio_unitario"].isna()) | (d["total"].isna())
            ),
            error="total debe ser cantidad*precio_unitario cuando ambas existan"
        )
    ],
    strict=False  # permitir columnas extra
)

LeadsSchema = pa.DataFrameSchema(
    {
        "fecha": pa.Column(pa.DateTime, nullable=False),
        "campana": pa.Column(pa.String, nullable=False),
        "leads": pa.Column(pa.Int, checks=pa.Check.ge(0), nullable=True),
        "conversiones": pa.Column(pa.Int, checks=pa.Check.ge(0), nullable=True),
    },
    strict=False
)

InventarioSchema = pa.DataFrameSchema(
    {
        "id": pa.Column(pa.String, nullable=True),
        "producto": pa.Column(pa.String, nullable=False),
        "stock": pa.Column(pa.Int, checks=pa.Check.ge(0)),
        "almacen": pa.Column(pa.String, nullable=True),
    },
    strict=False
)

FinanzasSchema = pa.DataFrameSchema(
    {
        "fecha": pa.Column(pa.DateTime, nullable=False),
        "categoria": pa.Column(pa.String, nullable=False),
        "tipo": pa.Column(pa.String, checks=pa.Check.isin({"ingreso", "gasto"})),
        "monto": pa.Column(pa.Float, checks=pa.Check.ge(0)),
    },
    strict=False
)

def _schema_for(dtype: str) -> Optional[pa.DataFrameSchema]:
    if dtype == "ventas": return VentasSchema
    if dtype == "leads": return LeadsSchema
    if dtype == "inventario": return InventarioSchema
    if dtype == "finanzas": return FinanzasSchema
    return None  # generico: sin esquema estricto


# ---------------------------------------
# 5) Carga simple (igual que versión base)
# ---------------------------------------

def load_any(path: pathlib.Path) -> pd.DataFrame:
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    elif path.suffix.lower() in (".xlsx", ".xls"):
        return pd.read_excel(path)
    else:
        raise ValueError(f"Formato no soportado: {path.suffix}")

def combine_files(files: List[pathlib.Path]) -> pd.DataFrame:
    """
    Une todos los adjuntos en un solo DataFrame crudo (raw), añadiendo __source_file.
    No valida ni limpia: eso se hace luego por archivo.
    """
    dfs = []
    for f in files:
        try:
            df = load_any(f)
            df["__source_file"] = f.name
            dfs.append(df)
        except Exception as e:
            log.error(f"Error leyendo {f.name}: {e}")
    if not dfs:
        return pd.DataFrame()
    raw = pd.concat(dfs, ignore_index=True)
    return raw

# -----------------------------------------
# 6) Limpieza + validación por archivo fuente
# -----------------------------------------

def _process_single_file(df_file: pd.DataFrame, source_name: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Procesa un único archivo:
    - normaliza nombres, estandariza sinónimos
    - detección de tipo
    - coerción de tipos (fechas, numéricos, etc.)
    - validación con pandera (si aplica)
    Devuelve: (valid_rows, invalid_rows)
    """
    if df_file.empty:
        return df_file, pd.DataFrame()

    # Normaliza y estandariza columnas
    df = normalize_columns(df_file)
    df = standardize_columns(df)

    # Detecta el tipo y aplica conversión
    dtype = detect_dataset_type(df)
    df = coerce_common(df)
    df = coerce_by_type(df, dtype)

    # Adjunta metadatos
    df["__dataset_type"] = dtype
    df["__validation_ok"] = True

    schema = _schema_for(dtype)
    if schema is None:
        # 'generico': no validamos estricto
        return df, pd.DataFrame()

    # Validación
    try:
        df_valid = schema.validate(df, lazy=True)
        return df_valid, pd.DataFrame()
    except pa.errors.SchemaErrors as err:
        # Extrae filas problemáticas (tabla de fallos)
        failure = err.failure_cases.copy()
        # failure típicamente tiene: schema_context, column, check, failure_case, index
        failure["__source_file"] = source_name
        failure["__dataset_type"] = dtype

        # Marcamos todo el df como invalidado para trazabilidad
        try:
            df_invalid = df.copy()
            df_invalid["__validation_ok"] = False
            df_invalid["__validation_error_rows"] = len(failure.index.unique())
            # Resumen textual de problemas más comunes (hasta 3 ejemplos por check)
            top_err = (
                failure.groupby(["column", "check"])["failure_case"]
                .apply(lambda s: ", ".join(map(str, s.head(3))))
                .reset_index()
            )
            df_invalid["__validation_summary"] = "; ".join(
                f"{r['column']}→{r['check']} (ej: {r['failure_case']})" for _, r in top_err.iterrows()
            )
        except Exception:
            df_invalid = df.assign(__validation_ok=False)

        return pd.DataFrame(), df_invalid

def clean_and_validate(raw_combined: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Separa por archivo de origen y procesa cada uno.
    Devuelve:
      - df_clean: unión de todos los archivos que PASARON validación
      - df_invalid: unión de archivos (completos) que FALLARON validación
    """
    if raw_combined.empty:
        return raw_combined, pd.DataFrame()

    clean_parts = []
    invalid_parts = []

    for source, part in raw_combined.groupby("__source_file", dropna=False):
        valid, invalid = _process_single_file(part, source)
        if not valid.empty:
            clean_parts.append(valid)
        if not invalid.empty:
            invalid_parts.append(invalid)

    df_clean = pd.concat(clean_parts, ignore_index=True) if clean_parts else pd.DataFrame()
    df_invalid = pd.concat(invalid_parts, ignore_index=True) if invalid_parts else pd.DataFrame()
    return df_clean, df_invalid

# --------------------------
# 7) Resúmenes y exportación
# --------------------------

def basic_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Resumen: filas por archivo y por tipo detectado (si existe)."""
    if df.empty:
        return df
    cols = ["__source_file"]
    if "__dataset_type" in df.columns:
        cols.append("__dataset_type")
    by_file = df.groupby(cols).size().reset_index(name="rows")
    return by_file

def export_outputs(df_clean: pd.DataFrame,
                   df_invalid: Optional[pd.DataFrame] = None) -> tuple[pathlib.Path, pathlib.Path]:
    """
    Exporta:
      - CSV 'combined.csv' con datos limpios
      - XLSX 'combined.xlsx' con:
          - raw_clean (datos limpios)
          - summary (resumen por archivo/tipo)
          - invalid (si hay inválidos)
    """
    csv_path = PROCESSED_DIR / "combined.csv"
    xlsx_path = PROCESSED_DIR / "combined.xlsx"

    # CSV limpio
    (df_clean if not df_clean.empty else pd.DataFrame()).to_csv(csv_path, index=False)

    # Excel con varias pestañas
    with pd.ExcelWriter(xlsx_path) as writer:
        (df_clean if not df_clean.empty else pd.DataFrame()).to_excel(writer, index=False, sheet_name="raw_clean")
        basic_summary(df_clean).to_excel(writer, index=False, sheet_name="summary")
        if df_invalid is not None and not df_invalid.empty:
            df_invalid.to_excel(writer, index=False, sheet_name="invalid")

    return csv_path, xlsx_path