# -*- coding: utf-8 -*-
"""
Cargar CSVs a MySQL (catering_ops) en orden de dependencias.
Archivos esperados (carpeta ./data):
  empleados_preview.csv
  productos_preview.csv
  lotes_preview.csv
  vuelos_preview.csv
  ventas_preview.csv
  inventario_mov_preview.csv
  (opcional) vuelos_empleados_preview.csv

Autor: tú 😎
"""

import os
import sys
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text

# =========================
# 1) CONFIGURACIÓN MySQL
# =========================
USER = "hacker"                 # <--- ajusta
PASSWORD = "hfeuibewuibefoif"   # <--- ajusta
HOST = "64.23.179.210"          # <--- ajusta (o 127.0.0.1 si local)
PORT = 3306
DATABASE = "hackmty"            # <--- ajusta (o catering_ops si así la creaste)

# Carpeta con CSVs
DATA_DIR = Path("data")

# Parámetros de inserción
CHUNKSIZE = 1000
METHOD = "multi"

# =========================
# 2) CONEXIÓN
# =========================
def get_engine():
    uri = f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
    return create_engine(uri, pool_pre_ping=True)

# =========================
# 3) UTILIDADES
# =========================
def read_csv_smart(path: Path, parse_dates=None):
    """Lee CSV con pandas y hace strip a los headers. parse_dates = lista de columnas fecha a parsear."""
    if not path.exists():
        raise FileNotFoundError(f"No se encontró el archivo: {path}")
    kwargs = {}
    if parse_dates:
        kwargs["parse_dates"] = parse_dates
    df = pd.read_csv(path, **kwargs)
    df.columns = [str(c).strip() for c in df.columns]
    # Normalizar NaN -> None para respetar NULL en DB
    df = df.where(pd.notna(df), None)
    return df

def insert_df(df: pd.DataFrame, table: str, engine):
    """Inserta un DataFrame en la tabla dada (append)."""
    if df.empty:
        print(f"⚠️  {table}: DataFrame vacío, se omite inserción.")
        return
    df.to_sql(table, con=engine, if_exists="append", index=False, method=METHOD, chunksize=CHUNKSIZE)
    print(f"✅ {table}: Insertados {len(df)} registros.")

def count_rows(table: str, engine):
    with engine.connect() as conn:
        res = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
        return res.scalar()

# =========================
# 4) PLAN DE CARGA
# =========================
# Archivos -> tabla destino -> columnas fecha a parsear
PLAN = [
    # tablas sin FKs entrantes
    ("empleados_preview.csv",         "empleados",       []),
    ("productos_preview.csv",         "productos",       []),

    # depende de productos
    ("lotes_preview.csv",             "lotes",           ["fecha_prod", "fecha_cad"]),

    # independiente
    ("vuelos_preview.csv",            "vuelos",          ["fecha_vuelo"]),

    # depende de vuelos y productos (lot_id puede ir NULL)
    ("ventas_preview.csv",            "ventas",          []),

    # depende de lotes/productos; flight_id y empleado_id pueden ser NULL
    ("inventario_mov_preview.csv",    "inventario_mov",  ["fecha_mov"]),

    # depende de vuelos y empleados (si lo tienes)
    ("vuelos_empleados_preview.csv",  "vuelos_empleados", []),
]

# =========================
# 5) EJECUCIÓN
# =========================
def main():
    print("🔌 Conectando a MySQL...")
    engine = get_engine()
    try:
        with engine.connect() as conn:
            res = conn.execute(text("SELECT NOW()"))
            print("   Conexión OK. Hora del servidor:", res.scalar())
    except Exception as e:
        print("❌ No se pudo conectar a MySQL:", e)
        sys.exit(1)

    # --- OPCIONAL: limpiar tablas antes de insertar (respeta FKs) ---
    # Descomenta si quieres vaciar y recargar desde cero.
    # OJO: el orden importa para FKs; desactivamos checks mientras limpiamos y reactivamos después.
    """
    print("🧹 Limpiando tablas (opcional)...")
    with engine.begin() as conn:
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))
        for t in ["vuelos_empleados", "inventario_mov", "ventas", "vuelos", "lotes", "productos", "empleados"]:
            try:
                conn.execute(text(f"DELETE FROM {t};"))
                print(f"   {t}: borrado.")
            except Exception as e:
                print(f"   {t}: no se pudo borrar ({e}).")
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))
    """

    # --- Inserción según PLAN ---
    for filename, table, date_cols in PLAN:
        path = DATA_DIR / filename
        if not path.exists():
            # si el archivo no existe (p. ej. vuelos_empleados_preview.csv) solo avisamos y seguimos
            print(f"ℹ️  {table}: archivo no encontrado ({filename}), se omite.")
            continue

        print(f"⬆️  Cargando {filename} → {table} ...")
        try:
            df = read_csv_smart(path, parse_dates=date_cols)
            # Normalizaciones específicas por tabla (si aplica)
            if table == "lotes":
                # Asegurar tipos mínimos
                for c in ["product_id", "cantidad_inicial"]:
                    if c in df.columns:
                        df[c] = pd.to_numeric(df[c], errors="coerce")
                # Convertir fechas a string compatible si pandas deja Timestamps
                for c in ["fecha_prod", "fecha_cad"]:
                    if c in df.columns:
                        df[c] = pd.to_datetime(df[c], errors="coerce").dt.strftime("%Y-%m-%d")
                        df[c] = df[c].where(df[c].notna(), None)

            if table == "vuelos":
                if "fecha_vuelo" in df.columns:
                    df["fecha_vuelo"] = pd.to_datetime(df["fecha_vuelo"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
                    df["fecha_vuelo"] = df["fecha_vuelo"].where(df["fecha_vuelo"].notna(), None)

            if table == "inventario_mov":
                if "fecha_mov" in df.columns:
                    df["fecha_mov"] = pd.to_datetime(df["fecha_mov"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
                    df["fecha_mov"] = df["fecha_mov"].where(df["fecha_mov"].notna(), None)

            insert_df(df, table, engine)
            print(f"   {table}: total ahora = {count_rows(table, engine)} filas.")

        except Exception as e:
            print(f"❌ Error cargando {filename} → {table}: {e}")
            # Si algo falla, continuamos con las demás para diagnosticar todo
            continue

    print("🏁 Proceso finalizado.")

if __name__ == "__main__":
    main()
