import os
import json
from typing import Dict, Any, Optional
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, validator
import psycopg2
from psycopg2.extras import execute_values
from datetime import date
from fastapi.middleware.cors import CORSMiddleware

DATABASE_URL = os.getenv("DATABASE_URL")  # dari Railway
assert DATABASE_URL, "Env DATABASE_URL belum di-set"

app = FastAPI(title="PM2.5 Ingest & API")

# CORS untuk Vercel frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # ganti dengan domain vercel kamu agar lebih aman
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---- Models ----
class PayloadAggregat(BaseModel):
    tanggal: date
    kota: str = "Depok"
    estimasi: Dict[str, float] = Field(..., description="Map kecamatan->nilai PM2.5")
    rata_rata_kota: float
    tanggal_fitur: Optional[Dict[str, Dict[str, str]]] = None  # per kecamatan -> per fitur

    @validator("estimasi")
    def not_empty(cls, v):
        if not v:
            raise ValueError("estimasi kosong")
        return v

# ---- DB helper ----
def get_conn():
    return psycopg2.connect(DATABASE_URL, connect_timeout=10, sslmode="require")

# ---- Ingest endpoint ----
@app.post("/api/pm25/ingest")
def ingest(payload: PayloadAggregat):
    # 1) Upsert ke estimasi_harian
    kolom_map = {
        "Beji":"beji","Bojongsari":"bojongsari","Cilodong":"cilodong","Cimanggis":"cimanggis",
        "Cinere":"cinere","Cipayung":"cipayung","Limo":"limo","Pancoran Mas":"pancoran_mas",
        "Sawangan":"sawangan","Sukmajaya":"sukmajaya","Tapos":"tapos"
    }

    cols = ['tanggal', 'kota', 'rata_rata_kota'] + list(kolom_map.values())
    values = [payload.tanggal, payload.kota, payload.rata_rata_kota]
    for k in kolom_map.keys():
        values.append(payload.estimasi.get(k))  # bisa None jika tidak ada

    placeholders = ", ".join(["%s"]*len(values))
    update_set = ", ".join([f'{c}=EXCLUDED.{c}' for c in cols[1:]])

    upsert_sql = f"""
      INSERT INTO estimasi_harian ({", ".join(cols)})
      VALUES ({placeholders})
      ON CONFLICT (tanggal) DO UPDATE SET {update_set}
    """

    # 2) Upsert metadata per kecamatan (jika ada)
    meta_rows = []
    if payload.tanggal_fitur:
        for kec, meta in payload.tanggal_fitur.items():
            fitur = {}  # optional: simpan nilai fitur asli kalau dikirim
            meta_rows.append( (payload.tanggal, kec, json.dumps(fitur), json.dumps(meta)) )

    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(upsert_sql, values)

            if meta_rows:
                execute_values(
                    cur,
                    """
                    INSERT INTO estimasi_metadata (tanggal, kecamatan, fitur, tanggal_fitur)
                    VALUES %s
                    ON CONFLICT (tanggal, kecamatan) DO UPDATE
                    SET fitur=EXCLUDED.fitur, tanggal_fitur=EXCLUDED.tanggal_fitur
                    """,
                    meta_rows
                )

        return {"status":"ok","upserted":"estimasi_harian","meta_rows":len(meta_rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ---- Query endpoints ----
@app.get("/api/pm25/latest")
def latest():
    sql = """
      SELECT * FROM estimasi_harian
      ORDER BY tanggal DESC LIMIT 1
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
        if not row:
            return {"data": None}
        cols = [d.name for d in cur.description]
        return {"data": dict(zip(cols, row))}

@app.get("/api/pm25/history")
def history(limit: int = 30):
    sql = """
      SELECT tanggal, kota, rata_rata_kota, beji, bojongsari, cilodong, cimanggis, cinere,
             cipayung, limo, pancoran_mas, sawangan, sukmajaya, tapos
      FROM estimasi_harian
      ORDER BY tanggal DESC
      LIMIT %s
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (limit,))
        cols = [d.name for d in cur.description]
        return {"data": [dict(zip(cols, r)) for r in cur.fetchall()]}

@app.get("/api/pm25/stats")
def stats():
    sql = """
      SELECT
        COUNT(*) AS n_hari,
        AVG(rata_rata_kota) AS pm25_avg,
        MIN(rata_rata_kota) AS pm25_min,
        MAX(rata_rata_kota) AS pm25_max
      FROM estimasi_harian
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
        cols = [d.name for d in cur.description]
        return {"data": dict(zip(cols, row))}


