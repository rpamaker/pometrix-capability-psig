# ───────── Versión 27/06/25 – TXT fijo sin barra final (ajustada) ─────────
import azure.functions as func
import io, re, logging, datetime as dt, json

from bsp2 import get_exchange_rate_for_date

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# ───────── Google Drive ─────────
GOOGLE_CREDENTIALS_PATH = "credentials.json"
DRIVE_FOLDER_ID        = "1Abz1Ngv5WrFaKkURXTeMYW6nrmBo9DFP"
SCOPES = ["https://www.googleapis.com/auth/drive.file"]

credentials = service_account.Credentials.from_service_account_file(
    GOOGLE_CREDENTIALS_PATH, scopes=SCOPES
)
drive = build("drive", "v3", credentials=credentials)

# ───────── Anchos fijos ─────────
FIELD_WIDTHS = {
    "L": ["tipo", 1, "fecha", 8, "concepto", 6, "nro_asiento", 1],
    "A": ["tipo", 1, "nro_linea", 6, "espacio_fijo", 1,
          "importe", 10, "detalle", 45],
    "R": ["tipo", 1, "cuenta", 6, "descripcion", 30, "debe_haber", 1,
          "monto", 13, "espacio_1", 13, "centro_costo", 6,
          "espacio_2", 6, "espacio_final", 8],
}

# ───────── Helpers formato ─────────
def num_str(val: float, *, decimals: int = 2) -> str:
    """1234.56 → '123456' (últimos 2 dígitos son decimales)."""
    return str(int(round(val * (10**decimals))))

def tc_str(val: float) -> str:
    """41.95 → '41950000' (sin punto + '0000')."""
    return num_str(val) + "0000"

def fmt(val, length: int, *, align: str = "left") -> str:
    if val is None:
        val = ""
    if isinstance(val, float):
        val = num_str(val)
    val = str(val)
    if len(val) > length:
        val = val[:length]
    pad = " " * (length - len(val))
    return pad + val if align == "right" else val + pad

def build_line(line_type: str, **kwargs) -> str:
    schema = FIELD_WIDTHS[line_type]
    out = []
    for i in range(0, len(schema), 2):
        name, width = schema[i], schema[i + 1]
        align = "right" if name in {"importe", "monto",
                                    "nro_linea", "nro_asiento"} else "left"
        out.append(fmt(kwargs.get(name, ""), width, align=align))
    return "|".join(out) + "\n"          # ← sin barra final

# ───────── Google Drive utils ─────────
def next_filename() -> str:
    q = f"'{DRIVE_FOLDER_ID}' in parents and name contains 'Fact' and trashed=false"
    files = drive.files().list(q=q, fields="files(name)", pageSize=1000).execute()
    nums = [int(m.group(1))
            for f in files.get("files", [])
            if (m := re.search(r"Fact(\d{4})\.txt$", f["name"]))]  # noqa: E501
    return f"Fact{(max(nums) + 1 if nums else 1):04d}.txt"

def upload_to_drive(name: str, content: str) -> str:
    media = MediaIoBaseUpload(io.BytesIO(content.encode()), mimetype="text/plain")
    meta  = {"name": name, "parents": [DRIVE_FOLDER_ID]}
    file  = drive.files().create(body=meta, media_body=media,
                                 fields="id").execute()
    return file["id"]

# ───────── Constantes ─────────
CURRENCY_USD_CODES = {"USD", "US$", "DOL", "DÓLAR"}

# ───────── HTTP Trigger ─────────
@app.route(route="http_trigger")
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("🟢 Pometrix TXT generator invoked")

    try:
        body = req.get_json()
        posting = body.get("posting", [])
    except ValueError:
        return func.HttpResponse("Invalid JSON", status_code=400)
    if not posting:
        return func.HttpResponse("Payload mal formado", status_code=400)

    # ── datos generales ──
    numero_factura = str(posting[0].get("numero", "")).strip()
    fecha_str = posting[0].get("fecha", dt.date.today().isoformat())
    try:
        fecha_dt = dt.datetime.strptime(fecha_str, "%Y-%m-%d").date()
    except ValueError:
        return func.HttpResponse("Fecha inválida (YYYY-MM-DD)", status_code=400)

    try:
        tc_val = get_exchange_rate_for_date(fecha_dt)
    except RuntimeError as err:
        return func.HttpResponse(f"Error TC: {err}", status_code=502)

    prov_nom = posting[0].get("proveedor nombre", "SIN NOMBRE").replace("\n", " ")

    buf = io.StringIO()

    # ───────── Línea L ─────────
    buf.write(build_line("L",
                         tipo="L",
                         fecha=fecha_str.replace("-", ""),
                         concepto="GASTOS",
                         nro_asiento="0"))

    # ───────── Línea A ─────────
    buf.write(build_line(
        "A",
        tipo="A",
        nro_linea=numero_factura,
        espacio_fijo=" ",
        importe=tc_str(tc_val),
        # ------------- CAMBIO -------------
        # Antes: "Nro Doc {numero_factura} - {prov_id} {prov_nom}"
        # Ahora: "EF.{numero_factura} {prov_nom}"
        detalle=f"EF.{numero_factura} {prov_nom}"
    ))

    # ───────── Líneas R ─────────
    for item in posting:
        moneda = (item.get("moneda") or "UYU").upper()
        monto  = float(item.get("Monto", 0))
        if moneda in CURRENCY_USD_CODES:
            monto *= tc_val

        buf.write(build_line("R",
                             tipo="R",
                             cuenta=item.get("Cuenta", ""),
                             descripcion=item.get("Descripcion", "").replace("\n", " "),
                             debe_haber=item.get("D/H", "D"),
                             monto=num_str(monto),
                             espacio_1="",
                             centro_costo=item.get("centroDeCosto", ""),
                             espacio_2="",
                             espacio_final=""))

    # ── subir a Drive ──
    try:
        file_id = upload_to_drive(next_filename(), buf.getvalue())
    except Exception as e:
        logging.error(f"❌ Upload error: {e}")
        return func.HttpResponse("Error al subir a Drive", status_code=500)

    return func.HttpResponse(
        json.dumps({
            "ok": True,
            "fileId": file_id,
            "tipoCambioUSD": tc_val,
            "lineas": len(posting)
        }),
        status_code=200,
        mimetype="application/json"
    )
