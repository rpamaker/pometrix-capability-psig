# function_app.py   (raíz del repo)

import azure.functions as func
import io, re, logging, datetime as dt
from datetime import date, timedelta

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

# ─── BSP2 ────────────────────────────────────────────────────────
# Asegurate de que bsp2.py esté en tu PYTHONPATH
from bsp2 import get_exchange_rate_for_date  # espera un `datetime.date`

# ─────────────────── ⚙️  Azure Functions app ────────────────────
app = func.FunctionApp()

# ─────────────────── 🔐  Google Drive ────────────────────────────
GOOGLE_CREDENTIALS_PATH = "credentials.json"
DRIVE_FOLDER_ID        = "1Abz1Ngv5WrFaKkURXTeMYW6nrmBo9DFP"

SCOPES      = ["https://www.googleapis.com/auth/drive.file"]
credentials = service_account.Credentials.from_service_account_file(
    GOOGLE_CREDENTIALS_PATH, scopes=SCOPES
)
drive = build("drive", "v3", credentials=credentials)

# ─────────────────── 📄 Nombre de archivo secuencial ────────────
def next_filename() -> str:
    q = f"'{DRIVE_FOLDER_ID}' in parents and name contains 'Fact' and trashed=false"
    files = drive.files().list(q=q, fields="files(name)", pageSize=1000).execute()
    nums = [
        int(m.group(1))
        for f in files.get("files", [])
        if (m := re.search(r"Fact(\d{4})\.txt$", f["name"]))
    ]
    return f"Fact{(max(nums) + 1 if nums else 1):04d}.txt"

# ─────────────────── ☁️ Subir archivo a Drive ──────────────────
def upload_to_drive(name: str, content: str) -> str:
    media = MediaIoBaseUpload(io.BytesIO(content.encode()), mimetype="text/plain")
    meta  = {"name": name, "parents": [DRIVE_FOLDER_ID]}
    file  = drive.files().create(body=meta, media_body=media, fields="id").execute()
    return file["id"]

# ─────────────────── 🌐 HTTP Trigger ────────────────────────────
@app.function_name(name="httpexample")
@app.route(route="httpexample", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def http_example(req: func.HttpRequest) -> func.HttpResponse:
    # 1️⃣ JSON → dict
    try:
        data = req.get_json()
    except Exception:
        return func.HttpResponse("JSON inválido", status_code=400)

    posting = data.get("posting") or []
    if not posting:
        return func.HttpResponse("El array 'posting' no puede estar vacío", status_code=400)

    # 2️⃣ Fecha y tipo de cambio
    fecha_str = posting[0].get("fecha") or dt.date.today().isoformat()
    try:
        # ★ FIX → convertir el string ISO a objeto date
        fecha_date = dt.datetime.strptime(fecha_str.strip(), "%Y-%m-%d").date()
    except ValueError:
        return func.HttpResponse("Formato de fecha inválido (use YYYY-MM-DD)", status_code=400)

    try:
        tc = get_exchange_rate_for_date(fecha_date)  # bsp2 espera `date`
    except RuntimeError as err:
        return func.HttpResponse(f"Error al obtener TC: {err}", status_code=502)

    # 3️⃣ Encabezados
    proveedor_id  = posting[0].get("proveedor id", "000000")
    proveedor_nom = posting[0].get("proveedor nombre", "SIN NOMBRE").replace("\n", " ")
    proveedor_inf = f"{proveedor_id} {proveedor_nom}".strip()

    buf = io.StringIO()
    buf.write(f"L|{fecha_str}|GASTO|{tc}\n")
    buf.write(f"A|123| |{tc}|-{proveedor_inf}\n")

    # 4️⃣ Detalles
    for item in posting:
        fila = [
            "R",
            item.get("Cuenta", ""),
            item.get("Descripcion", "").replace("\n", " "),
            item.get("D/H", ""),
            str(item.get("Monto", "")),
            "",
            "",
            item.get("centroDeCosto", ""),
        ]
        buf.write("|".join(fila) + "|\n")

    # 5️⃣ Subir a Drive y responder
    file_id = upload_to_drive(next_filename(), buf.getvalue())
    
    return func.HttpResponse(f"TXT subido a Drive (ID: {file_id})", status_code=200)

# ─────────────────── 🏃 Ejecutar local para pruebas puntuales ───
if __name__ == "__main__":
    hoy = date.today()
    hace_30_dias = hoy - timedelta(days=30)
    print("Hoy:", hoy)
    print("Hace 30 días:", hace_30_dias)

    try:
        tc = get_exchange_rate_for_date(hace_30_dias)
        print("TCV hace 30 días:", tc)
    except RuntimeError as e:
        print("Error:", e)
