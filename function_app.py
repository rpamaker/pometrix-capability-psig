import azure.functions as func
import io, re, logging, datetime as dt
import json
from bsp2 import get_exchange_rate_for_date  # espera un `datetime.date`
from datetime import date, timedelta

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload


app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


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


@app.route(route="http_trigger")
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        req_body = req.get_json()
    except ValueError:
        return func.HttpResponse(
            "Invalid JSON",
            status_code=400
        )

    # Espera un payload con una lista bajo la clave 'posting'
    if not isinstance(req_body, dict) or 'posting' not in req_body or not isinstance(req_body['posting'], list):
        return func.HttpResponse(
            "Invalid payload: expected a JSON object with a 'posting' list",
            status_code=400
        )

    posting = req_body['posting']

    # Comienzo de la logica
    # 2️⃣ Fecha y tipo de cambio - La fehca es unica por factura por lo que viene misma fecha en todas las lineas, tomo la primera
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

    # 3️⃣ Agrego encabezados encabezados
    proveedor_id  = posting[0].get("proveedor id", "000000")
    proveedor_nom = posting[0].get("proveedor nombre", "SIN NOMBRE").replace("\n", " ")
    proveedor_inf = f"{proveedor_id} {proveedor_nom}".strip()

    buf = io.StringIO()
    buf.write(f"L|{fecha_str}|GASTO|0\n")
    buf.write(f"A|123| |{tc}|-{proveedor_inf}\n")

    # 4️⃣ Detalles por linea
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

    # 5️⃣ Subir a Drive
    file_id = upload_to_drive(next_filename(), buf.getvalue())

    # 6️⃣ Respuesta
    return func.HttpResponse(
        json.dumps(req_body),
        status_code=200,
        mimetype="application/json"
    )
