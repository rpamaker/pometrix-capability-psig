import azure.functions as func
import json, io, logging, re
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

app = func.FunctionApp()

# ────────────────────────
# ⚙️  CONFIGURACIÓN
# ────────────────────────
GOOGLE_CREDENTIALS_PATH = "credentials.json"                 # ruta al .json de servicio
DRIVE_FOLDER_ID = "1Abz1Ngv5WrFaKkURXTeMYW6nrmBo9DFP"       # ID de la carpeta destino

# ────────────────────────
# 🔐 AUTENTICACIÓN GOOGLE
# ────────────────────────
SCOPES = ['https://www.googleapis.com/auth/drive.file']
credentials = service_account.Credentials.from_service_account_file(
    GOOGLE_CREDENTIALS_PATH, scopes=SCOPES
)
drive_service = build('drive', 'v3', credentials=credentials)

# ────────────────────────
# 📄 PRÓXIMO NOMBRE DISPONIBLE
# ────────────────────────
def get_next_filename() -> str:
    """
    Devuelve 'FactXXXX.txt' con el siguiente número disponible.
    Lee los nombres que ya existen en la carpeta de Drive.
    """
    query = (
        f"'{DRIVE_FOLDER_ID}' in parents and "
        "name contains 'Fact' and name contains '.txt' and trashed = false"
    )
    res = drive_service.files().list(
        q=query,
        spaces='drive',
        fields='files(name)',
        pageSize=1000        # subí si la carpeta se hace enorme
    ).execute()

    nums = [
        int(m.group(1))
        for f in res.get("files", [])
        if (m := re.search(r"Fact(\d+)\.txt$", f["name"]))
    ]
    next_n = max(nums) + 1 if nums else 1
    return f"Fact{next_n:04d}.txt"

# ────────────────────────
# ☁️ SUBIR A DRIVE
# ────────────────────────
def upload_to_drive(content: str, filename: str) -> str:
    media = MediaIoBaseUpload(
        io.BytesIO(content.encode('utf-8')),
        mimetype='text/plain',
        resumable=False
    )
    file_metadata = {
        'name': filename,
        'parents': [DRIVE_FOLDER_ID]
    }
    file = drive_service.files().create(
        body=file_metadata,
        media_body=media,
        fields='id'
    ).execute()
    return file["id"]

# ────────────────────────
# 🌐 ENDPOINT HTTP
# ────────────────────────
@app.function_name(name="httpexample")
@app.route(route="httpexample", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def http_example(req: func.HttpRequest) -> func.HttpResponse:
    try:
        data = req.get_json()
    except Exception:
        return func.HttpResponse("Invalid JSON", status_code=400)

    posting = data.get("posting")
    if not posting:
        return func.HttpResponse("El array 'posting' no puede estar vacío", status_code=400)

    fecha = posting[0].get("fecha", "2025-01-01")
    proveedor_id = posting[0].get("proveedor id", "000000")
    proveedor_nombre = posting[0].get("proveedor nombre", "SIN NOMBRE").replace("\n", " ")
    proveedor_info = f"{proveedor_id} {proveedor_nombre}".strip()

    buffer = io.StringIO()
    buffer.write(f"L|{fecha}|GASTO|0\n")
    buffer.write(f"A|123| |38.50|-{proveedor_info}\n")

    for item in posting:
        fila = [
            "R",
            item.get("Cuenta", ""),
            item.get("Descripcion", "").replace("\n", " "),
            item.get("D/H", ""),
            str(item.get("Monto", "")),
            "", "",
            item.get("centroDeCosto", "")
        ]
        buffer.write("|".join(fila) + "|\n")

    txt_content = buffer.getvalue()
    buffer.close()

    filename = get_next_filename()
    drive_file_id = upload_to_drive(txt_content, filename)

    return func.HttpResponse(
        f"Archivo {filename} subido a Google Drive (ID: {drive_file_id})",
        mimetype="text/plain",
        status_code=200
    )
