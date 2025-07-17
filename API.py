from fastapi import FastAPI, HTTPException, File, UploadFile, Form
import psycopg2
import os
import pickle
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import base64
import idna
import re
import unidecode
from openai import OpenAI
from pydantic import BaseModel
from googleapiclient.discovery import build

# Modelos de datos
class PromptRequest(BaseModel):
    prompt: str

class EmailRequest(BaseModel):
    categoria: str
    tipo: str
    tipo_envio: str
    cantidad: int
    asunto: str
    mensaje: str

# Inicializar FastAPI
app = FastAPI()

# Cliente de OpenAI
client = OpenAI(
    api_key="",
    base_url="https://openrouter.ai/api/v1"
)

# Rutas de credenciales
CREDENTIALS_FILE = "credentials.json"
TOKEN_FILE = "token.pickle"
SCOPES = ["https://www.googleapis.com/auth/gmail.modify"]

# Conectar a la base de datos PostgreSQL
def conectar_db():
    try:
        conexion = psycopg2.connect(
            user="postgres",
            password="MecheAC09",
            host="localhost",
            port="5432",
            dbname="revistaPostgre",
        )
        return conexion
    except Exception as e:
        # Si ya es un HTTPException, lo re‑lanzamos sin envolver
        if isinstance(e, HTTPException):
            raise
        raise HTTPException(status_code=500,
                            detail=f"Error de conexión a la base de datos: {e}")

# Obtener credenciales de OAuth2
def obtener_credenciales():
    creds = None
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, "rb") as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_FILE, "wb") as token:
            pickle.dump(creds, token)
    return creds

#limpieza
def limpiar_correo(correo: str) -> str | None:
    """
    Limpia el correo:
    - Elimina espacios
    - Reemplaza tildes (ej: josé → jose)
    - Valida el formato
    """
    correo = correo.strip()  # quita espacios
    correo = unidecode.unidecode(correo)  # elimina tildes
    # Validar formato simple de email
    if re.match(r"^[^@]+@[^@]+\.[^@]+$", correo):
        return correo
    return None  # no válido

#Corregir los mail
def convertir_a_punycode(email):
    try:
        local, domain = email.strip().split("@")
        domain_ascii = idna.encode(domain).decode("ascii")
        return f"{local}@{domain_ascii}"
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Email inválido o no convertible: {email} - {e}")

 #Validar email   
def validar_email(correo: str) -> bool:
    correo = correo.strip()
    patron = r"^[\w\.\-+%]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    return re.match(patron, correo) is not None

# Función para obtener respuesta de la IA
def generar_respuesta_ia(prompt):
    try:
        chat = client.chat.completions.create(
            model="deepseek/deepseek-r1:free",
            messages=[
                {
                    "role": "user",
                    "content": prompt
                }
            ]
        )
        return chat.choices[0].message.content
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al comunicarse con la IA: {e}")
    
def guardar_IDCorreo(email, message_id):
    conexion = conectar_db()
    cursor = conexion.cursor()
    cursor.execute("UPDATE clientes SET message_id = %s WHERE mail = %s;", (message_id, email))
    conexion.commit()

def modificar_ultimo_envio(mail):
    conexion = conectar_db()
    cursor = conexion.cursor()
    cursor.execute("UPDATE clientes SET ultimo_envio = NOW() WHERE mail =%s;", (mail,))
    conexion.commit()

# Función para reemplazar términos clave
def personalizar_mensaje(mensaje, asunto, empresa):
    # Reemplazar términos clave en el mensaje
    mensaje_personalizado = mensaje.replace("[Empresa]", empresa)
    
    # Reemplazar términos clave en el asunto
    asunto_personalizado = asunto.replace("[Empresa]", empresa)
    
    return asunto_personalizado, mensaje_personalizado

def enviar_recordatorio(service, destinatarios, mensaje_id, mensaje, archivo_adjunto=None, nombre_archivo=None):
    destinatarios_limpios = [convertir_a_punycode(d.strip()) for d in destinatarios.split(',') if d.strip()]

    mensaje_original = service.users().messages().get(userId='me', id=mensaje_id, format='metadata').execute()
    headers = mensaje_original.get("payload", {}).get("headers", [])

    def obtener_header(headers, clave):
        return next((h["value"] for h in headers if h["name"].lower() == clave.lower()), None)

    asunto = obtener_header(headers, "subject") or "(sin asunto)"
    referencia = obtener_header(headers, "Message-Id")
    remitente = "pinguinolalo00@gmail.com"

    mime_mensaje = MIMEMultipart()
    mime_mensaje["to"] = remitente  # Enviar a nosotros mismos
    mime_mensaje["bcc"] = ", ".join(destinatarios_limpios)
    mime_mensaje["from"] = remitente
    mime_mensaje["subject"] = "Re: " + asunto
    mime_mensaje["In-Reply-To"] = referencia
    mime_mensaje["References"] = referencia
    mime_mensaje.attach(MIMEText(mensaje, "html"))

    if isinstance(archivo_adjunto, bytes):
        filename = nombre_archivo if nombre_archivo else "documento.pdf"
        part = MIMEApplication(archivo_adjunto, Name=filename)
        part.add_header("Content-Disposition", f"attachment; filename={filename}")
        mime_mensaje.attach(part)

    raw_message = base64.urlsafe_b64encode(mime_mensaje.as_bytes()).decode("utf-8")
    message = {
        "raw": raw_message,
        "threadId": mensaje_original.get('threadId')
    }

    try:
        service.users().messages().send(userId="me", body=message).execute()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al enviar correo a {destinatarios}: {e}")


def enviar_correo(service, destinatarios, asunto, mensaje, archivo_adjunto=None, nombre_archivo=None):
    destinatarios_limpios = [convertir_a_punycode(d.strip()) for d in destinatarios.split(',') if d.strip()]

    mime_mensaje = MIMEMultipart()
    mime_mensaje["to"] = "pinguinolalo00@gmail.com" # Enviar a nosotros mismos
    mime_mensaje["bcc"] = ", ".join(destinatarios_limpios)
    mime_mensaje["subject"] = asunto
    mime_mensaje["from"] = "pinguinolalo00@gmail.com"
    
    mime_mensaje.attach(MIMEText(mensaje, "html"))

    if isinstance(archivo_adjunto, bytes):
        filename = nombre_archivo if nombre_archivo else "documento.pdf"
        part = MIMEApplication(archivo_adjunto, Name=filename)
        part.add_header("Content-Disposition", f"attachment; filename={filename}")
        mime_mensaje.attach(part)

    raw_message = base64.urlsafe_b64encode(mime_mensaje.as_bytes()).decode("utf-8")
    message = {"raw": raw_message}

    try:
        sent_message = service.users().messages().send(userId="me", body=message).execute()
        message_id = sent_message['id']
        for email in destinatarios_limpios:
            guardar_IDCorreo(email, message_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al enviar correo a {destinatarios}: {e}")



def get_tipo_correo(tipo, tipo_envio):
    base_query = """
        FROM clientes cli
        JOIN cliente_detalle cd ON cli.id = cd.clienteid
    """
    group_by = " GROUP BY cli.nombre"

    if tipo == "Sub-Recordatorio":
        query = """
        SELECT cli.nombre, STRING_AGG(cli.mail, ',') as mails, cli.message_id
        """ + base_query + """
        WHERE cd.subcategoriaid = %s AND cli.estado = TRUE
        """ + group_by
    elif tipo == "Sub-Normal":
        query = """
        SELECT cli.nombre, STRING_AGG(cli.mail, ',') as mails
        """ + base_query + """
        WHERE cd.subcategoriaid = %s
        """ + group_by
    elif tipo == "Cat-Recordatorio":
        query = """
        SELECT cli.nombre, STRING_AGG(cli.mail, ',') as mails, cli.message_id
        """ + base_query + """
        JOIN subcategorias sub ON cd.subcategoriaid = sub.id
        JOIN categorias cat ON sub.categoriaid = cat.id
        WHERE cat.id = %s AND cli.estado = TRUE
        """ + group_by
    elif tipo == "Cat-Normal":
        query = """
        SELECT cli.nombre, STRING_AGG(cli.mail, ',') as mails
        """ + base_query + """
        JOIN subcategorias sub ON cd.subcategoriaid = sub.id
        JOIN categorias cat ON sub.categoriaid = cat.id
        WHERE cat.id = %s
        """ + group_by
    else:
        query = None

    if query and tipo_envio == "Personalizado":
        # La cláusula ORDER BY no es compatible con GROUP BY de esta manera.
        # Se puede ajustar si es necesario, pero por ahora se omite.
        pass

    return query


# Endpoint para procesar el prompt con la IA
@app.post("/generar-contenido/")
async def generar_contenido(request: PromptRequest):
    try:
        contenido_generado = generar_respuesta_ia(request.prompt)
        return {"contenido": contenido_generado}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Endpoint para enviar correos con el mensaje generado por la IA y archivo adjunto (ruta local)
@app.post("/enviar-correos-personalizados/")
async def enviar_correos_personalizados(request: EmailRequest):
    import asyncio

    BATCH_SIZE = 30
    TIEMPO_ENTRE_LOTES = 10
    TIEMPO_ENTRE_CORREOS = 10

    conexion = conectar_db()
    cursor = conexion.cursor()
    creds = obtener_credenciales()
    service = build("gmail", "v1", credentials=creds)

    fallidos = []

    try:
        tipo = request.tipo
        categoria = request.categoria
        query = get_tipo_correo(tipo, request.tipo_envio)

        if query:
            cursor.execute(query, (categoria,))
        clientes = cursor.fetchall()

        if not clientes:
            return {"mensaje": "No se encontraron clientes", "fallidos": []}

        correos_enviados = []
        max_envios = len(clientes) if request.tipo_envio == "Todos" or request.cantidad > len(clientes) else request.cantidad
        es_recordatorio = tipo in ("Sub-Recordatorio", "Cat-Recordatorio")

        for i in range(0, max_envios, BATCH_SIZE):
            lote = clientes[i:i + BATCH_SIZE]
            for cliente in lote:
                try:
                    if es_recordatorio:
                        nombre, mails, message_id = cliente
                    else:
                        nombre, mails = cliente

                    asunto_personalizado, mensaje_personalizado = personalizar_mensaje(request.mensaje, request.asunto, nombre)

                    if es_recordatorio:
                        enviar_recordatorio(service, mails, message_id, mensaje_personalizado)
                    else:
                        enviar_correo(service, mails, asunto_personalizado, mensaje_personalizado)

                    for mail in mails.split(','):
                        modificar_ultimo_envio(mail)
                    correos_enviados.append({"destinatario": mails, "empresa": nombre})
                    await asyncio.sleep(TIEMPO_ENTRE_CORREOS)

                except Exception as e:
                    razon = str(e)
                    if "Invalid to header" in razon or "Invalid email" in razon:
                        razon_simplificada = "Correo inválido"
                    elif "domain not found" in razon:
                        razon_simplificada = "Dominio no válido"
                    else:
                        razon_simplificada = "Error al enviar"

                    fallidos.append({"mail": mails, "empresa": nombre, "razon": razon_simplificada})

            await asyncio.sleep(TIEMPO_ENTRE_LOTES)

        return {
            "mensaje": f"Se enviaron {len(correos_enviados)} correos exitosamente",
            "detalle": correos_enviados,
            "fallidos": fallidos
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conexion.close()



# Endpoint para enviar correos con archivo adjunto subido a través de la API
@app.post("/enviar-correos-con-adjunto/")
async def enviar_correos_con_adjunto(
    categoria: str = Form(...),
    tipo: str = Form(...),
    tipo_envio: str = Form(...),
    cantidad: int = Form(...),
    asunto: str = Form(...),
    mensaje: str = Form(...),
    archivo: UploadFile = File(...)
):
    import asyncio

    BATCH_SIZE = 30
    TIEMPO_ENTRE_LOTES = 10
    TIEMPO_ENTRE_CORREOS = 10

    conexion = conectar_db()
    cursor = conexion.cursor()
    creds = obtener_credenciales()
    service = build("gmail", "v1", credentials=creds)

    fallidos = []

    try:
        contenido_archivo = await archivo.read()
        nombre_archivo = archivo.filename
        query = get_tipo_correo(tipo, tipo_envio)

        if query:
            cursor.execute(query, (categoria,))
        clientes = cursor.fetchall()

        if not clientes:
            return {"mensaje": "No se encontraron clientes", "fallidos": []}

        correos_enviados = []
        max_envios = len(clientes) if tipo_envio == "Todos" or cantidad > len(clientes) else cantidad
        es_recordatorio = tipo in ("Sub-Recordatorio", "Cat-Recordatorio")

        for i in range(0, max_envios, BATCH_SIZE):
            lote = clientes[i:i + BATCH_SIZE]
            for cliente in lote:
                try:
                    if es_recordatorio:
                        nombre, mails, message_id = cliente
                    else:
                        nombre, mails = cliente

                    asunto_personalizado, mensaje_personalizado = personalizar_mensaje(mensaje, asunto, nombre)

                    if es_recordatorio:
                        enviar_recordatorio(service, mails, message_id, mensaje_personalizado, contenido_archivo, nombre_archivo)
                    else:
                        enviar_correo(service, mails, asunto_personalizado, mensaje_personalizado, contenido_archivo, nombre_archivo)

                    for mail in mails.split(','):
                        modificar_ultimo_envio(mail)
                    correos_enviados.append({"destinatario": mails, "empresa": nombre})
                    await asyncio.sleep(TIEMPO_ENTRE_CORREOS)

                except Exception as e:
                    razon = str(e)
                    if "Invalid to header" in razon or "Invalid email" in razon:
                        razon_simplificada = "Correo inválido"
                    elif "domain not found" in razon:
                        razon_simplificada = "Dominio no válido"
                    else:
                        razon_simplificada = "Error al enviar"

                    fallidos.append({"mail": mails, "empresa": nombre, "razon": razon_simplificada})

            await asyncio.sleep(TIEMPO_ENTRE_LOTES)

        return {
            "mensaje": f"Se enviaron {len(correos_enviados)} correos exitosamente",
            "detalle": correos_enviados,
            "fallidos": fallidos
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conexion.close()


# Para pruebas
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)