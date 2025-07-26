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
    api_key="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",
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
            password="password",
            host="localhost",
            port="5432",
            dbname="revistaPostgre",
        )
        return conexion
    except Exception as e:
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
    correo = correo.strip()
    correo = unidecode.unidecode(correo)
    if re.match(r"^[^@]+@[^@]+\.[^@]+$", correo):
        return correo
    return None

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
    cursor.close()
    conexion.close()

def modificar_ultimo_envio(mail):
    conexion = conectar_db()
    cursor = conexion.cursor()
    cursor.execute("UPDATE clientes SET ultimo_envio = NOW() WHERE mail =%s;", (mail,))
    conexion.commit()
    cursor.close()
    conexion.close()

def obtener_message_ids_por_empresa(nombre_empresa):
    """Obtiene todos los message_ids de los correos de una empresa"""
    conexion = conectar_db()
    cursor = conexion.cursor()
    cursor.execute("SELECT DISTINCT message_id FROM clientes WHERE nombre = %s AND message_id IS NOT NULL;", (nombre_empresa,))
    message_ids = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conexion.close()
    return message_ids

# Función para reemplazar términos clave
def personalizar_mensaje(mensaje, asunto, empresa):
    # Reemplazar términos clave en el mensaje
    mensaje_personalizado = mensaje.replace("[Empresa]", empresa)
    
    # Reemplazar términos clave en el asunto
    asunto_personalizado = asunto.replace("[Empresa]", empresa)
    
    return asunto_personalizado, mensaje_personalizado

def enviar_recordatorio(service, destinatarios, asunto, mensaje, thread_id=None, message_id=None, archivo_adjunto=None, nombre_archivo=None):
    """Envía un correo como respuesta a un hilo existente."""
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.mime.application import MIMEApplication
    import base64

    # Convertir destinatarios a punycode si es necesario
    lista_destinatarios = [convertir_a_punycode(d.strip()) for d in destinatarios.split(",") if d.strip()]
    destinatarios_final = ", ".join(lista_destinatarios)

    mime_mensaje = MIMEMultipart()
    mime_mensaje["to"] = destinatarios_final
    mime_mensaje["subject"] = asunto
    mime_mensaje["from"] = "pinguinolalo00@gmail.com"

    # Encabezados para que se vea como reply
    if message_id:
        mime_mensaje["In-Reply-To"] = message_id
        mime_mensaje["References"] = message_id

    mime_mensaje.attach(MIMEText(mensaje, "html"))

    # Adjuntar archivo si se proporciona
    if isinstance(archivo_adjunto, bytes):
        filename = nombre_archivo if nombre_archivo else "documento.pdf"
        part = MIMEApplication(archivo_adjunto, Name=filename)
        part.add_header("Content-Disposition", f"attachment; filename={filename}")
        mime_mensaje.attach(part)

    # Codificar el mensaje en base64
    raw_message = base64.urlsafe_b64encode(mime_mensaje.as_bytes()).decode("utf-8")
    
    # Crear cuerpo del mensaje para la API de Gmail
    message_body = {"raw": raw_message}
    if thread_id:
        message_body["threadId"] = thread_id

    try:
        sent_message = service.users().messages().send(
            userId="me",
            body=message_body
        ).execute()

        new_message_id = sent_message['id']
        
        # Guardar ID del nuevo mensaje para seguimiento
        for destinatario in lista_destinatarios:
            guardar_IDCorreo(destinatario, new_message_id)
            modificar_ultimo_envio(destinatario)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al enviar recordatorio a {destinatarios_final}: {e}")

def enviar_correo(service, destinatarios, asunto, mensaje, archivo_adjunto=None, nombre_archivo=None):
    # Separar múltiples destinatarios (ya separados por coma) y convertir cada uno
    lista_destinatarios = [convertir_a_punycode(d.strip()) for d in destinatarios.split(",") if d.strip()]
    destinatarios_final = ", ".join(lista_destinatarios)

    mime_mensaje = MIMEMultipart()
    mime_mensaje["to"] = destinatarios_final
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
        # Guardar el ID para cada destinatario individual
        for destinatario in lista_destinatarios:
            guardar_IDCorreo(destinatario, message_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al enviar correo a {destinatarios_final}: {e}")
    
def obtener_asunto_original(service, message_id):
    try:
        mensaje = service.users().messages().get(userId="me", id=message_id, format="metadata").execute()
        headers = mensaje["payload"]["headers"]
        for header in headers:
            if header["name"].lower() == "subject":
                return header["value"]
        return None  # No se encontró asunto
    except Exception as e:
        print(f"Error al obtener asunto: {e}")
        return None

# FUNCIÓN FALTANTE: enviar_correo_con_adjunto
def enviar_correo_con_adjunto(service, destinatarios, asunto, mensaje, archivo_adjunto, nombre_archivo):
    return enviar_correo(service, destinatarios, asunto, mensaje, archivo_adjunto, nombre_archivo)

def get_tipo_correo(tipo, tipo_envio):
    if tipo == "Sub-Recordatorio":
        if tipo_envio == "Personalizado":
            query = """
            SELECT DISTINCT cli.nombre, cli.mail, cli.message_id, cli.ultimo_envio
            FROM clientes cli
            JOIN cliente_detalle cd ON cli.id = cd.clienteid
            WHERE cd.subcategoriaid = %s AND cli.estado = TRUE AND cli.message_id IS NOT NULL
            AND (cli.ultimo_envio IS NULL OR cli.ultimo_envio < NOW() - INTERVAL '3 days')
            ORDER BY cli.ultimo_envio ASC
            """
        else:
            query = """
            SELECT DISTINCT cli.nombre, cli.mail, cli.message_id
            FROM clientes cli
            JOIN cliente_detalle cd ON cli.id = cd.clienteid
            WHERE cd.subcategoriaid = %s AND cli.estado = TRUE AND cli.message_id IS NOT NULL
            """
    
    elif tipo == "Sub-Normal":
        if tipo_envio == "Personalizado":
            query = """
            SELECT DISTINCT cli.nombre, cli.mail, cli.ultimo_envio
            FROM clientes cli
            JOIN cliente_detalle cd ON cli.id = cd.clienteid
            WHERE cd.subcategoriaid = %s
            AND (cli.ultimo_envio IS NULL OR cli.ultimo_envio < NOW() - INTERVAL '3 days')
            ORDER BY cli.ultimo_envio ASC
            """
        else:
            query = """
            SELECT DISTINCT cli.nombre, cli.mail
            FROM clientes cli
            JOIN cliente_detalle cd ON cli.id = cd.clienteid
            WHERE cd.subcategoriaid = %s
            """
    
    elif tipo == "Cat-Recordatorio":
        if tipo_envio == "Personalizado":
            query = """
            SELECT DISTINCT cli.nombre, cli.mail, cli.message_id, cli.ultimo_envio
            FROM clientes cli
            JOIN cliente_detalle cd ON cli.id = cd.clienteid
            JOIN subcategorias sub ON cd.subcategoriaid = sub.id
            JOIN categorias cat ON sub.categoriaid = cat.id
            WHERE cat.id = %s AND cli.estado = TRUE AND cli.message_id IS NOT NULL
            AND (cli.ultimo_envio IS NULL OR cli.ultimo_envio < NOW() - INTERVAL '3 days')
            ORDER BY cli.ultimo_envio ASC
            """
        else:
            query = """
            SELECT DISTINCT cli.nombre, cli.mail, cli.message_id
            FROM clientes cli
            JOIN cliente_detalle cd ON cli.id = cd.clienteid
            JOIN subcategorias sub ON cd.subcategoriaid = sub.id
            JOIN categorias cat ON sub.categoriaid = cat.id
            WHERE cat.id = %s AND cli.estado = TRUE AND cli.message_id IS NOT NULL
            """
    
    elif tipo == "Cat-Normal":
        if tipo_envio == "Personalizado":
            query = """
            SELECT DISTINCT cli.nombre, cli.mail, cli.ultimo_envio
            FROM clientes cli
            JOIN cliente_detalle cd ON cli.id = cd.clienteid
            JOIN subcategorias sub ON cd.subcategoriaid = sub.id
            JOIN categorias cat ON sub.categoriaid = cat.id
            WHERE cat.id = %s
            AND (cli.ultimo_envio IS NULL OR cli.ultimo_envio < NOW() - INTERVAL '3 days')
            ORDER BY cli.ultimo_envio ASC
            """
        else:
            query = """
            SELECT DISTINCT cli.nombre, cli.mail
            FROM clientes cli
            JOIN cliente_detalle cd ON cli.id = cd.clienteid
            JOIN subcategorias sub ON cd.subcategoriaid = sub.id
            JOIN categorias cat ON sub.categoriaid = cat.id
            WHERE cat.id = %s
            """
    
    else:
        query = None
    return query

def obtener_message_id_header(service, internal_message_id):
    try:
        mensaje = service.users().messages().get(
            userId="me",
            id=internal_message_id,
            format="metadata",
            metadataHeaders=["Message-ID"]
        ).execute()

        headers = mensaje["payload"]["headers"]
        for header in headers:
            if header["name"].lower() == "message-id":
                return header["value"]  # ← Este es el que necesitas
        return None
    except Exception as e:
        print(f"Error al obtener Message-ID: {e}")
        return None

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

        # Determinar si es recordatorio
        es_recordatorio = "Recordatorio" in tipo

        # Agrupar por empresa y juntar todos sus correos (eliminando duplicados)
        empresas = {}
        clientes_procesados = set()  # Para evitar duplicados por correo
        
        for fila in clientes:
            nombre = fila[0]
            correo = fila[1]
            message_id = fila[2] if len(fila) > 2 else None
            
            correo_limpio = limpiar_correo(correo)
            if not correo_limpio:
                fallidos.append({"mail": correo, "empresa": nombre, "razon": "Correo inválido"})
                continue
            
            # Crear clave única por correo para evitar duplicados
            clave_cliente = correo_limpio.lower()
            if clave_cliente in clientes_procesados:
                continue  # Saltar si ya procesamos este correo
            
            clientes_procesados.add(clave_cliente)
                
            if nombre not in empresas:
                empresas[nombre] = {"correos": set(), "message_ids": set()}
            
            empresas[nombre]["correos"].add(correo_limpio)
            if message_id:
                empresas[nombre]["message_ids"].add(message_id)

        correos_enviados = []
        empresas_items = list(empresas.items())
        max_envios = len(empresas_items) if request.tipo_envio == "Todos" or request.cantidad > len(empresas_items) else request.cantidad

        for i in range(0, max_envios, BATCH_SIZE):
            lote = empresas_items[i:i + BATCH_SIZE]
            for nombre, data in lote:
                try:
                    # Enviar a todos los correos juntos
                    destinatarios = ", ".join([convertir_a_punycode(c) for c in data["correos"]])
                    asunto_personalizado, mensaje_personalizado = personalizar_mensaje(request.mensaje, request.asunto, nombre)

                    if es_recordatorio and data["message_ids"]:
                        try:
                            # Obtener el ID interno del mensaje original
                            original_internal_id = list(data["message_ids"])[0]

                            # Obtener el threadId del mensaje original
                            original_message = service.users().messages().get(userId="me", id=original_internal_id, format="metadata").execute()
                            original_thread_id = original_message.get("threadId")

                            # Obtener el Message-ID real del header para el In-Reply-To
                            original_message_id_header = obtener_message_id_header(service, original_internal_id)

                            # Crear el asunto para la respuesta
                            original_asunto = obtener_asunto_original(service, original_internal_id)
                            asunto_reply = f"Re: {original_asunto}" if original_asunto else asunto_personalizado

                            # Enviar el recordatorio como una respuesta en el hilo
                            enviar_recordatorio(
                                service=service,
                                destinatarios=destinatarios,
                                asunto=asunto_reply,
                                mensaje=mensaje_personalizado,
                                thread_id=original_thread_id,  # Usar el threadId para mantener el hilo
                                message_id=original_message_id_header  # Usar el Message-ID para la referencia
                            )
                        except Exception as e:
                            # Si falla la obtención de datos del correo original, enviar como correo nuevo
                            enviar_correo(service, destinatarios, asunto_personalizado, mensaje_personalizado)
                            for correo in data["correos"]:
                                modificar_ultimo_envio(correo)
                    else:
                        # Envío normal
                        enviar_correo(service, destinatarios, asunto_personalizado, mensaje_personalizado)
                        
                        # Solo actualizar ultimo_envio para correos normales
                        for correo in data["correos"]:
                            modificar_ultimo_envio(correo)

                    correos_enviados.append({"empresa": nombre, "destinatarios": destinatarios})
                    await asyncio.sleep(TIEMPO_ENTRE_CORREOS)

                except Exception as e:
                    razon = str(e)
                    fallidos.append({"empresa": nombre, "razon": razon})

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
    tipo: str = Form(...),
    categoria: str = Form(...),
    asunto: str = Form(...),
    mensaje: str = Form(...),
    tipo_envio: str = Form(...),
    cantidad: int = Form(...),
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
        query = get_tipo_correo(tipo, tipo_envio)
        if query:
            cursor.execute(query, (categoria,))
        clientes = cursor.fetchall()

        if not clientes:
            return {"mensaje": "No se encontraron clientes", "fallidos": []}

        contenido_adjunto = await archivo.read()
        es_recordatorio = "Recordatorio" in tipo

        # Agrupar correos por empresa (eliminando duplicados)
        empresas = {}
        clientes_procesados = set()  # Para evitar duplicados por correo
        
        for fila in clientes:
            nombre = fila[0]
            correo = fila[1]
            message_id = fila[2] if len(fila) > 2 else None
            
            correo_limpio = limpiar_correo(correo)
            if not correo_limpio:
                fallidos.append({"mail": correo, "empresa": nombre, "razon": "Correo inválido"})
                continue
            
            # Crear clave única por correo para evitar duplicados
            clave_cliente = correo_limpio.lower()
            if clave_cliente in clientes_procesados:
                continue  # Saltar si ya procesamos este correo
            
            clientes_procesados.add(clave_cliente)
                
            if nombre not in empresas:
                empresas[nombre] = {"correos": set(), "message_ids": set()}
            
            empresas[nombre]["correos"].add(correo_limpio)
            if message_id:
                empresas[nombre]["message_ids"].add(message_id)

        correos_enviados = []
        empresas_items = list(empresas.items())
        max_envios = len(empresas_items) if tipo_envio == "Todos" or cantidad > len(empresas_items) else cantidad

        for i in range(0, max_envios, BATCH_SIZE):
            lote = empresas_items[i:i + BATCH_SIZE]
            for nombre, data in lote:
                try:
                    destinatarios = ", ".join([convertir_a_punycode(c) for c in data["correos"]])
                    asunto_personalizado, mensaje_personalizado = personalizar_mensaje(mensaje, asunto, nombre)

                    if es_recordatorio and data["message_ids"]:
                        try:
                            # Obtener el ID interno del mensaje original
                            original_internal_id = list(data["message_ids"])[0]

                            # Obtener el threadId del mensaje original
                            original_message = service.users().messages().get(userId="me", id=original_internal_id, format="metadata").execute()
                            original_thread_id = original_message.get("threadId")

                            # Obtener el Message-ID real del header para el In-Reply-To
                            original_message_id_header = obtener_message_id_header(service, original_internal_id)

                            # Crear el asunto para la respuesta
                            original_asunto = obtener_asunto_original(service, original_internal_id)
                            asunto_reply = f"Re: {original_asunto}" if original_asunto else asunto_personalizado

                            # Enviar el recordatorio como una respuesta en el hilo
                            enviar_recordatorio(
                                service=service,
                                destinatarios=destinatarios,
                                asunto=asunto_reply,
                                mensaje=mensaje_personalizado,
                                thread_id=original_thread_id,
                                message_id=original_message_id_header,
                                archivo_adjunto=contenido_adjunto,
                                nombre_archivo=archivo.filename
                            )
                        except Exception as e:
                            # Si falla, enviar como correo nuevo con adjunto
                            enviar_correo_con_adjunto(
                                service,
                                destinatarios,
                                asunto_personalizado,
                                mensaje_personalizado,
                                contenido_adjunto,
                                archivo.filename
                            )
                            for correo in data["correos"]:
                                modificar_ultimo_envio(correo)
                    else:
                        # Envío normal con adjunto
                        message_id = enviar_correo_con_adjunto(
                            service,
                            destinatarios,
                            asunto_personalizado,
                            mensaje_personalizado,
                            contenido_adjunto,
                            archivo.filename
                        )
                        
                        # Solo actualizar ultimo_envio para correos normales
                        for correo in data["correos"]:
                            modificar_ultimo_envio(correo)

                    correos_enviados.append({"empresa": nombre, "destinatarios": destinatarios})
                    await asyncio.sleep(TIEMPO_ENTRE_CORREOS)

                except Exception as e:
                    fallidos.append({"empresa": nombre, "razon": str(e)})

            await asyncio.sleep(TIEMPO_ENTRE_LOTES)

        return {
            "mensaje": f"Se enviaron {len(correos_enviados)} correos con adjunto exitosamente",
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