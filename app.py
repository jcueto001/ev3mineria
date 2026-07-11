from flask import Flask, request, jsonify, Response
from datetime import datetime, timezone
from google.cloud import pubsub_v1
from google.oauth2 import service_account
import json
import os
import csv
from io import StringIO


app = Flask(__name__)


# =========================================================
# CONFIGURACIÓN GENERAL
# =========================================================

DATA_FILE = os.path.join(os.getcwd(), "datos_realtime.jsonl")

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "")
PUBSUB_TOPIC_ID = os.environ.get("PUBSUB_TOPIC_ID", "")
GCP_SERVICE_ACCOUNT_JSON = os.environ.get(
    "GCP_SERVICE_ACCOUNT_JSON",
    ""
)

publisher = None
topic_path = None


COLUMNAS_CSV = [
    "fecha_recepcion_api",
    "fecha_registro",
    "id_cliente",
    "cliente",
    "genero",
    "id_producto",
    "producto",
    "precio",
    "cantidad",
    "monto",
    "forma_pago",
    "estado_validacion",
    "observaciones"
]


# =========================================================
# CONFIGURACIÓN DE PUB/SUB
# =========================================================

def configurar_pubsub():
    global publisher, topic_path

    if not GCP_PROJECT_ID:
        print(
            "ADVERTENCIA: GCP_PROJECT_ID no está configurado",
            flush=True
        )
        return

    if not PUBSUB_TOPIC_ID:
        print(
            "ADVERTENCIA: PUBSUB_TOPIC_ID no está configurado",
            flush=True
        )
        return

    if not GCP_SERVICE_ACCOUNT_JSON:
        print(
            "ADVERTENCIA: GCP_SERVICE_ACCOUNT_JSON no está configurado",
            flush=True
        )
        return

    try:
        credenciales_info = json.loads(
            GCP_SERVICE_ACCOUNT_JSON
        )

        credenciales = (
            service_account.Credentials
            .from_service_account_info(
                credenciales_info
            )
        )

        publisher = pubsub_v1.PublisherClient(
            credentials=credenciales
        )

        topic_path = publisher.topic_path(
            GCP_PROJECT_ID,
            PUBSUB_TOPIC_ID
        )

        print(
            "PUB/SUB CONFIGURADO CORRECTAMENTE:",
            topic_path,
            flush=True
        )

    except json.JSONDecodeError as error:
        print(
            "ERROR: GCP_SERVICE_ACCOUNT_JSON no contiene JSON válido:",
            str(error),
            flush=True
        )

    except Exception as error:
        print(
            "ERROR CONFIGURANDO PUB/SUB:",
            str(error),
            flush=True
        )


configurar_pubsub()


# =========================================================
# ENDPOINTS
# =========================================================

@app.route("/", methods=["GET"])
def inicio():
    return jsonify({
        "mensaje": "API funcionando correctamente",
        "pubsub_configurado": publisher is not None,
        "topic": topic_path
    }), 200


@app.route("/recibir-datos", methods=["POST"])
def recibir_datos():
    data = request.get_json(silent=True)

    if data is None:
        return jsonify({
            "mensaje": "No se recibió JSON válido"
        }), 400

    fecha_recepcion = (
        datetime.now(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )

    registro = {
        "fecha_recepcion": fecha_recepcion,
        "data": data
    }

    print(
        "POST RECIBIDO:",
        registro,
        flush=True
    )

    print(
        "GUARDANDO EN:",
        DATA_FILE,
        flush=True
    )

    # Mantiene el almacenamiento crudo en JSONL.
    try:
        with open(
            DATA_FILE,
            "a",
            encoding="utf-8"
        ) as archivo:
            archivo.write(
                json.dumps(
                    registro,
                    ensure_ascii=False
                ) + "\n"
            )

    except OSError as error:
        print(
            "ERROR GUARDANDO JSONL:",
            str(error),
            flush=True
        )

        return jsonify({
            "mensaje": "Error guardando el registro",
            "error": str(error)
        }), 500

    # Duoc puede enviar un objeto o una lista.
    if isinstance(data, dict):
        items = [data]

    elif isinstance(data, list):
        items = data

    else:
        return jsonify({
            "mensaje": (
                "El contenido JSON debe ser "
                "un objeto o una lista"
            )
        }), 400

    publicados = []
    errores_pubsub = []
    duplicados_lote = set()

    for item in items:
        if not isinstance(item, dict):
            errores_pubsub.append(
                "Se ignoró un elemento porque no era un objeto JSON"
            )
            continue

        fila_limpia = transformar_item(
            item,
            fecha_recepcion
        )

        # Evita duplicados dentro del mismo POST.
        clave = crear_clave_duplicado(
            fila_limpia
        )

        if clave in duplicados_lote:
            print(
                "REGISTRO DUPLICADO OMITIDO:",
                fila_limpia,
                flush=True
            )
            continue

        duplicados_lote.add(clave)

        try:
            message_id = publicar_en_pubsub(
                fila_limpia
            )

            if message_id is not None:
                publicados.append({
                    "message_id": message_id,
                    "producto": fila_limpia["producto"],
                    "estado_validacion": (
                        fila_limpia["estado_validacion"]
                    )
                })

            else:
                errores_pubsub.append(
                    "Pub/Sub no está configurado"
                )

        except Exception as error:
            mensaje_error = str(error)

            print(
                "ERROR PUBLICANDO EN PUB/SUB:",
                mensaje_error,
                flush=True
            )

            errores_pubsub.append(
                mensaje_error
            )

    return jsonify({
        "mensaje": "Datos recibidos correctamente",
        "archivo": DATA_FILE,
        "total_registros_crudos": contar_registros_crudos(),
        "cantidad_elementos_recibidos": len(items),
        "mensajes_publicados_pubsub": len(publicados),
        "publicados": publicados,
        "errores_pubsub": errores_pubsub,
        "data": data
    }), 200


@app.route("/ver-datos", methods=["GET"])
def ver_datos():
    datos = leer_registros_crudos()

    return jsonify({
        "mensaje": (
            "Datos encontrados"
            if datos
            else "Aún no hay datos recibidos"
        ),
        "archivo": DATA_FILE,
        "total_registros_crudos": len(datos),
        "datos": datos
    }), 200


@app.route("/datos-limpios", methods=["GET"])
def datos_limpios():
    filas = obtener_datos_limpios()

    return jsonify({
        "mensaje": "Datos limpios generados correctamente",
        "total_registros_limpios": len(filas),
        "datos": filas
    }), 200


@app.route("/descargar-csv", methods=["GET"])
def descargar_csv():
    filas = obtener_datos_limpios()

    salida = StringIO()

    writer = csv.DictWriter(
        salida,
        fieldnames=COLUMNAS_CSV
    )

    writer.writeheader()
    writer.writerows(filas)

    csv_texto = salida.getvalue()

    return Response(
        csv_texto,
        mimetype="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": (
                "attachment; "
                "filename=datos_realtime_limpios.csv"
            )
        }
    )


@app.route("/resumen", methods=["GET"])
def resumen():
    filas = obtener_datos_limpios()

    total_monto = sum(
        fila["monto"]
        for fila in filas
        if isinstance(
            fila["monto"],
            (int, float)
        )
    )

    total_cantidad = sum(
        fila["cantidad"]
        for fila in filas
        if isinstance(
            fila["cantidad"],
            int
        )
    )

    productos = {}
    formas_pago = {}

    for fila in filas:
        producto = (
            fila["producto"]
            or "Sin producto"
        )

        forma_pago = (
            fila["forma_pago"]
            or "Sin forma de pago"
        )

        productos[producto] = (
            productos.get(producto, 0) + 1
        )

        formas_pago[forma_pago] = (
            formas_pago.get(forma_pago, 0) + 1
        )

    return jsonify({
        "total_registros_limpios": len(filas),
        "total_cantidad": total_cantidad,
        "total_monto": round(total_monto, 2),
        "productos": productos,
        "formas_pago": formas_pago
    }), 200


@app.route("/debug", methods=["GET"])
def debug():
    return jsonify({
        "cwd": os.getcwd(),
        "archivo": DATA_FILE,
        "existe_archivo": os.path.exists(DATA_FILE),
        "gcp_project_id": GCP_PROJECT_ID,
        "pubsub_topic_id": PUBSUB_TOPIC_ID,
        "pubsub_configurado": publisher is not None,
        "topic_path": topic_path
    }), 200


# =========================================================
# PUB/SUB
# =========================================================

def publicar_en_pubsub(fila):
    if publisher is None or topic_path is None:
        print(
            "PUB/SUB NO ESTÁ CONFIGURADO",
            flush=True
        )
        return None

    mensaje_bytes = json.dumps(
        fila,
        ensure_ascii=False
    ).encode("utf-8")

    futuro = publisher.publish(
        topic_path,
        mensaje_bytes,
        origen="render-flask",
        tipo_evento="venta_realtime"
    )

    message_id = futuro.result(
        timeout=30
    )

    print(
        "MENSAJE PUBLICADO EN PUB/SUB:",
        message_id,
        fila,
        flush=True
    )

    return message_id


# =========================================================
# LECTURA Y TRANSFORMACIÓN
# =========================================================

def leer_registros_crudos():
    if not os.path.exists(DATA_FILE):
        return []

    datos = []

    try:
        with open(
            DATA_FILE,
            "r",
            encoding="utf-8"
        ) as archivo:

            for linea in archivo:
                linea = linea.strip()

                if not linea:
                    continue

                try:
                    datos.append(
                        json.loads(linea)
                    )

                except json.JSONDecodeError:
                    print(
                        "LÍNEA INVÁLIDA IGNORADA:",
                        linea,
                        flush=True
                    )

    except OSError as error:
        print(
            "ERROR LEYENDO JSONL:",
            str(error),
            flush=True
        )

    return datos


def contar_registros_crudos():
    return len(
        leer_registros_crudos()
    )


def obtener_datos_limpios():
    registros = leer_registros_crudos()

    filas = []
    duplicados = set()

    for registro in registros:
        fecha_recepcion = registro.get(
            "fecha_recepcion",
            ""
        )

        data = registro.get(
            "data",
            []
        )

        if isinstance(data, dict):
            items = [data]

        elif isinstance(data, list):
            items = data

        else:
            continue

        for item in items:
            if not isinstance(item, dict):
                continue

            fila = transformar_item(
                item,
                fecha_recepcion
            )

            clave_duplicado = (
                crear_clave_duplicado(
                    fila
                )
            )

            if clave_duplicado in duplicados:
                continue

            duplicados.add(
                clave_duplicado
            )

            filas.append(
                fila
            )

    filas.sort(
        key=lambda fila: (
            fila["fecha_registro"]
            or fila["fecha_recepcion_api"]
        )
    )

    return filas


def transformar_item(item, fecha_recepcion):
    precio = convertir_float(
        item.get("precio")
    )

    cantidad = convertir_int(
        item.get("cantidad")
    )

    monto = convertir_float(
        item.get("monto")
    )

    # Si no viene monto, se calcula.
    if (
        monto is None
        and precio is not None
        and cantidad is not None
    ):
        monto = round(
            precio * cantidad,
            2
        )

    producto = limpiar_texto(
        item.get("producto")
    )

    cliente = limpiar_texto(
        item.get("cliente")
    )

    forma_pago = limpiar_texto(
        item.get("forma_pago")
    )

    genero = limpiar_texto(
        item.get("genero")
    )

    observaciones = []

    if not producto:
        observaciones.append(
            "Producto vacío"
        )

    if precio is None:
        observaciones.append(
            "Precio inválido o vacío"
        )

    if cantidad is None:
        observaciones.append(
            "Cantidad inválida o vacía"
        )

    if monto is None:
        observaciones.append(
            "Monto inválido o vacío"
        )

    estado_validacion = (
        "OK"
        if not observaciones
        else "OBSERVADO"
    )

    return {
        "fecha_recepcion_api": fecha_recepcion,
        "fecha_registro": limpiar_texto(
            item.get("fecreg")
        ),
        "id_cliente": limpiar_texto(
            item.get("id_cliente")
        ),
        "cliente": cliente,
        "genero": genero,
        "id_producto": limpiar_texto(
            item.get("id_producto")
        ),
        "producto": producto,
        "precio": (
            precio
            if precio is not None
            else 0.0
        ),
        "cantidad": (
            cantidad
            if cantidad is not None
            else 0
        ),
        "monto": (
            monto
            if monto is not None
            else 0.0
        ),
        "forma_pago": forma_pago,
        "estado_validacion": estado_validacion,
        "observaciones": "; ".join(
            observaciones
        )
    }


def crear_clave_duplicado(fila):
    campos_clave = {
        "fecha_registro": fila["fecha_registro"],
        "id_cliente": fila["id_cliente"],
        "cliente": fila["cliente"],
        "id_producto": fila["id_producto"],
        "producto": fila["producto"],
        "precio": fila["precio"],
        "cantidad": fila["cantidad"],
        "monto": fila["monto"],
        "forma_pago": fila["forma_pago"]
    }

    return json.dumps(
        campos_clave,
        sort_keys=True,
        ensure_ascii=False
    )


def limpiar_texto(valor):
    if valor is None:
        return ""

    return str(valor).strip()


def convertir_float(valor):
    if valor is None or valor == "":
        return None

    try:
        texto = str(valor).strip()

        texto = (
            texto
            .replace("$", "")
            .replace(" ", "")
        )

        if "," in texto and "." in texto:
            texto = (
                texto
                .replace(".", "")
                .replace(",", ".")
            )
        else:
            texto = texto.replace(
                ",",
                "."
            )

        return float(texto)

    except (
        ValueError,
        TypeError
    ):
        return None


def convertir_int(valor):
    if valor is None or valor == "":
        return None

    try:
        texto = (
            str(valor)
            .strip()
            .replace(",", ".")
        )

        return int(
            float(texto)
        )

    except (
        ValueError,
        TypeError
    ):
        return None


# =========================================================
# EJECUCIÓN LOCAL
# =========================================================

if __name__ == "__main__":
    puerto = int(
        os.environ.get(
            "PORT",
            5000
        )
    )

    app.run(
        host="0.0.0.0",
        port=puerto
    )








