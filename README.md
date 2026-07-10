# API de Procesamiento de Datos de Subastas en Tiempo Real

Este repositorio contiene la implementación de un microservicio de ingesta de datos desarrollado en Python con Flask. El sistema está diseñado para capturar, validar, limpiar y exponer datos de subastas tecnológicas, actuando como una capa de persistencia y transformación dentro de una arquitectura de Big Data.

## Descripción del Proyecto

El proyecto implementa un pipeline de datos que permite la ingesta de eventos en tiempo real, garantizando la integridad, trazabilidad y calidad de la información. El sistema centraliza la recepción de datos, elimina duplicados mediante un algoritmo de idempotencia y prepara los conjuntos de datos para su explotación analítica en herramientas de Business Intelligence como Power BI.

## Arquitectura del Sistema

El pipeline se divide en tres capas funcionales:

1. **Capa Bronze (Persistencia Cruda):** Almacenamiento secuencial de alta velocidad en formato JSON Lines (`.jsonl`).
2. **Capa de Procesamiento (Transformación):** Limpieza, normalización de tipos de datos, validación de registros y eliminación de duplicados.
3. **Capa de Consumo (Gold Layer):** Exposición de datos normalizados mediante servicios REST para consumo analítico.

---

# Cómo utilizar la API

Una vez desplegada la aplicación (por ejemplo en Render), la API queda disponible mediante una URL pública.

## 1. Enviar datos

Los datos deben enviarse mediante una solicitud **HTTP POST** al endpoint:

```
POST /recibir-datos
```

Ejemplo de cuerpo JSON:

```json
{
  "fecha_registro": "2026-07-10T10:30:00",
  "id_cliente": "C001",
  "cliente": "Juan Pérez",
  "genero": "M",
  "id_producto": "P100",
  "producto": "NVIDIA",
  "precio": 850,
  "cantidad": 2,
  "forma_pago": "Crédito"
}
```

Al recibir el evento, la API:

* valida que el JSON sea correcto;
* agrega automáticamente la fecha de recepción (`fecha_recepcion_api`);
* almacena el registro en formato JSON Lines;
* mantiene la trazabilidad del dato;
* deja disponible la información para su procesamiento.

---

## 2. Consultar los datos almacenados

Para visualizar los registros originales recibidos por la API:

```
GET /ver-datos
```

Este endpoint muestra los datos tal como fueron almacenados en la capa Bronze.

---

## 3. Obtener datos procesados

Para consultar los registros limpios y validados:

```
GET /datos-limpios
```

Durante este proceso la aplicación:

* normaliza textos;
* convierte tipos de datos;
* calcula automáticamente el monto cuando es necesario;
* valida campos obligatorios;
* elimina registros duplicados;
* genera el estado de validación.

---

## 4. Descargar los datos para análisis

Para generar un archivo CSV listo para herramientas de análisis:

```
GET /descargar-csv
```

Este endpoint entrega la información preparada para Power BI u otras plataformas de Business Intelligence.

---

## 5. Obtener indicadores generales

Para visualizar un resumen del procesamiento:

```
GET /resumen
```

El endpoint entrega métricas como:

* total de registros;
* monto total procesado;
* cantidad de registros válidos;
* distribución por producto;
* distribución por forma de pago.

---

## 6. Verificar el estado del servicio

Para comprobar que la API se encuentra operativa:

```
GET /
```

Para obtener información de diagnóstico:

```
GET /debug
```

Este endpoint permite verificar información técnica utilizada para depuración del sistema.

---

# Flujo del procesamiento

El funcionamiento completo de la aplicación es el siguiente:

1. La fuente de datos envía eventos mediante HTTP POST.
2. Flask recibe el evento.
3. Se valida la estructura del JSON.
4. Se agrega la fecha de recepción.
5. El registro se almacena en formato JSON Lines (Bronze).
6. Al consultar los datos, se ejecuta el proceso de limpieza y transformación.
7. Se normalizan los campos.
8. Se validan los registros.
9. Se eliminan duplicados mediante una clave lógica.
10. Los datos limpios se publican mediante los distintos endpoints.
11. Power BI consume automáticamente el endpoint `/descargar-csv` para generar el dashboard.

---

# Endpoints de la API

| Método | Endpoint         | Descripción                                   |
| ------ | ---------------- | --------------------------------------------- |
| POST   | `/recibir-datos` | Recibe eventos en formato JSON.               |
| GET    | `/`              | Verifica que la API esté funcionando.         |
| GET    | `/ver-datos`     | Muestra los registros originales almacenados. |
| GET    | `/datos-limpios` | Entrega los datos procesados y validados.     |
| GET    | `/descargar-csv` | Descarga un archivo CSV limpio.               |
| GET    | `/resumen`       | Devuelve métricas agregadas del sistema.      |
| GET    | `/debug`         | Información técnica para diagnóstico.         |

---

# Especificaciones Técnicas

* **Lenguaje:** Python.
* **Framework:** Flask.
* **Servidor:** Gunicorn.
* **Formato de almacenamiento:** JSON Lines (`.jsonl`).
* **Arquitectura:** REST.
* **Persistencia:** Estrategia Append-Only.
* **Control de calidad:** Validación mediante `estado_validacion`.
* **Control de duplicidad:** Claves lógicas para garantizar idempotencia.
* **Despliegue:** Render.

---

# Integración con Power BI

El sistema está configurado para permitir la automatización del flujo de datos hacia Power BI:

1. Conectarse mediante **Obtener datos → Web** utilizando el endpoint `/descargar-csv`.
2. Configurar los tipos de datos en **Power Query**.
3. Crear medidas DAX según las necesidades del análisis.
4. Diseñar dashboards interactivos utilizando los datos procesados por la API.

Cada actualización del informe consulta nuevamente el endpoint, permitiendo visualizar la información más reciente disponible.

---

# Licencia

Desarrollado como parte de la infraestructura de datos del proyecto académico para Duoc UC.
