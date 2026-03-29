# Examen Medio Semestre Correción 
Gabriela Coloma - 00325312

## Contexto del problema

Megaline es una empresa de telecomunicaciones que ofrece planes mensuales con una
combinación de minutos, mensajes y datos móviles. La Gerencia quiere responder:
**¿qué plan es más rentable y por qué?**

La empresa tiene 25 millones de clientes en todo USA, aproximadamente 10 TB de datos.

### Tablas fuente disponibles
- `megaline_users` — catálogo de usuarios, su plan contratado y fechas de creación/deserción
- `megaline_plans` — catálogo de planes con cuota mensual, límites incluidos y tarifas por excedente
- `megaline_calls` — eventos de llamadas (uso). Cada fila es una llamada
- `megaline_messages` — eventos de SMS. Cada fila es un mensaje
- `megaline_internet` — sesiones de internet. Cada fila es una sesión con MB consumidos

---

## Examen Práctico (50 pts)

### Pregunta 1 — Docker Compose (10 pts)
Escribir el `docker-compose.yml` para levantar la infraestructura que se implementaría
para la empresa.

**Respuesta:** [`docker-compose.yml`](Practica/docker-compose.yml)

---

### Pregunta 2 — Data Loader genérico (10 pts)
Crear un data loader genérico para traer los datos de las tablas de la BD con
**reintentos y chunking**. Todo lo que se necesita asumir que ya son variables definidas.

**Respuesta:** [`data_loader.py`](Practica/data_loader.py)

---

### Pregunta 3 — Diagrama ERD capa Gold (10 pts)
Crear el diagrama ERD de modelamiento dimensional que se implementa para la capa Gold
(nombre de las tablas, columnas, relaciones, con esquema de estrella).
La tabla de hechos debe unificar todos los servicios.

**Respuesta:** [`diagrama_ERD_gold.md`](Practica/diagrama_ERD_gold.md)

---

### Pregunta 4 — FROM y JOINs en DBT (10 pts)
Estás creando la tabla de hechos con DBT a partir del diagrama ERD.
Escribe solo el FROM y JOINs con su clave de unión (left, inner o outer) para crearla.

Supuestos:
- Se tiene un archivo `sources.yml` con el source para la tabla de planes y usuarios
- Ya se tienen modelos en silver para cada tabla: `stg_calls`, `stg_internet`, `stg_messages`

**Respuesta:** [`fct_eventos.sql`](Practica/fct_eventos.sql)

---

### Pregunta 5 — PySpark: ingreso promedio por plan en 2025 (10 pts)
Al ser 10 TB de datos se necesita usar Spark para resolver la pregunta de negocio.
Usando PySpark, calcular el ingreso promedio de cada plan en 2025.

Supuestos:
- Usar la tabla de hechos y dimensiones para calcular
- La tabla de hechos tiene datos desde 2015 hasta la actualidad

**Respuesta:** [`PySpark.py`](Practica/PySpark.py)

---

### Extra — CREATE TABLE + Particionamiento Enero-Febrero 2025 (5 pts)
Escribir el código SQL para crear la tabla de hechos y el código para particionar,
escribiendo el particionamiento solo de Enero-Febrero 2025.

**Respuesta:** [`create_partitions.sql`](Practica/create_partitions.sql)

---

## Examen Teórico
**Respuesta:** [`Correción_Teoríca_Examen_Medio_Semestre.pdf`](Correción_Teoríca_Examen_Medio_Semestre.pdf)
