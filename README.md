# 🚀 Proyecto Final - ETL con Azure Databricks

Proyecto desarrollado como parte del curso de **Ingeniería de Datos**, implementando un pipeline ETL con **Azure Databricks**, **PySpark**, **Delta Lake**, **Unity Catalog** y **Power BI**, siguiendo la arquitectura **Medallion (Bronze, Silver y Gold)**.

---

## 📖 Descripción

Este proyecto implementa un proceso ETL completo para transformar datos transaccionales almacenados en archivos CSV dentro de Azure Data Lake Storage Gen2.

El procesamiento se realiza utilizando Azure Databricks, aplicando la arquitectura Medallion para obtener información analítica que posteriormente es consumida mediante un Dashboard Ejecutivo desarrollado en Power BI.

---

## 🏗 Arquitectura del Proyecto

<p align="center">
<img src="images/Arquitectura_ProyectoETL_Databricks.png" width="900">
</p>
---

# 🛠️ Tecnologías Utilizadas

| Tecnología | Uso en el Proyecto |
|------------|--------------------|
| Azure Databricks | Procesamiento y transformación de datos |
| Apache Spark (PySpark) | Motor de procesamiento distribuido |
| Delta Lake | Almacenamiento de tablas Delta |
| Azure Data Lake Storage Gen2 | Almacenamiento de archivos CSV (Raw) |
| Unity Catalog | Gobierno y administración de metadatos |
| SQL | Consultas y transformaciones |
| Python | Desarrollo de notebooks ETL |
| Power BI Desktop | Visualización y análisis de datos |
| GitHub | Control de versiones |
| GitHub Actions | Workflow de Integración Continua (CI) |

---

# 📂 Estructura del Proyecto

```text
Proyecto_Final_Databricks_MME_2026/
│
├── .github/
│   └── workflows/
│       └── ci_databricks.yml
│
├── preamb/
│   ├── 00_Catalog.ipynb
│   ├── 01_UnityCatalog.ipynb
│   ├── 02_ExternalLocation.ipynb
│   └── 03_Schemas.ipynb
│
├── proceso/
│   ├── 01_Bronze.ipynb
│   ├── 02_Silver.ipynb
│   └── 03_Gold.ipynb
│
├── seguridad/
│   └── grant.ipynb
│
├── reversion/
│   └── drop_tablas.ipynb
│
├── dashboard/
│   ├── Proyecto_ETL_Dashboard.pbix
│   └── dashboard.png
│
├── datasets/
│   ├── clientes.csv
│   └── transacciones.csv
│
├── evidencias/
│   ├── arquitectura.png
│   ├── workflow.png
│   └── capturas/
│
└── README.md
```

---

# 🔄 Flujo ETL

El proyecto implementa la arquitectura **Medallion**, organizando el procesamiento de datos en tres capas:

### 🥉 Bronze

- Ingesta de archivos CSV desde Azure Data Lake Storage Gen2.
- Lectura de datos mediante PySpark.
- Almacenamiento en formato Delta sin modificaciones.

### 🥈 Silver

- Limpieza y validación de los datos.
- Estandarización de columnas.
- Corrección de formatos y codificación.
- Eliminación de registros inconsistentes.

### 🥇 Gold

- Construcción de tablas analíticas.
- Generación de indicadores (KPIs).
- Resúmenes por ciudad, cliente, estado y tipo de transacción.
- Preparación de la información para consumo en Power BI.
