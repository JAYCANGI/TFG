# 📣 Detección de Sesgos Informativos en Prensa sobre Inmigración — TFG

Este repositorio contiene el código, datos y resultados de mi Trabajo de Fin de Grado, en el que he desarrollado una metodología de Procesamiento de Lenguaje Natural (PLN) para detectar sesgos informativos en artículos de prensa.

---

## 📝 Resumen del Proyecto

- **🎯 Objetivo:** Identificar sesgos informativos en 3 840 artículos sobre inmigración extraídos de cinco periódicos digitales españoles.  
- **🛠 Metodología:**
  1. **🌐 Web Scraping** de cinco periódicos digitales para extraer URLs y contenido completo de artículos sobre inmigración.  
  2. **🔍 Normalización y limpieza** de texto (eliminación de HTML, caracteres especiales, boilerplate).  
  3. **📊 Vectorización** con SpaCy para representación numérica de los documentos.  
  4. **📈 Cálculo de métricas:**  
     - **😊 Sentimiento** con VADER (positivo/negativo/neutral).  
     - **📰 Postura ideológica** mediante TF-IDF.  
     - **🏷 Entidades nombradas** (NER) con SpaCy.  
  5. **🔗 Clustering** con K-Means en cuatro configuraciones distintas.  


---

## 📊 Resultados Obtenidos

- **🏆 Mejor configuración de K-Means:**  
  - Combinación de vectores de sentimiento y postura ideológica.  
  - **Índice Silhouette:** 0,541  
  - **Davies-Bouldin:** 0,624  
- **🧩 Clústeres identificados:**  
  - **🔴 Grupo 1:** Artículos con lenguaje claramente negativo.  
  - **⚪ Grupo 2:** Artículos con tono neutral.  


---

## ⚙️ Estructura del Repositorio
- **data/**
  - **raw/**:  
    Contiene los JSON con los artículos tal y como se obtienen originalmente.
  - **processed/**:  
    Bases de datos generadas tras el preprocesamiento, con los textos ya limpios, sus vectores, etiquetas asignadas y resultados de clustering.

- **src/**
  - **pipelines/**:  
    Scripts para limpieza de texto, normalización, tokenización y demás transformaciones.
  - **scrapers/**:  
    Código para extraer datos de la web (web scraping).
  - **visualizations/**:  
    Gráficos y visualizaciones previas y posteriores al proceso de limpieza de datos.

- **models/**  
  Modelos entrenados para:
  - Etiquetación automática
  - Clustering de documentos
  - Gestión de corpus de palabras positivas y negativas

- **requirements.txt**  
  Lista de dependencias necesarias para ejecutar el proyecto en Python.

- **README.md**  
  Documento de referencia con la descripción completa del proyecto, instrucciones de instalación y uso.
