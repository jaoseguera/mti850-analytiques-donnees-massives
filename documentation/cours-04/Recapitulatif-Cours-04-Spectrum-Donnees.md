# MTI850 — Analytique des données massives
## Synthèse & Récapitulatif du Cours 04 : Le Spectre des Données, Architectures de Stockage et Opérations Spark SQL / DataFrames

**Enseignant :** Prof. Alessandro L. Koerich  
**Département :** Génie logiciel et des TI, École de technologie supérieure (ÉTS)  
**Session :** Automne 2026  

---

## 1. Fondements de Gestion des Données : Modèle vs Schéma

Tout système d'information distribué repose sur deux concepts directeurs fondamentaux :

* **Modèle de données (*Data Model*) :**  
  Cadre conceptuel de haut niveau définissant la manière dont les données sont structurées, reliées et manipulées.
  - *Exemple :* Le modèle relationnel structure l'information en relations (tables), tuples (lignes) et attributs (colonnes), reliées par des clés et interrogées via l'algèbre relationnelle (SQL).
* **Schéma (*Schema*) :**  
  Implémentation concrète et spécifique d'un modèle de données pour une collection particulière. Il formalise le nom des champs, leur type de données strict (`STRING`, `INT`, `TIMESTAMP`, etc.) et leurs contraintes de nullité.

---

## 2. Le Spectre de la Structure des Données (*The Structure Spectrum*)

Les données générées dans le monde numérique ne se limitent pas à des tables relationnelles bien ordonnées. Elles se distribuent sur un spectre continu caractérisé par leur niveau de contrainte structurelle :

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                             LE SPECTRE DE STRUCTURE                              │
├──────────────────────────┬───────────────────────────┬───────────────────────────┤
│    DONNÉES STRUCTURÉES   │  DONNÉES SEMI-STRUCTURÉES │   DONNÉES NON STRUCTURÉES │
│     (Schema-First)       │       (Schema-Later)      │       (Schema-Never)      │
├──────────────────────────┼───────────────────────────┼───────────────────────────┤
│ • Schéma strict fixé     │ • Structure auto-         │ • Aucun format tabulaire  │
│   avant l'écriture.      │   descriptive & flexible. │   ni schéma inhérent.     │
│ • Tables SGBDR, CSV,     │ • Paires clé-valeur,      │ • Texte brut libre,       │
│   Parquet, ORC, Excel.   │   balises hiérarchiques.  │   images, audio, vidéo,   │
│ • Idéal pour requêtes    │ • JSON, XML, HTML,        │   séquences génomiques.   │
│   SQL et SGBD classiques.│   e-mails, logs, posts.   │ • Nécessite extraction    │
│ • Représente ~20%        │ • Nécessite un parsing    │   de caractéristiques     │
│   des données mondiales. │   (schéma inféré/extrait).│   (NLP, Vision par IA).   │
└──────────────────────────┴───────────────────────────┴───────────────────────────┘
```

### 2.1 Analyse par Type de Données
1. **Données Structurées (*Schema-First*) :**
   - L'application émettrice et le système de stockage s'accordent préalablement sur le schéma.
   - Accès prédictible, optimisations de stockage colonnaire poussées (compression par dictionnaire).
2. **Données Semi-Structurées (*Schema-Later*) :**
   - **Exemple de l'E-mail :** Les en-têtes (`From`, `To`, `Subject`, `Date`) sont hautement structurés, tandis que le corps du message est du texte libre non structuré.
   - **Réseaux sociaux (Posts X/Twitter, Facebook, Instagram) :** Métadonnées relationnelles précises (identifiant, horodatage, compteur de mentions "J'aime") combinées à du texte libre, des hashtags et des fichiers multimédias.
   - **JSON / XML :** Les types de données peuvent varier d'une ligne à l'autre (*Schema Drift*).
3. **Données Non Structurées (*Schema-Never*) :**
   - Aucune règle syntaxique intrinsèque : images CCTV, flux vidéo YouTube, ondes acoustiques, corpus de texte brut.
   - **Le défi WYOC (*Write Your Own Code*) :** « Imposer une structure sur la donnée brute ». Spark et Hadoop agissent comme des magasins de données opérationnels (*Operational Data Store - ODS*) où les données brutes sont ingérées puis transformées par des pipelines de traitement batch ou streaming.

---

## 3. Comparatif des Systèmes de Stockage : HDFS vs Stockage Objet Cloud

Le cours formalise la bascule entre les systèmes de fichiers distribués traditionnels sur site (*on-premise*) et les magasins d'objets cloud gérés :

| Caractéristique | HDFS (Hadoop) | AWS S3 | Google Cloud Storage (GCS) | Azure Blob Storage |
| :--- | :--- | :--- | :--- | :--- |
| **Type de Stockage** | Système de fichiers distribué hiérarchique | Stockage Objet Plat (*Object Storage*) | Stockage Objet Plat | Stockage Objet Plat |
| **Organisation** | Fichiers découpés en blocs (64/128/256 Mo) sur DataNodes | *Buckets* contenant des objets (`clé` + données + métadonnées) | *Buckets* avec objets | *Containers* avec blobs (Block, Append, Page) |
| **Gestion de l'Infra** | Manuelle (administration des serveurs, NameNode) | Entièrement infogérée (*Serverless*) | Entièrement infogérée | Entièrement infogérée |
| **Mise à l'échelle** | Ajout physique de machines au cluster | Virtuellement infinie et automatique | Virtuellement infinie et automatique | Virtuellement infinie et automatique |
| **Taille d'un Objet** | Limité par l'espace agrégé du cluster | De quelques Ko jusqu'à **5 To** par objet | Jusqu'à 5 To par objet | Jusqu'à 5 To par blob |
| **Durabilité** | Dépend de la réplication (ex. x3) et santé disques | **99,999999999%** (11 neuf) | **99,999999999%** | **99,999999999%** |
| **Cohérence (*Consistency*)**| Cohérence forte immédiate au sein du cluster | Cohérence forte en lecture-après-écriture (*Strong Read-After-Write*) | Cohérence forte globale | Cohérence forte |
| **Sémantique de Dossiers**| Vrais répertoires physiques POSIX | Pas de répertoires réels : simulés par des préfixes de clés (`/`) | Simulés par préfixes | Hiérarchie activable (ADLS Gen2) |
| **Coût** | CapEx (achat matériel) + OpEx (maintenance 24/7) | Facturation à l'usage (Go stocké + requêtes API REST) | Facturation à l'usage | Facturation à l'usage avec paliers (Hot, Cool, Archive) |

---

## 4. Pratique Avancée : L'API DataFrameReader et DataFrameWriter

Spark fournit une interface unifiée de lecture et d'écriture pour l'ensemble des formats de données :

```python
# Modèle universel de lecture
df = spark.read.format("format") \
    .option("clé", "valeur") \
    .schema(schema_explicite) \
    .load("chemin")

# Modèle universel d'écriture
df.write.format("format") \
    .mode("overwrite|append|ignore|errorIfExists") \
    .option("clé", "valeur") \
    .save("chemin")
```

### 4.1 Formats de Fichiers Pris en Charge
1. **CSV :**
   - Format texte tabulaire universel mais dépourvu de typage natif.
   - *Bonne pratique :* Éviter `option("inferSchema", "true")` en production car cette option force Spark à lire le jeu de données en entier deux fois. Définir un schéma DDL explicite.
2. **Parquet :**
   - Format colonnaire binaire open-source optimisé pour Spark. Schéma auto-descriptif intégré, compression Snappy par défaut, statistiques min/max par bloc.
3. **JSON :**
   - Lecture de structures imbriquées et de tableaux.
4. **Avro :**
   - Format d'encodage binaire orienté ligne avec schéma JSON compact, référence de l'industrie pour les flux de messagerie temps réel (Apache Kafka).
5. **ORC (*Optimized Row Columnar*) :**
   - Format colonnaire alternatif hautement compressé, originaire de l'écosystème Apache Hive.
6. **Fichiers Binaires et Images (`binaryFile`) :**
   - Ingestion directe de données multimédias (images de vidéosurveillance CCTV) sous forme de flux binaire brut avec extraction de métadonnées de chemin :
   ```python
   bin_df = spark.read.format("binaryFile") \
       .option("recursiveFileLookup", "true") \
       .option("pathGlobFilter", "*.jpg") \
       .load("data/cctvVideos/train_images/")
   ```

---

## 5. Tables SQL Gérées vs Non Gérées et Vues Temporaires

Spark SQL permet d'exposer des DataFrames sous forme de tables et vues relationnelles interrogeables via du SQL ANSI standard.

```
                                  CATALOGUE METASTORE SPARK SQL
                                                 │
                     ┌───────────────────────────┴───────────────────────────┐
                     ▼                                                       ▼
        ┌─────────────────────────────┐                         ┌─────────────────────────────┐
        │   MANAGED TABLE (GÉRÉE)     │                         │  UNMANAGED / EXTERNAL TABLE │
        │                             │                         │                             │
        │ • Métadonnées gérées par    │                         │ • Métadonnées gérées par    │
        │   Spark / Hive Metastore.   │                         │   Spark / Hive Metastore.   │
        │ • Données physiques dans    │                         │ • Données physiques dans    │
        │   le dossier par défaut.    │                         │   un emplacement externe    │
        │                             │                         │   spécifié (`LOCATION`).    │
        │ ⚠️ DROP TABLE supprime       │                         │ 🛡️ DROP TABLE supprime       │
        │   LES MÉTADONNÉES ET LES    │                         │   UNIQUEMENT LES            │
        │   FICHIERS SUR DISQUE !     │                         │   MÉTADONNÉES (données OK). │
        └─────────────────────────────┘                         └─────────────────────────────┘
```

### 5.1 Tables Gérées (*Managed Tables*) vs Tables Non Gérées (*External Tables*)
* **Table Gérée (*Managed*) :**
  ```python
  flights_df.write.saveAsTable("managed_us_delay_flights_tbl")
  ```
  Spark contrôle le cycle de vie complet. Si un analyste exécute `DROP TABLE managed_us_delay_flights_tbl`, les données physiques sont définitivement effacées du stockage.
* **Table Externe / Non Gérée (*Unmanaged*) :**
  ```python
  flights_df.write.option("path", "/data/us_flights_delay").saveAsTable("us_delay_flights_tbl")
  ```
  Le système enregistre un pointeur vers un répertoire externe. Si la table est supprimée (`DROP TABLE`), seul le catalogue de métadonnées est effacé. **Les fichiers sources Parquet/CSV restent intacts sur le stockage.**

### 5.2 Vues Temporaires Locales vs Globales
* **Vue Temporaire Locale :**
  ```python
  df.createOrReplaceTempView("us_delay_flights_tbl")
  ```
  Accessible uniquement au sein de la `SparkSession` courante. Elle disparaît dès que la session se ferme.
* **Vue Temporaire Globale :**
  ```python
  df.createOrReplaceGlobalTempView("us_origin_airport_SFO_global_tmp_view")
  ```
  Visible par toutes les `SparkSession` de l'application. Elle réside dans la base de données réservée `global_temp` et s'interroge via `SELECT * FROM global_temp.us_origin_airport_SFO_global_tmp_view`.

---

## 6. Analyse Pratique : Cas des Appels d'Urgence de San Francisco (`sf-fire-calls.csv`)

Le notebook d'atelier illustre les transformations essentielles appliquées sur un grand jeu de données semi-structuré converti en table relationnelle :

### 6.1 Définition d'un Schéma StructType Robuste
Au lieu de subir le coût d'inférence, le schéma est formellement typé :
```python
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, FloatType

fire_schema = StructType([
    StructField("CallNumber", IntegerType(), True),
    StructField("UnitID", StringType(), True),
    StructField("IncidentNumber", IntegerType(), True),
    StructField("CallType", StringType(), True),
    StructField("CallDate", StringType(), True),
    StructField("Delay", FloatType(), True)
])

fire_df = spark.read.schema(fire_schema).option("header", "true").csv("sf-fire-calls.csv")
```

### 6.2 Transformations et Conversions Temporelles
```python
from pyspark.sql.functions import col, to_timestamp, year, month

# 1. Renommage d'une colonne (Immuable)
new_fire_df = fire_df.withColumnRenamed("Delay", "ResponseDelayedinMins")

# 2. Conversion de chaînes textuelles en timestamps natifs
fire_ts_df = new_fire_df \
    .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy")) \
    .drop("CallDate")

# 3. Agrégations métier : types d'appels d'urgence les plus fréquents
fire_ts_df.select("CallType") \
    .where(col("CallType").isNotNull()) \
    .groupBy("CallType") \
    .count() \
    .orderBy("count", ascending=False) \
    .show(10, truncate=False)
```
