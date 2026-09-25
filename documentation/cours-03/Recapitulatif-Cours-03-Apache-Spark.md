# MTI850 — Analytique des données massives
## Synthèse & Récapitulatif du Cours 03 : Introduction à Apache Spark, DataFrames et Modèle de Calcul

**Enseignant :** Prof. Alessandro L. Koerich  
**Département :** Génie logiciel et des TI, École de technologie supérieure (ÉTS)  
**Session :** Automne 2026  

---

## 1. Genèse, Évolution et Architecture d'Apache Spark

Apache Spark s'est imposé comme le successeur direct et le standard incontesté face à Hadoop MapReduce pour le traitement distribué à grande échelle.

### 1.1 Repères Historiques et Évolution des Versions

* **2009–2014 :** Conçu en 2009 à l'UC Berkeley AMPLab par Matei Zaharia. Open-sourcé en 2010, confié à la fondation Apache en 2013, et couronné *Top-Level Project* en février 2014.
* **Spark 1.x (2014–2016) — Les Fondations :**
  - Introduction de l'abstraction fondamentale **RDD** (*Resilient Distributed Datasets*).
  - Interfaces en Scala, Java, Python et R.
  - Modules phares : Spark Core, Spark SQL, MLlib, GraphX, Spark Streaming.
  - Gain de performance : 10x à 100x plus rapide que Hadoop MapReduce en mémoire.
  - Limites : Code impératif RDD verbeux, peu d'optimisation de requêtes automatique.
* **Spark 2.x (2016–2020) — Unification & Moteurs d'Optimisation :**
  - L'API **DataFrame & Dataset** devient le standard unique de programmation.
  - Déploiement de l'optimiseur déclaratif **Catalyst** et du moteur d'exécution bas-niveau **Tungsten** (mémoire binaire *off-heap*).
  - Introduction de **Structured Streaming** (unification du code batch et temps réel sous une même API de table continue).
* **Spark 3.x (2020–2023) — Matériel Moderne & Exécution Adaptative :**
  - **Adaptive Query Execution (AQE) :** Ré-optimisation dynamique des plans d'exécution pendant le calcul (ajustement automatique du nombre de partitions de shuffle, gestion des biais de données / *skew joins*).
  - Élision dynamique des partitions (*Dynamic Partition Pruning - DPP*).
  - Accélération GPU (NVIDIA RAPIDS), intégration native Kubernetes, et vectorisation Python via Apache Arrow (Pandas UDFs).
* **Spark 4.x (2024–Présent) — Ère du Lakehouse et de l'IA Distribuée :**
  - Échange colonnaire zéro-copie optimisé avec Arrow pour Pandas/PyTorch.
  - Intégration transparente avec les formats de tables Lakehouse (Delta Lake, Apache Iceberg, Apache Hudi).
  - Amélioration des règles d'optimisation ANSI SQL et support des pipelines d'entraînement pour modèles de fondation (LLMs).

---

## 2. Architecture du Système : Driver, Cluster Manager et Executors

Une application Spark fonctionne selon une architecture distribuée maître-esclave coordonnée :

```
                               ┌─────────────────────────────────┐
                               │         DRIVER PROGRAM          │
                               │  - Code principal (Python/JVM)  │
                               │  - SparkSession / SparkContext  │
                               │  - DAG Scheduler / Task Planner │
                               └────────────────┬────────────────┘
                                                │
                                    ┌───────────▼───────────┐
                                    │    CLUSTER MANAGER    │
                                    │ (Standalone/YARN/K8s) │
                                    └─────┬───────────┬─────┘
                                          │           │
                     ┌────────────────────┘           └────────────────────┐
                     ▼                                                     ▼
      ┌─────────────────────────────┐                       ┌─────────────────────────────┐
      │     WORKER NODE (1)         │                       │     WORKER NODE (2)         │
      │ ┌─────────────────────────┐ │                       │ ┌─────────────────────────┐ │
      │ │      SPARK EXECUTOR     │ │                       │ │      SPARK EXECUTOR     │ │
      │ │  - Exécute les tâches   │ │                       │ │  - Exécute les tâches   │ │
      │ │  - Stocke le cache RAM  │ │                       │ │  - Stocke le cache RAM  │ │
      │ └─────────────────────────┘ │                       │ └─────────────────────────┘ │
      └─────────────────────────────┘                       └─────────────────────────────┘
```

### 2.1 Les Acteurs de l'Exécution
1. **Le Driver Program :** 
   - Exécute la fonction `main()` de l'application sur la machine cliente.
   - Crée la `SparkSession`.
   - Transforme le code utilisateur en un graphe acyclique dirigé (**DAG**).
   - Découpe le DAG en étapes (*Stages*) et tâches élémentaires (*Tasks*), puis planifie leur envoi aux nœuds de calcul.
2. **Le Cluster Manager :**
   - Fournit et alloue les ressources physiques globales (cœurs CPU et mémoire).
   - Spark supporte 4 gestionnaires : **Standalone** (intégré), **Hadoop YARN**, **Apache Mesos**, et **Kubernetes**.
3. **Les Executors (Ouvriers) :**
   - Processus JVM tournant sur les nœuds de calcul (*Worker Nodes*).
   - Exécutent en parallèle les tâches individuelles (*Tasks*) envoyées par le Driver.
   - Conservent en mémoire vive ou sur disque local les données mises en cache (`cache()` / `persist()`).

### 2.2 Sous le capot de PySpark : Le pont Py4J
En Python, le code utilisateur n'interagit pas directement avec le matériel du cluster :
- Le Driver Python communique avec une JVM locale via une passerelle de sockets TCP appelée **Py4J**.
- Vos ordres de manipulation de DataFrames sont traduits par la passerelle en objets Java / Scala et soumis au moteur d'optimisation Catalyst.
- **Conséquence directe :** Tant que vous utilisez les opérations natives de DataFrames (`select`, `filter`, `groupBy`), le code s'exécute à la vitesse du C++/Scala dans la JVM sans surcoût d'interpréteur Python !

---

## 3. L'Entrée Unifiée : De SparkContext à SparkSession

Avant Spark 2.0, le développeur devait jongler entre plusieurs contextes hétérogènes (`SparkContext` pour les RDDs, `SQLContext` pour le SQL, `HiveContext` pour les métadonnées Hive).

Depuis Spark 2.0+, la **`SparkSession`** est le point d'entrée unique et universel :

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MonApplicationSpark") \
    .master("local[*]") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

# Accès au SparkContext sous-jacent si nécessaire
sc = spark.sparkContext
```

* **`master("local[*]")` :** Mode local utilisant tous les cœurs logiques disponibles sur votre machine de développement.
* **`master("yarn")` :** Mode cluster déléguant l'allocation à Hadoop YARN.
* **`getOrCreate()` :** Récupère la session active ou en instancie une nouvelle si aucune n'existe (patron singleton).

---

## 4. L'Abstraction Fondamentale : Le DataFrame

Un **DataFrame** est une collection distribuée de lignes d'enregistrements organisées sous des colonnes nommées (conceptuellement identique à une table relationnelle SQL ou une feuille Excel, mais partitionnée sur des centaines de serveurs).

### 4.1 Caractéristiques Majeures
* **Immuabilité (*Immutable*) :** Un DataFrame ne peut jamais être modifié sur place (*in-place*). Toute opération produit un *nouveau* DataFrame.
* **Lignage Résilient (*Lineage Graph*) :** Spark garde en mémoire la recette complète des transformations qui ont servi à construire le DataFrame. En cas de crash d'un serveur, seule la partition manquante est recalculée.
* **Partitionnement distribué :** Les lignes de données sont découpées en blocs appelés **partitions**, traitées chacune par un cœur CPU sur un exécuteur.

### 4.2 Sources de Création de DataFrames
1. **Depuis une collection Python locale :**
   ```python
   data = [("Alice", 25), ("Bob", 30)]
   df = spark.createDataFrame(data, ["nom", "age"])
   ```
2. **Depuis un DataFrame Pandas existant :**
   ```python
   import pandas as pd
   pdf = pd.DataFrame({"nom": ["Alice", "Bob"], "age": [25, 30]})
   df = spark.createDataFrame(pdf)
   ```
3. **Depuis des systèmes de stockage externes (HDFS, S3, Disque local) :**
   ```python
   df_csv = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("data.csv")
   df_json = spark.read.json("blogs.json")
   df_parquet = spark.read.parquet("data.parquet")
   ```

---

## 5. Le Cœur du Modèle : Transformations vs Actions

Toute l'architecture Spark repose sur la distinction nette entre deux types d'opérations :

```
   [ Source de Données ]
            │
            ▼ (Transformations : Paresseuses / Lazy Evaluation)
   [ DataFrame Initial ] ──select()──► [ DF Filtré ] ──groupBy()──► [ DF Agrégé ]
                                                                          │
                                                      (Action Déclenchée) │
                                                                          ▼
                                                                [ Action : show() / count() ]
                                                                * Compile le DAG
                                                                * Optimise via Catalyst
                                                                * Lance les calculs réels !
```

### 5.1 Les Transformations (Évaluation Paresseuse / *Lazy Evaluation*)
Les transformations ne calculent **absolument rien immédiatement**. Elles se contentent d'enregistrer la recette logique dans le graphe de dépendance :
* `select(*cols)` : Sélectionne et projette des colonnes.
* `filter(condition)` ou `where(condition)` : Filtre les lignes selon une expression booléenne.
* `drop(col)` : Supprime une colonne.
* `distinct()` : Élimine les doublons de lignes.
* `sort(*cols)` ou `orderBy(*cols)` : Trie les lignes selon une ou plusieurs colonnes.
* `groupBy(*cols)` : Regroupe les données pour préparer une agrégation (`count()`, `avg()`, `max()`, `sum()`, `agg()`).
* `explode(col)` : Éclate un tableau ou une liste en plusieurs lignes distinctes.

### 5.2 Les Actions (Déclencheurs d'Exécution)
Une action oblige Spark à compiler le graphe, optimiser le plan physique et envoyer les tâches aux exécuteurs pour matérialiser un résultat :
* `show(n)` : Affiche les $n$ premières lignes dans la console formatée.
* `count()` : Retourne le nombre total de lignes du DataFrame.
* `take(n)` : Rapatrie les $n$ premières lignes sous forme de liste d'objets `Row` dans le Driver.
* `collect()` : **Rapatrie la TOTALITÉ du DataFrame en mémoire locale du Driver.**
* `describe()` : Calcule les statistiques descriptives de base (count, mean, stddev, min, max).
* `write.save(...)` : Écrit le jeu de données vers un stockage persistant (Parquet, CSV, base de données).

---

## 6. Bonnes et Mauvaises Pratiques de Programmation

### 6.1 Où tourne le code ? (Driver vs Executors)
* Tout code Python séquentiel en dehors des expressions de DataFrames tourne **sur le Driver** (qui ne dispose que d'une quantité limitée de mémoire RAM).
* Les transformations de DataFrames tournent en parallèle **sur les Executors** (qui cumulent la mémoire de tout le cluster).

### 6.2 L'Anti-Pattern Mortel : Le Piège du `collect()`
Supposons que vous souhaitiez concaténer deux DataFrames `df_a` et `df_b`.

```python
# ❌ L'ERREUR CATASTROPHIQUE À BANNIR ABSOLUMENT :
liste_a = df_a.collect()  # Rapatrie 500 Go sur la RAM du Driver -> CRASH OOM !
liste_b = df_b.collect()
df_final = spark.createDataFrame(liste_a + liste_b)

# ✅ LA BONNE PRATIQUE DISTRIBUÉE :
df_final = df_a.union(df_b)  # Tout le calcul reste distribué sur les exécuteurs !
```

> **Règle d'or de production :**  
> Ne jamais utiliser `.collect()` sur des jeux de données réels. Utilisez toujours `.take(n)` pour inspecter un échantillon ou laissez Spark écrire le résultat directement avec `.write`.

### 6.3 Gestion du Cache Mémoire (`cache()` et `persist()`)
Par défaut, si vous appliquez deux actions successives sur le même DataFrame transformé, **Spark recalcule toute la chaîne depuis le fichier source disque deux fois de suite !**

```python
# Sans cache : lit le fichier source 2 fois
lignes_df = spark.read.text("gros_fichier.log")
commentaires_df = lignes_df.filter(lignes_df.value.startswith("#"))
print(lignes_df.count())        # Exécution 1 (lit tout le fichier)
print(commentaires_df.count())  # Exécution 2 (relit tout le fichier à nouveau !)

# Avec cache : lit le fichier une seule fois et conserve le résultat en RAM
lignes_df = spark.read.text("gros_fichier.log").cache()
commentaires_df = lignes_df.filter(lignes_df.value.startswith("#"))
print(lignes_df.count())        # Lit et met en cache RAM
print(commentaires_df.count())  # Réutilise le cache instantanément !
```

---

## 7. Cas Pratiques : Étude des Scripts de Cours

### 7.1 Comptage et Agrégation des M&Ms (`Count_MnMs_A25.ipynb`)
Le notebook pratique démontre l'utilisation de la chaîne `groupBy` $\rightarrow$ `count` $\rightarrow$ `orderBy` :

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import count

spark = SparkSession.builder.appName("MnMCount").getOrCreate()
mnm_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("mnm_dataset.csv")

# Agrégation par État et par Couleur
count_df = mnm_df.select("State", "Color", "Count") \
    .groupBy("State", "Color") \
    .agg(count("Count").alias("Total")) \
    .orderBy("Total", ascending=False)

count_df.show(10)
```

### 7.2 Analyse de Blogs JSON (`Blogs.ipynb.txt`)
Démontre la capacité de Spark SQL à inférer automatiquement des schémas semi-structurés complexes à partir de documents JSON natifs sans nécessiter de définition préalable de modèle relationnel.
