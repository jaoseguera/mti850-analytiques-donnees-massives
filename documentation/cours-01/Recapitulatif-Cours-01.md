# MTI850 — Analytique des données massives
## Synthèse & Récapitulatif du Cours 01 : Présentation et Fondements

**Enseignant :** Prof. Alessandro L. Koerich  
**Département :** Génie logiciel et des technologies de l'information, École de technologie supérieure (ÉTS)  
**Session :** Automne 2026  

---

## 1. Vue d'ensemble du cours et structure académique

Le cours **MTI850** a pour vocation de maîtriser les architectures, les paradigmes de programmation distribuée et les algorithmes de machine learning à grande échelle (*Distributed ML*).

### 1.1 Objectifs pédagogiques principaux
1. **Concepts fondamentaux de la science des données massives :**
   - Préparation des données, cycle de vie ETL (*Extract-Transform-Load*), opérations batch et flux continu.
   - Analytique descriptive, diagnostique, prédictive et prescriptive.
2. **L'écosystème Apache Spark :**
   - Abstractions fondamentales : RDD (*Resilient Distributed Datasets*), DataFrames et Datasets.
   - Optimisation de requêtes avec Spark SQL et le moteur Catalyst.
   - Pipelines d'apprentissage automatique avec Spark MLlib.
   - Traitement en temps réel avec Spark Streaming / Structured Streaming.
3. **Apprentissage statistique à grande échelle (*Scalable & Distributed ML*) :**
   - Comprendre comment passer d'un modèle statistique sur machine unique à un entraînement distribué sur cluster.
   - Étude de la descente de gradient distribuée, de la hiérarchie de communication réseau et des goulots d'étranglement mémoire/stockage.
   - Algorithmes clés : régression linéaire, classification, filtrage collaboratif (ALS - *Alternating Least Squares*), arbres de décision.
   - Intégration de Spark avec des bibliothèques de Deep Learning modernes (ex. PyTorch).

### 1.2 Modalités d'évaluation
* **Travaux pratiques / Projets dirigés (PDs) : 30%**
  - PD0 (2%) : Prise en main de l'environnement (Docker, Hadoop, Spark, JupyterLab).
  - PD1 (3%) : Révision mathématique et manipulation HDFS/Python.
  - PD2 (5%), PD3 (5%), PD4 (5%) : Spark DataFrames, SQL et pipelines.
  - PD5 (10%) : Machine Learning distribué avancé.
* **Examen intra (mi-session) : 30%** (Date prévue : 23 octobre)
  - Questions à choix multiples, vrai/faux et questions à développement technique/conceptuel.
* **Projet final de session : 40%** (Présentation : 4 décembre)
  - Réalisé en équipes de 3 étudiants sur un cas d'usage réel à grande échelle.

---

## 2. Définition et Réalité du "Big Data"

### 2.1 Définitions formelles
* **Définition opérationnelle :** Des jeux de données qui ne tiennent pas dans la mémoire vive centrale (*in-core memory*), et qui souvent ne tiennent même plus sur le disque dur local d'une machine conventionnelle.
* **Définition système :** Tout volume de données numériques inconfortable ou impraticable à stocker, transporter, indexer ou analyser avec les outils de gestion de bases de données traditionnels (SGBD relationnels, scripts locaux).
* **Le seuil critique :** Les données deviennent « massives » (*Big*) dès lors que leur **volume**, leur **vélocité**, leur **variété** ou leurs **exigences de calcul** dépassent les capacités pratiques des systèmes monolithiques.

### 2.2 La nuance capitale : La taille ne suffit pas
> **Est-ce qu'1 To de données représente du Big Data ?**  
> **Réponse clé :** Cela dépend entièrement de la charge de travail (*workload*) !
> - Si l'opération consiste à faire un *scan* séquentiel unique (ex. calcul d'une somme globale), un serveur moderne avec un disque NVMe rapide peut le faire en quelques minutes sans cluster.
> - En revanche, si la charge implique :
>   - Un flux continu en temps réel avec fenêtrage glissant (*sliding window*),
>   - Des jointures itératives lourdes (*many-to-many joins*) entre plusieurs tables d'1 To,
>   - L'entraînement itératif d'un modèle d'apprentissage statistique nécessitant 100 passes en mémoire,  
>   alors 1 To devient immédiatement un défi système massif exigeant un cluster distribué.

### 2.3 L'évolution des dimensions : Du modèle des 3V aux 5V
1. **Volume :** L'échelle brute des données générées (Téraoctets, Pétaoctets, Exaoctets).
2. **Vélocité (*Velocity*) :** La cadence d'arrivée et d'évacuation des flux de données (ex. millions d'événements/seconde nécessitant une latence milliseconde).
3. **Variété :** L'hétérogénéité des formats :
   - *Structuré :* Tables SQL, schémas stricts.
   - *Semi-structuré :* JSON, XML, logs de serveurs, Avro.
   - *Non structuré :* Vidéos de surveillance, flux audio, images satellites, texte brut.
4. **Véracité (*Veracity*) :** La qualité, le bruit, la fiabilité et l'intégrité des données collectées.
5. **Variabilité :** Les variations du débit, les changements imprévisibles de schéma (*schema drift*) et l'inconstance de la signification sémantique selon le contexte temporel.

---

## 3. L'Histoire et la Révolution Fondatrice de Google (2003–2006)

Au début des années 2000, l'explosion du Web a confronté les géants du web (particulièrement Google) à une limite physique insurmontable : les serveurs relationnels classiques (*scale-up*, serveurs spécialisés ultra-coûteux) ne pouvaient plus indexer le web.

Google a publié **trois articles de recherche fondateurs** qui ont posé les bases de l'ingénierie moderne des données massives :

```
                  ┌──────────────────────────────────────────────┐
                  │          GOOGLE PAPERS FONDATRICES           │
                  └──────────────────────┬───────────────────────┘
                                         │
         ┌───────────────────────────────┼───────────────────────────────┐
         ▼                               ▼                               ▼
  1. GFS (2003)                  2. MapReduce (2004)             3. Bigtable (2006)
  Fichier distribué              Calcul parallèle                Base NoSQL distribuée
  Tolérant aux pannes sur        fonctionnel (Map + Reduce)      Triée, multi-dimensionnelle
  matériel banalisé              Tolérant aux pannes             reposant sur GFS
         │                               │                               │
         └───────────────────────────────┼───────────────────────────────┘
                                         │ Inspiration open-source
                                         ▼
                     ┌───────────────────────────────────────┐
                     │            APACHE HADOOP              │
                     │  - HDFS (issu de GFS)                 │
                     │  - MapReduce (moteur de calcul)       │
                     │  - HBase (issu de Bigtable)           │
                     └───────────────────┬───────────────────┘
                                         │ Limite : I/O disque lourd
                                         ▼
                     ┌───────────────────────────────────────┐
                     │             APACHE SPARK              │
                     │  - Calcul en mémoire (In-Memory DAG)  │
                     │  - 10x à 100x plus rapide             │
                     └───────────────────────────────────────┘
```

### 3.1 Google File System (GFS, 2003)
* **Auteurs :** Sanjay Ghemawat, Howard Gobioff, Shun-Tak Leung (ACM SOSP 2003).
* **Idée révolutionnaire :** Construire un système de fichiers distribué hautement disponible non pas sur du matériel de stockage propriétaire coûteux (SAN/NAS), mais sur des milliers de serveurs banalisés d'entrée de gamme (*commodity hardware*) susceptibles de tomber en panne constamment.
* **Architecture :**
  - **Master unique :** Gère uniquement les métadonnées (noms de fichiers, tables de correspondance, réplication) gardées en RAM.
  - **Chunkservers :** Stockent les blocs physiques de données (*chunks* de 64 Mo par défaut).
  - **Réplication :** Chaque bloc est dupliqué 3 fois par défaut sur des nœuds et des racks différents.
  - **Chemin de données découplé :** Le client demande au Master où se trouvent les blocs, puis communique directement avec les Chunkservers pour lire/écrire les données, éliminant tout goulot d'étranglement sur le Master.

### 3.2 MapReduce (2004)
* **Auteurs :** Jeffrey Dean, Sanjay Ghemawat (USENIX OSDI 2004).
* **Paradigme :** Inspiré de la programmation fonctionnelle (`map` et `reduce`).
  - **Map :** Traite chaque bloc localement et émet des paires clé-valeur intermédiaires `(clé, valeur)`.
  - **Shuffle & Sort :** Regroupe par le réseau toutes les valeurs partageant la même clé vers les réducteurs correspondants.
  - **Reduce :** Agrège ou transforme les listes de valeurs associées à chaque clé unique pour produire le résultat final.
* **Avantage :** Le développeur n'écrit que deux fonctions ; l'infrastructure gère automatiquement la distribution, la tolérance aux pannes (réexécution des tâches défaillantes) et la localité des données (*data locality*).

### 3.3 Bigtable (2006)
* **Auteurs :** Fay Chang et al. (USENIX OSDI 2006).
* **Nature :** Une carte creuse, distribuée, persistante et multidimensionnelle triée :
  $$\text{Table}(row\_key, column\_family:qualifier, timestamp) \rightarrow value$$
* Démontre la faisabilité d'organiser des pétaoctets de données structurées sans modèle relationnel rigide, avec lecture et écriture ultra-rapides adossées à GFS.

### 3.4 De Hadoop à Apache Spark
* **Apache Hadoop (2006) :** Doug Cutting et Mike Cafarella créent la version open-source de GFS (HDFS) et MapReduce au sein de la fondation Apache.
* **Le goulot d'étranglement de MapReduce :** Chaque cycle MapReduce écrit obligatoirement ses données intermédiaires sur disque (HDFS) pour garantir la tolérance aux pannes. Dans les algorithmes itératifs (ex. Machine Learning, PageRank), 90% du temps est perdu en écritures/lectures I/O disques et en sérialisation réseau.
* **L'arrivée d'Apache Spark (2010–2014, UC Berkeley AMPLab) :** Matei Zaharia et son équipe introduisent les RDDs et le calcul distribué en mémoire vive (*in-memory*). Spark conserve les données en cache RAM à travers les itérations et ne recalcule que les partitions perdues grâce au graphe de lignage (*lineage graph* / DAG), multipliant les vitesses de calcul par un facteur 10 à 100.

---

## 4. Architecture Moderne d'Analytique et Stockage à Grande Échelle

### 4.1 Pourquoi le Big Data est un problème de systèmes (*Systems Problem*)
Gérer des données massives n'est pas simplement un problème mathématique ou algorithmique ; c'est un problème d'infrastructure matérielle et logicielle :
- **Bande passante mémoire vs disque vs réseau :** La mémoire RAM est des ordres de grandeur plus rapide que le SSD NVMe, lui-même plus rapide que le réseau du cluster.
- **Principe de localité des données (*Data Locality*) :** Il est infiniment moins coûteux de déplacer le code de calcul (quelques kilo-octets) vers le nœud où résident les données plutôt que de transférer des gigaoctets de données à travers le réseau vers le CPU.

### 4.2 L'évolution : Du Data Warehouse au Data Lakehouse
1. **Data Warehouse (Entrepôt classique) :**
   - Schéma à l'écriture (*Schema-on-write*), données hautement structurées, requêtes SQL analytiques (OLAP).
   - Inconvénient : Coûteux, rigide, incapable de stocker des vidéos, des logs bruts ou du texte libre.
2. **Data Lake (Lac de données - HDFS, S3, ADLS) :**
   - Schéma à la lecture (*Schema-on-read*), stockage brut économique de tous types de données (fichiers bruts).
   - Inconvénient : Risque de devenir un "dépotoir de données" (*data swamp*), absence de garanties ACID, corruptions en cas d'écritures concurrentes.
3. **Data Lakehouse (Architecture moderne unifiée) :**
   - Combine le faible coût et l'élasticité du stockage objet du Data Lake avec les transactions ACID, le versionnement (*time travel*) et la gouvernance du Data Warehouse (via des technologies comme Delta Lake, Apache Iceberg, Apache Hudi).

### 4.3 Formats de fichiers à l'échelle : Pourquoi le format est crucial
- **Formats lignes (CSV, JSON) :**
  - Pratiques pour l'écriture unitaire et la lecture humaine.
  - Catastrophiques pour l'analytique : pour calculer la moyenne d'une seule colonne sur 1 milliard de lignes, le moteur doit lire l'intégralité du fichier sur disque.
- **Formats colonnaires (Parquet, ORC) :**
  - Les données sont stockées par colonnes et compressées par bloc.
  - **Projection Pushdown :** Le moteur ne lit sur disque que les colonnes mentionnées dans le `SELECT`.
  - **Predicate Pushdown :** Les métadonnées de bloc (Min/Max de chaque colonne) permettent d'ignorer des blocs entiers de données sans même les lire (*block skipping*).
  - Taux de compression de 70% à 90% supérieur à du texte brut.

### 4.4 Batch vs Streaming : Deux horizons temporels complémentaires
| Critère | Traitement par lots (*Batch*) | Traitement en flux (*Streaming*) |
| :--- | :--- | :--- |
| **Périmètre des données** | Données historiques bornées (*Bounded*) | Données infinies en continu (*Unbounded*) |
| **Latence** | Minutes, heures, jours | Millisecondes à secondes |
| **Débit (*Throughput*)** | Très élevé (optimisation globale) | Équilibré pour une faible latence |
| **Cas d'usage** | Rapports financiers mensuels, réentraînement de modèles | Détection de fraudes bancaires, alertes d'accidents de trafic |

---

## 5. Cas d'Usage Intégrateur : Analyse Massif d'une Ville Intelligente (*Smart City IoT*)

Le cours a présenté une architecture de référence complète montrant comment toutes les briques logicielles s'assemblent :

1. **Ingestion multi-sources :**
   - Capteurs IoT (feux de circulation, détecteurs de vitesse, caméras CCTV, bornes de stationnement).
   - Données mobiles et sociales (Waze, Google Maps, signalements citoyens).
   - Données publiques et administratives (APIs météo, horaires d'autobus, cadastre routier).
2. **Stockage unifié (HDFS / Cloud Object Storage) :**
   - Collecte brute de flux JSON, logs et vidéos.
3. **Moteurs de calcul Apache Spark spécialisés :**
   - **Spark Streaming :** Détection d'accidents et d'embouteillages en temps réel à la seconde près.
   - **Spark SQL :** Requêtes ad-hoc et analyse rétrospective des tendances de circulation sur les 5 dernières années.
   - **Spark MLlib :** Modèles prédictifs d'estimation du trafic routier et prévision de la demande pour les transports collectifs.
   - **Spark GraphX :** Modélisation du réseau routier sous forme de graphe et algorithmes de routage dynamique optimal.
4. **Restitution & Consommateurs :**
   - Centres de contrôle du trafic urbain (dashboards temps réel Grafana/Tableau).
   - Applications mobiles grand public (guidage GPS).
   - Urbanistes et décideurs publics (planification d'infrastructures durables et réduction des émissions de GES).

---

## 6. Synthèse pour la Révision et l'Examen

* [x] **Comprendre la frontière du Big Data :** Ce n'est pas une taille fixe (ex. 1 Go vs 1 To), mais l'adéquation entre la taille, la complexité des opérations (I/O, jointures, itérations ML) et les ressources matérielles disponibles.
* [x] **Connaître la trilogie Google :** GFS (2003, stockage blocs distribué), MapReduce (2004, calcul parallèle tolérant aux pannes), Bigtable (2006, table creuse multi-dimensionnelle).
* [x] **Savoir expliquer le saut de Hadoop à Spark :** Hadoop MapReduce écrit sur disque entre chaque étape ; Spark exécute un graphe acyclique dirigé (DAG) en mémoire vive avec tolérance aux pannes par reconstruction de lignage.
* [x] **Distinguer Batch et Streaming :** Données bornées vs non bornées ; latence vs débit ; cas d'usage analytique vs temps réel.
* [x] **Savoir pourquoi les formats colonnaires (Parquet) surclassent les formats textes (CSV/JSON) :** Compression, *projection pushdown* (lecture sélective de colonnes), *predicate pushdown* (saut de blocs de données via métadonnées Min/Max).
