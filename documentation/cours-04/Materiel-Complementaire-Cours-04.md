# MTI850 — Analytique des données massives
## Matériel Complémentaire Avancé : Le Fléau des Petits Fichiers, Internes de Parquet/Avro et Optimisation Partitioning/Bucketing

**Complément au Cours 04 :** Approfondissement technique de niveau Maîtrise  
**Auteur / Référence d'étude :** Analyse avancée pour l'ingénierie des données massives (ÉTS Montréal)

---

## 1. Le Fléau des Petits Fichiers (*The Small Files Problem*) et Stratégies de Compactage

L'un des défis les plus pernicieux dans les architectures de données massives (que ce soit sur HDFS ou sur stockage objet S3/GCS) est la prolifération incontrôlée de millions de micro-fichiers de quelques kilo-octets.

### 1.1 Origine et Conséquences Systémiques

```
           SOURCES DE PETITS FICHIERS                       CONSÉQUENCES SYSTÉMIQUES
 ┌───────────────────────────────────────────┐      ┌───────────────────────────────────────────┐
 │ • Ingestion Streaming continue            │      │ • HDFS : Sature la mémoire RAM du         │
 │   (Micro-batchs Kafka / IoT toutes les 5s)│      │   NameNode (150 octets par fichier/bloc). │
 │ • Partitionnement excessif                │────► │ • Cloud (S3/GCS) : Explosion du temps     │
 │   (ex. partition par jour + heure + ville)│      │   de latence HTTP (GET/LIST) et surcoût.  │
 │ • Tâches Spark trop nombreuses            │      │ • Spark : Surcharge de planification      │
 │   (chacune écrit son propre fichier).     │      │   (des millions de tâches de 2 ms).       │
 └───────────────────────────────────────────┘      └───────────────────────────────────────────┘
```

#### Sur HDFS :
Chaque fichier, répertoire et bloc de données requiert une entrée de métadonnées en mémoire vive (RAM) sur le **NameNode** (environ 150 octets). Un million de fichiers de 10 Ko ne stockent que 10 Go de données réelles mais consomment autant de RAM qu'un pétaoctet de gros fichiers ! De plus, la lecture séquentielle rapide est transformée en une suite de sauts de têtes de disques lents (*Disk Seeks*).

#### Sur Stockage Objet Cloud (AWS S3, Google Cloud Storage) :
Sur S3, il n'y a pas de NameNode, mais chaque lecture de fichier exige une requête API HTTP/HTTPS (`GET` ou `HEAD`).
- Chaque appel réseau subit une latence incompressible de 10 à 50 millisecondes (*Time To First Byte*).
- Si une requête Spark doit scanner 100 000 micro-fichiers, elle passe des dizaines de minutes uniquement à négocier des connexions TLS et des en-têtes HTTP, alors que le volume de données utile n'est que de quelques mégaoctets !
- De plus, les requêtes `LIST` et `GET` sont facturées par million d'appels par les fournisseurs cloud.

---

### 1.2 Stratégies d'Ingénierie pour Résoudre le Problème

#### 1. Arbitrage Spark : `coalesce(n)` vs `repartition(n)`
Avant d'écrire un DataFrame sur disque, il est capital de contrôler le nombre de fichiers générés :

| Opération | Mécanisme Interne | Utilise un Shuffle Réseau ? | Cas d'Usage Idéal |
| :--- | :--- | :--- | :--- |
| **`df.coalesce(n)`** | Fusionne les partitions existantes sur les mêmes nœuds ouvriers. | **Non (Zéro Shuffle).** Très rapide et économe. | **Réduire** le nombre de fichiers de sortie avant l'écriture finale (ex. passer de 200 à 4 fichiers). |
| **`df.repartition(n)`** | Redistribue uniformément toutes les données à travers le cluster par hachage. | **Oui (Full Shuffle).** Coûteux en réseau et disque. | Augmenter le parallélisme si le DataFrame n'a pas assez de partitions, ou rééquilibrer des partitions biaisées. |

#### 2. Réglages du Moteur Spark pour l'Ingestion
Spark SQL intègre deux paramètres fondamentaux pour regrouper automatiquement les petits fichiers lors de la lecture :
- `spark.sql.files.maxPartitionBytes` (par défaut 128 Mo) : Définit la taille maximale d'une partition logique Spark.
- `spark.sql.files.openCostInBytes` (par défaut 4 Mo) : Spark ajoute un coût fictif de 4 Mo à chaque fichier scanné pour simuler la latence d'ouverture. Si un répertoire contient 20 fichiers de 500 Ko, Spark les regroupe dans une seule et même tâche au lieu de créer 20 tâches minuscules.

#### 3. Compactage Automatique dans les Architectures Lakehouse (*Bin-Packing*)
Dans les tables modernes (**Apache Iceberg**, **Delta Lake**), le problème est résolu de manière asynchrone par des commandes d'optimisation :
```sql
-- Dans Delta Lake : regroupe les petits fichiers en fichiers optimaux de 1 Go
OPTIMIZE delta.`/data/fire_calls` ZORDER BY (IncidentDate);
```

---

## 2. Anatomie Interne des Formats de Données Big Data : Parquet vs Avro

Le choix du format de fichier détermine la vitesse de lecture, le taux de compression et le coût de stockage d'un entrepôt de données.

```
       FORMAT ORIENTÉ LIGNE (AVRO)                   FORMAT ORIENTÉ COLONNE (PARQUET / ORC)
 ┌─────────────────────────────────────┐      ┌──────────────┬──────────────┬──────────────┐
 │ Ligne 1 : [101, "Alice", 4500.00]   │      │ Colonne ID   │ Colonne Nom  │ Colonne Solde│
 │ Ligne 2 : [102, "Bob",   3200.50]   │      │ [101, 102,   │ ["Alice",    │ [4500.00,    │
 │ Ligne 3 : [103, "Carla", 5100.00]   │      │  103]        │  "Bob",      │  3200.50,    │
 └─────────────────────────────────────┘      │              │  "Carla"]    │  5100.00]    │
   • Idéal pour l'écriture continue           └──────────────┴──────────────┴──────────────┘
     (Append streaming, Kafka, OLTP).           • Idéal pour l'analytique et Spark SQL.
   • Mauvais pour requêtes analytiques          • Seules les colonnes demandées sont lues.
     (doit lire toutes les colonnes).           • Compression maximale par type de données.
```

### 2.1 La Structure Physique d'un Fichier Apache Parquet

Un fichier Parquet n'est pas un simple flux d'octets ; c'est une structure hiérarchique complexe optimisée pour les lectures sélectives :

```
┌────────────────────────────────────────────────────────────────────────┐
│                        ANATOMIE D'UN FICHIER PARQUET                   │
├────────────────────────────────────────────────────────────────────────┤
│ Magic Number : "PAR1" (4 octets)                                       │
├────────────────────────────────────────────────────────────────────────┤
│ ROW GROUP 1 (Taille typique : 128 Mo à 1 Go de données)               │
│   ├── Column Chunk 1 (ex. Colonne 'CallType')                          │
│   │     ├── Data Page 1 [Encodage par dictionnaire + RLE] (~1 Mo)     │
│   │     └── Data Page 2 [Statistiques locales : min/max/null_count]    │
│   ├── Column Chunk 2 (ex. Colonne 'Delay')                             │
│   └── Column Chunk 3 (ex. Colonne 'IncidentDate')                      │
├────────────────────────────────────────────────────────────────────────┤
│ ROW GROUP 2 ...                                                        │
├────────────────────────────────────────────────────────────────────────┤
│ FILE METADATA FOOTER (Pied de page)                                    │
│   ├── Schéma complet des colonnes et types stricts                     │
│   ├── Emplacements et décalages d'octets (offsets) de chaque chunk     │
│   └── Statistiques globales par Row Group : [min, max, count, nulls]   │
├────────────────────────────────────────────────────────────────────────┤
│ 4 octets : Longueur du Footer (taille en octets)                       │
│ Magic Number final : "PAR1" (4 octets)                                 │
└────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Comment Parquet Accélère les Requêtes de 100x ?

1. **Lecture par le Pied de Page (*Footer-First Reading*) :**  
   Lorsqu'un exécuteur Spark lit un fichier Parquet, il commence par lire **les derniers octets du fichier** (le *Footer*). Il charge l'arbre des métadonnées en une seule opération et sait exactement où se trouve chaque colonne sur le disque.
2. **Élision de Colonnes (*Column Projection*) :**  
   Si votre requête est `SELECT CallType FROM table`, Spark saute physiquement par-dessus les octets des autres colonnes sans jamais les transférer à travers le réseau ou le bus mémoire.
3. **Poussée de Prédicats & Élimination de Groupes de Lignes (*Row Group Skipping*) :**  
   Supposons la requête `WHERE Delay > 120`. Le moteur consulte le footer du *Row Group 1* et lit : `Delay_min = 0.5, Delay_max = 45.0`.  
   Puisque la valeur maximale (45.0) est inférieure à 120, Spark **saute l'intégralité du Row Group (128 Mo d'octets)** sans même le décompresser !

---

## 3. Partitionnement et Bucketing de Tables Spark SQL en Production

Lorsqu'une table dépasse plusieurs dizaines de gigaoctets, la manière dont les fichiers sont découpés sur le stockage conditionne l'efficacité des filtres et des jointures.

```
       TABLE PARTITIONNÉE (Partitioning)                 TABLE EN SEAUX (Bucketing)
 ┌───────────────────────────────────────────┐      ┌───────────────────────────────────────────┐
 │ Répertoires physiques hiérarchiques :     │      │ Fichiers numérotés avec hachage fixe :    │
 │ /data/vols/annee=2026/mois=09/            │      │ bucket_0000.parquet (hash(user_id) % 4 == 0)
 │ /data/vols/annee=2026/mois=10/            │      │ bucket_0001.parquet (hash(user_id) % 4 == 1)
 └───────────────────────────────────────────┘      └───────────────────────────────────────────┘
   • Idéal pour colonnes à FAIBLE cardinalité.        • Idéal pour colonnes à HAUTE cardinalité.
   • Élimine les répertoires inutiles.                • ÉLIMINE LE SHUFFLE LORS DES JOINTURES !
```

### 3.1 Le Partitionnement (*Partitioning*)
On découpe la table en sous-répertoires physiques en fonction de la valeur d'une ou plusieurs colonnes :
```python
df.write.partitionBy("annee", "mois").parquet("/data/vols/")
```
- **Avantage (*Partition Pruning*) :** Une requête `WHERE annee = 2026 AND mois = 09` ne lit que les fichiers du dossier correspondant et ignore tout le reste du stockage.
- **Le Danger mortel : L'Over-Partitioning (Sur-partitionnement) :**  
  Ne partitionnez **jamais** sur une colonne à haute cardinalité (comme `user_id` ou `immatriculation`). Si vous avez 5 millions d'utilisateurs, vous créerez 5 millions de sous-dossiers contenant chacun un minuscule fichier de 50 octets, ce qui détruira les performances du système de stockage !

### 3.2 Le Bucketing (*Clustering par Seaux*)
Le **Bucketing** est la technique reine pour les colonnes à haute cardinalité fréquemment utilisées dans des jointures (`JOIN`) ou des regroupements (`GROUP BY`) :
```python
df.write \
    .bucketBy(32, "user_id") \
    .sortBy("user_id") \
    .saveAsTable("clients_bucketed")
```
- **Mécanisme :** Spark applique une fonction de hachage déterministe sur la clé `user_id` modulo le nombre de buckets : $\text{hash}(user\_id) \pmod{32}$.
- Les enregistrements ayant la même clé se retrouvent systématiquement stockés dans le même fichier physique, pré-triés.
- **Le Miracle de la Jointure Sans Shuffle (*Shuffle-Free Sort-Merge Join*) :**  
  Si vous joignez deux tables massives (`commandes` et `clients`) qui sont toutes deux partitionnées en 32 buckets sur la clé `user_id`, Spark n'a besoin d'échanger **aucun octet sur le réseau** ! Chaque exécuteur lit simplement le bucket $k$ de la première table et le bucket $k$ de la deuxième table en parallèle localement. Le temps de jointure passe de plusieurs heures à quelques minutes !

---

## 4. Évolution des Catalogues de Métadonnées : Du Hive Metastore au Lakehouse

Dans Spark SQL classique, la connaissance des tables et vues repose historiquement sur le **Hive Metastore (HMS)** :
- Le HMS est un service centralisé adossé à une base de données relationnelle (MySQL, MariaDB ou PostgreSQL).
- Il conserve le schéma, les types, l'emplacement physique (`location`) et la liste des partitions.

### 4.1 Les Limites du Hive Metastore dans le Cloud Moderne
1. **Goulot d'étranglement centralisé :** Quand des milliers d'exécuteurs Spark démarrent simultanément pour analyser des centaines de milliers de partitions, ils bombardent la base de données relationnelle du HMS de requêtes SQL concurrentes, provoquant des pannes de connexion.
2. **Absence d'atomicité sur le stockage objet :** Le HMS ne gère pas les transactions ACID. Si une écriture échoue à mi-chemin, des fichiers orphelins corrompent la table.

### 4.2 Les Nouveaux Catalogues Ouverts de Lakehouse
Pour répondre à ce défi, l'industrie a conçu des spécifications de catalogues modernes :
- **Apache Iceberg REST Catalog :** Les métadonnées ne sont plus dans une base MySQL, mais écrites dans des fichiers de manifestes arborescents immuables (Avro/JSON) stockés directement avec les données dans le bucket S3.
- **Unity Catalog & AWS Glue Data Catalog :** Services serverless gérés garantissant le contrôle d'accès unifié aux colonnes, la traçabilité complète (*Data Lineage*) et l'audit de sécurité à l'échelle de l'entreprise.

---

## 5. Points Essentiels à Retenir pour l'Examen

1. **Le Problème des Petits Fichiers :** Pourquoi l'ingestion temps réel crée des micro-fichiers, comment cela épuise la RAM du NameNode HDFS ou multiplie les requêtes HTTP payantes sur AWS S3, et comment `coalesce()` résout ce problème sans déclencher de Shuffle.
2. **L'Anatomie de Parquet :** Rôle fondamental du *Footer* de métadonnées, distinction entre *Row Group* et *Column Chunk*, et fonctionnement du *Row Group Skipping* via les statistiques min/max.
3. **Partitioning vs Bucketing :** Le partitionnement crée des répertoires physiques pour les colonnes à faible cardinalité ; le bucketing distribue une clé à haute cardinalité dans un nombre fixe de seaux par hachage pour éliminer le Shuffle lors des jointures.
4. **Tables Gérées vs Externes :** Le comportement destructeur d'un `DROP TABLE` sur une table gérée face à la résilience d'une table non gérée (*External*).
