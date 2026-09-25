# MTI850 — Analytique des données massives
## Matériel Complémentaire Avancé : Optimiseur Catalyst, Projet Tungsten, Exécution Adaptative (AQE) et Vectorisation Arrow

**Complément au Cours 03 :** Approfondissement technique de niveau Maîtrise  
**Auteur / Référence d'étude :** Analyse avancée pour l'ingénierie des données massives (ÉTS Montréal)

---

## 1. L'Optimiseur Déclaratif Catalyst : Les Quatre Phases de Compilation

Dans Apache Spark, que vous écriviez une requête en **Spark SQL** ou via l'**API DataFrame**, votre code ne s'exécute jamais tel quel. Il est transmis au moteur d'optimisation **Catalyst**, qui compile votre logique déclarative en bytecode machine ultra-optimisé à travers quatre phases rigoureuses :

```
      [ Code Utilisateur (SQL ou DataFrame) ]
                         │
                         ▼
             [ 1. Plan Logique Non Résolu ]
                         │  (Validation avec le Catalogue : tables, types, colonnes)
                         ▼
             [ 2. Plan Logique Résolu ]
                         │  (Optimisations basées sur des règles - RBO)
                         ▼
             [ 3. Plan Logique Optimisé ]
                         │  (Modèle de Coût - CBO : choix des algorithmes)
                         ▼
             [ 4. Plans Physiques Multiples ] ──► [ Plan Physique Élu ]
                                                         │
                                                         ▼ (Génération de code Janino)
                                                 [ Bytecode Java / Tungsten ]
```

### 1.1 Phase 1 : Résolution de l'Arbre de Syntaxe (Analysis)
* Le code est transformé en un arbre abstrait non résolu (*Unresolved Logical Plan*). À ce stade, Spark ignore si les colonnes mentionnées existent réellement ou quel est leur type.
* Le composant d'analyse consulte le **Catalogue de métadonnées** (schémas Hive, métadonnées HDFS/S3, tables temporaires) pour associer les noms aux attributs réels et valider les types de données, produisant le *Resolved Logical Plan*.

### 1.2 Phase 2 : Optimisations Basées sur des Règles (Rule-Based Optimization - RBO)
Catalyst applique une suite d'arbres de réécriture formels pour simplifier la logique mathématique :
1. **Élision de projection (*Projection Pruning*) :** Si votre table contient 100 colonnes et que votre requête n'utilise que `nom` et `salaire`, toutes les 98 autres colonnes sont immédiatement retirées du plan d'exécution.
2. **Poussée de prédicats (*Predicate Pushdown*) :** Si vous écrivez `df.join(autre_df).filter(df.age > 30)`, l'optimiseur inverse l'ordre ! Il applique le filtre `age > 30` **avant** la jointure, réduisant drastiquement le volume de données à échanger sur le réseau. Mieux encore : si la source est un fichier Parquet ou une base relationnelle, le filtre est délégué directement au pilote de stockage (*Pushdown down to storage layer*).
3. **Plis de constantes (*Constant Folding*) :** Les expressions invariables comme `1000 * 60 * 60` sont pré-calculées à la compilation sous la valeur unique `3600000`, épargnant des milliards de calculs redondants au processeur lors de l'exécution sur les lignes.
4. **Simplification booléenne :** Simplification algébrique des expressions logiques (ex. `NOT(A OR B)` $\rightarrow$ `NOT(A) AND NOT(B)`).

### 1.3 Phase 3 : Planification Physique et Optimiseur Basé sur les Coûts (CBO)
Catalyst génère plusieurs plans physiques concurrents et utilise l'optimiseur basé sur les coûts (*Cost-Based Optimizer*) pour choisir l'algorithme d'exécution le plus économe en calcul et en réseau :

| Stratégie de Jointure | Mécanisme Interne | Complexité & Trafic Réseau | Cas d'Utilisation Idéal |
| :--- | :--- | :--- | :--- |
| **Broadcast Hash Join (BHJ)** | La plus petite table (< 10 Mo par défaut) est diffusée intégralement à tous les exécuteurs en mémoire. | **Zéro Shuffle réseau pour la grande table.** Rapidité extrême. | Une grande table jointe à une table de dimension modeste. |
| **Shuffle Hash Join** | Hache les deux tables sur la clé de jointure pour envoyer les mêmes clés sur les mêmes nœuds, puis construit une table de hachage en mémoire. | Shuffle réseau sur les deux tables. Consomme de la RAM. | Tables de tailles moyennes lorsque les données ne sont pas pré-triées. |
| **Sort-Merge Join (SMJ)** | Phase de Shuffle partitionné, tri local des données par clé sur chaque exécuteur, puis fusion séquentielle comme des fermetures éclair. | Shuffle réseau lourd + coût de tri CPU/Disque. | Deux très grandes tables ne tenant pas dans la mémoire vive (*Out-of-Core joins*). |

---

## 2. Le Projet Tungsten : La Physique du Matériel au Service de Spark

Au lancement de Spark, tout le monde pensait que la mémoire vive suffisait pour garantir des performances optimales. Mais vers 2015, les créateurs de Spark ont découvert que **le processeur et la gestion de mémoire JVM étaient devenus le principal goulot d'étranglement**, bien avant les E/S réseau et disque.

### 2.1 La Crise des Objets Java dans la JVM
Pour stocker une simple chaîne de caractères de 4 lettres comme `"ÉTS"` en mémoire Java classique :
* Une référence d'objet 64 bits = 8 octets.
* L'en-tête de l'objet JVM = 12 à 16 octets.
* Les métadonnées du tableau de caractères internes = 16 octets.
* *Total :* **Près de 48 octets en mémoire pour stocker 3 octets réels !**
Ce gaspillage sature la RAM et force le ramasse-miettes (*Garbage Collector*) à scanner des millions de pointeurs, provoquant des arrêts complets du cluster (*Stop-The-World GC Pauses*).

### 2.2 La Solution Tungsten : Gestion Mémoire Binaire Hors-Heap (*Off-Heap Unsafe*)
Le projet Tungsten contourne complètement le ramasse-miettes de Java :
1. **Format Binaire Compact :** Les lignes de DataFrames sont encodées sous forme de trames binaires brutes continues (identiques à des structures en langage C), stockées hors de la mémoire gérée par Java via `sun.misc.Unsafe`.
2. **Sympathie Mécanique (*Mechanical Sympathy*) et Cache L1/L2/L3 :** En disposant les données de façon contiguë dans la mémoire, le processeur peut charger des lignes entières directement dans ses caches matériels ultra-rapides sans subir de ratés de mémoire (*Cache Misses*).
3. **Génération de Code Complète (*Whole-Stage Code Generation*) :** Au lieu d'avoir des fonctions imbriquées appelées ligne par ligne (qui détruisent les registres CPU), Tungsten utilise le compilateur **Janino** pour synthétiser dynamiquement une seule et unique boucle `for` ultra-rapide en langage machine Java Bytecode.

---

## 3. Exécution Adaptative des Requêtes (AQE - Spark 3.x et 4.x)

Historiquement, le plan physique généré par Catalyst était figé avant le début du calcul. Si les statistiques initiales étaient imprécises, le cluster pouvait exécuter un plan désastreux.

L'**Adaptive Query Execution (AQE)** révolutionne ce modèle en introduisant une boucle de rétroaction : Spark observe les statistiques réelles des données **à la fin de chaque étape de Shuffle (*Stage Boundary*)** et ré-optimise le plan pour la suite de l'exécution.

```
[ Stage 1 : Map / Lecture ] ──► [ Écriture du Shuffle ] 
                                           │
                                           ▼ (AQE observe la taille réelle des blocs)
                               ┌───────────────────────┐
                               │ RÉ-OPTIMISATION EN VOL│
                               └───────────┬───────────┘
                                           │
               ┌───────────────────────────┼───────────────────────────┐
               ▼                           ▼                           ▼
    1. Fusion des Partitions      2. Basculement de Jointure    3. Traitement des Biais
    (Coalescing Shuffle)          (Sort-Merge ➔ Broadcast)     (Skew Join Handling)
    Combine les partitions        Si la table filtrée fait     Découpe les partitions
    trop petites (< 64 Mo).       < 10 Mo, supprime le shuffle géantes pour éviter les
                                  de la seconde étape !        bloqueurs traînards.
```

### 3.1 Les Trois Piliers d'AQE :
1. **Fusion Dynamique des Partitions (*Coalescing Shuffle Partitions*) :** Par défaut, `spark.sql.shuffle.partitions` est réglé sur 200. Si vos données filtrées ne font que 2 Mo, Spark 2 lançait 200 tâches ridicules traitant chacune 10 Ko. AQE détecte la taille réelle et fusionne automatiquement ces partitions pour n'exécuter que 2 ou 3 tâches utiles.
2. **Conversion Dynamique en Jointure Broadcast :** Si une table de 1 To est filtrée et qu'il ne reste que 5 Mo de données réelles, AQE annule le coûteux *Sort-Merge Join* prévu et le remplace à chaud par un *Broadcast Hash Join* instantané sans aucun Shuffle !
3. **Gestion des Biais de Données (*Skew Join Optimization*) :** Si 90% des enregistrements partagent la même clé (ex. `user_id = null` ou pays le plus peuplé), un seul exécuteur héritera de 90% du travail et bloquera tout le cluster pendant que les autres dorment. AQE repère automatiquement cette partition géante et la découpe en plusieurs sous-tâches traitées en parallèle.

---

## 4. PySpark Vectorisé : De Py4J à Apache Arrow et Pandas UDFs

L'utilisation de fonctions définies par l'utilisateur (UDF) en Python a longtemps été le cauchemar des architectes Big Data.

### 4.1 L'Enfer des UDFs Python Traditionnelles (`@udf`)
Quand vous exécutiez une UDF Python personnalisée dans Spark 1 ou 2 :
```
[ Donnée JVM ] ──Sérialisation Pickle (Ligne par ligne)──► [ Socket Py4J ] ──► [ Processus Python ] 
                                                                                        │ (Calcul lent)
[ Donnée JVM ] ◄──Sérialisation Pickle (Ligne par ligne)── [ Socket Py4J ] ◄────────────┘
```
* Chaque ligne devait être sérialisée en binaire Python (*Pickling*), envoyée via un socket local à un sous-processus Python, traitée ligne par ligne, puis ré-expédiée vers la JVM.
* Cette gymnastique consommait jusqu'à 80% du temps total d'exécution.

### 4.2 Le Salut par les Pandas UDFs et Apache Arrow (`@pandas_udf`)
Grâce à **Apache Arrow**, PySpark utilise désormais une représentation colonnaire partagée en mémoire vive :

```python
from pyspark.sql.functions import pandas_udf
import pandas as pd

# Pandas UDF vectorisée : reçoit une Série Pandas, renvoie une Série Pandas
@pandas_udf("double")
def conversion_devise_vectorisee(prix: pd.Series) -> pd.Series:
    return prix * 1.35  # Exécution vectorisée C / SIMD via NumPy
```

* Spark assemble les colonnes sous forme de blocs de mémoire Arrow.
* Le bloc de mémoire physique est partagé directement avec le moteur C de Pandas et NumPy **sans aucune copie de données (*Zero-Copy*)**.
* Le calcul bénéficie des instructions vectorielles SIMD (*Single Instruction, Multiple Data*) des microprocesseurs modernes, offrant des gains de vitesse de **10x à 100x** par rapport aux UDFs Python standards.

---

## 5. Maîtrise du Cache : Niveaux de Stockage et Pièges Mémoire

L'utilisation de `.cache()` n'est qu'un raccourci pour la méthode plus générale `.persist(StorageLevel)`.

### 5.1 Comparaison des Niveaux de Stockage (`StorageLevel`)

| Niveau de Stockage | Espace Utilisé | Utilisation CPU | Présence Disque | Description & Recommandation |
| :--- | :--- | :--- | :--- | :--- |
| **`MEMORY_ONLY`** | Très élevé (objets Java désérialisés) | Nulle (accès direct) | Non | Le plus rapide, mais sature vite la RAM. Si la mémoire manque, certaines partitions ne sont pas stockées et seront recalculées. |
| **`MEMORY_ONLY_SER`** | Faible (flux d'octets compacts) | Faible à modérée | Non | Stocke les objets sérialisés en tableaux d'octets. Économise 60% à 70% de RAM au prix d'un léger temps de dé-sérialisation. |
| **`MEMORY_AND_DISK`** | Élevé | Faible | **Oui** | Comportement par défaut de `.cache()` sur les DataFrames. Si une partition déborde de la RAM, elle est déversée sur disque plutôt que d'être perdue. |
| **`MEMORY_AND_DISK_SER`**| Faible | Modérée | **Oui** | Idéal pour les gros jeux de données complexes réutilisés intensivement. |
| **`DISK_ONLY`** | Modéré | Élevée (E/S disques) | **Oui** | Rarement utilisé, sauf si recalculer la chaîne de transformations est plus coûteux que de relire le disque local. |

### 5.2 La Règle d'Hygiène Mémoire : `.unpersist()`
Le cache de Spark utilise une politique d'éviction au plus ancien (*LRU - Least Recently Used*). Cependant, laisser des dizaines de DataFrames obsolètes en cache empêche le moteur d'allouer de la mémoire aux étapes de Shuffle.

> **Bonne pratique professionnelle :**  
> Dès qu'un bloc de transformations itératives est achevé, libérez explicitement les ressources :
> ```python
> df_intermediaire.unpersist()
> ```

---

## 6. Synthèse des Concepts Clés pour l'Examen

* [x] **Comprendre le cycle Catalyst :** Arbre non résolu $\rightarrow$ Résolution par catalogue $\rightarrow$ Optimisation logique (RBO) $\rightarrow$ Plan physique et coût (CBO) $\rightarrow$ Génération de code Janino.
* [x] **Expliquer les atouts de Tungsten :** Mémoire binaire continue *off-heap* échappant au Garbage Collector Java, exploitation maximale des caches processeurs L1/L2, et *Whole-Stage Code Generation*.
* [x] **Identifier les 3 actions d'AQE :** Fusion automatique des petites partitions de shuffle, conversion dynamique en Broadcast Join, et partitionnement des clés biaisées (*Skew Joins*).
* [x] **Justifier la supériorité d'Apache Arrow sur Py4J :** Échange colonnaire zéro-copie en mémoire partagée contre la sérialisation ligne par ligne coûteuse de Python pickle.
* [x] **Choisir le bon niveau de persistance :** Arbitrage entre empreinte mémoire (sérialisé vs désérialisé) et temps de calcul CPU/disque.
