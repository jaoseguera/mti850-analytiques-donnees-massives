# MTI850 — Analytique des données massives
## Matériel Complémentaire Avancé : Théorie des Systèmes Distribués, Architectures Internes et État de l'Art

**Complément au Cours 01 :** Approfondissement de niveau Maîtrise  
**Auteur / Référence d'étude :** Analyse avancée pour l'ingénierie des données massives (ÉTS Montréal)

---

## 1. Fondements Théoriques des Systèmes Distribués

Pour dépasser la vision superficielle du Big Data, il est indispensable de comprendre les lois mathématiques et les théorèmes de conception qui contraignent tous les systèmes distribués.

### 1.1 Le Théorème CAP (Eric Brewer, 2000) et le Modèle PACELC
Tout système distribué de stockage de données se heurte au théorème CAP, qui stipule qu'il est impossible de garantir simultanément ces trois propriétés en présence d'aléas réseau :
* **C (Consistency - Cohérence forte) :** Toute opération de lecture renvoie la donnée la plus récemment écrite ou une erreur.
* **A (Availability - Disponibilité) :** Toute requête non fautive reçoit une réponse non erronée, sans garantie qu'elle contienne la version la plus récente.
* **P (Partition Tolerance - Tolérance au partitionnement) :** Le système continue de fonctionner malgré la perte arbitraire de messages ou la rupture de communication physique entre nœuds.

Dans le monde réel, les pannes réseau et les pertes de paquets sont inévitables ($P$ est obligatoire). Le compromis se résume donc à choisir entre **CP** et **AP** :
* **Systèmes CP (ex. HDFS NameNode, Apache HBase, ZooKeeper, etcd) :** En cas de coupure réseau, le système refuse les écritures ou les lectures sur les nœuds isolés pour empêcher toute divergence de données (*split-brain*).
* **Systèmes AP (ex. Apache Cassandra, Amazon Dynamo, CouchDB) :** Le système privilégie la haute disponibilité. Même coupé du reste du cluster, un nœud acceptera l'écriture et synchronisera les conflits ultérieurement (*cohérence éventuelle*).

```
                            [ THÉORÈME CAP ]
                                   ▲
                                  / \
                                 /   \
                                /  P  \
                               /       \
                              /  Réseau \
                             /    coupé  \
                            /             \
        [ SYSTÈMES CP ]    /───────────────\    [ SYSTÈMES AP ]
      Cohérence Prioritaire                   Disponibilité Prioritaire
      - HDFS (NameNode)                       - Cassandra
      - HBase / Bigtable                      - DynamoDB
      - ZooKeeper / Raft                      - CouchDB
      (Refuse l'écriture si doute)            (Accepte l'écriture, réconcilie plus tard)
```

#### L'extension PACELC (Daniel Abadi, 2012)
Le théorème CAP n'explique pas le comportement du système **en temps normal** (quand le réseau fonctionne parfaitement). Le modèle PACELC complète CAP :
$$\text{If } \mathbf{P} \text{ (Partition) } \rightarrow \text{Choose between } \mathbf{A} \text{ or } \mathbf{C} \quad ; \quad \mathbf{E} \text{lse} \rightarrow \text{Choose between } \mathbf{L} \text{ (Latency) or } \mathbf{C} \text{ (Consistency)}$$
- Même sans partition réseau, garantir une cohérence immédiate ($C$) exige des allers-retours de synchronisation entre répliques, ce qui dégrade drastiquement la latence ($L$).
- Bigtable et HDFS choisissent **PC/EC** (cohérence en cas de panne, cohérence en temps normal). Cassandra choisit généralement **PA/EL** (faible latence et disponibilité maximale).

### 1.2 Lois de Scalabilité : Amdahl vs Gustafson

Pourquoi le calcul distribué permet-il de traiter des pétaoctets alors que la loi d'Amdahl semblait pessimiste ?

1. **Loi d'Amdahl (Mise à l'échelle forte - *Strong Scaling*) :**
   $$S(N) = \frac{1}{(1 - P) + \frac{P}{N}}$$
   Où $P$ est la fraction parallélisable et $N$ le nombre de processeurs. Si seulement 5% d'un programme est strictement séquentiel ($1-P = 0.05$), le gain d'accélération maximal théorique est borné par $1 / 0.05 = 20\times$, même avec 100 000 serveurs !
2. **Loi de Gustafson-Barsis (Mise à l'échelle faible - *Weak Scaling*) :**
   $$S(N) = N - (1 - P)(N - 1)$$
   John Gustafson a démontré qu'en Big Data, **la taille du problème grandit avec le nombre de machines**. On ne cherche pas à traiter 10 Mo avec 1 000 serveurs ; on utilise 1 000 serveurs pour traiter 10 To. La portion séquentielle devient négligeable face au volume massif de données distribuées, permettant une accélération quasi linéaire.

---

## 2. Anatomie Détaillée des Papiers Fondateurs

### 2.1 Les Secrets d'Ingénierie de Google File System (GFS, 2003)

Pourquoi GFS a-t-il fait des choix si radicaux par rapport aux systèmes de fichiers Unix POSIX ?

| Choix de conception GFS | Justification Système | Conséquence Pratique |
| :--- | :--- | :--- |
| **Taille de bloc à 64 Mo** (vs 4 Ko sur ext4) | Réduire le nombre total de blocs à indexer. | Les métadonnées de millions de Go tiennent entièrement dans les 64 Go de RAM du Master. Réduction drastique de l'overhead TCP. |
| **Append-Only (Ajout en fin de fichier)** | Évite les verrous d'écriture concurrents complexes à l'intérieur d'un bloc. | Débit séquentiel maximal pour les fichiers de logs et de crawls web. Les modifications aléatoires (*random writes*) sont interdites. |
| **Baux de mutation (*Chunk Leases*)** | Le Master accorde un bail de 60 secondes à un Chunkserver désigné comme "Primaire". | Le Master est déchargé de l'ordonnancement des écritures : le Primaire dicte l'ordre des mutations aux répliques secondaires. |
| **Consistance lâche (*Relaxed Consistency*)** | GFS garantit que les données sont définies (*defined*), mais accepte de légers doublons lors des ajouts atomiques concurrents (*record append*). | Les applications clientes (comme MapReduce) doivent être tolérantes aux doublons ou inclure des identifiants uniques. |

### 2.2 MapReduce : Les Problèmes Cachés résolus par Dean & Ghemawat (2004)

1. **La gestion des "Traînards" (*Stragglers*) et l'exécution spéculative :**
   - Dans un cluster de 1 000 nœuds, il y a toujours 2 ou 3 machines ralenties par un disque défectueux, un ventilateur encrassé ou un problème thermique CPU.
   - Si une étape attend la dernière tâche de Map, tout le cluster s'arrête.
   - **Solution :** *L'exécution spéculative* (*Backup Tasks*). Lorsque le job touche à sa fin, le Master lance une copie miroir des tâches traînardes sur d'autres nœuds. La première qui finit valide le résultat et la seconde est tuée. Cette simple idée réduit le temps d'exécution global de 30% à 40%.
2. **La fonction Combiner (Mini-Reduce local) :**
   - Évite d'engorger le réseau lors de la phase de *Shuffle*.
   - Exemple du WordCount : au lieu d'envoyer un milliard de messages `("le", 1)` à travers le commutateur réseau, le nœud de Map exécute un `Combiner` local qui émet `("le", 48200)`.

### 2.3 Spark & RDDs : Dépendances Étroites vs Dépendances Larges (Zaharia et al., 2012)

Le papier phare d'Apache Spark (*Resilient Distributed Datasets: A Fault-Tolerant Abstraction for In-Memory Cluster Computing*, NSDI 2012) détaille pourquoi Spark surclasse Hadoop :

```
    DÉPENDANCE ÉTROITE (Narrow)                DÉPENDANCE LARGE (Wide)
   Ex: map, filter, union                      Ex: groupByKey, join, distinct
   
   Partition Parent -> 1 Partition Enfant      Partition Parent -> Multiples Enfants
   
   [ P1 ] ────────────► [ C1 ]                 [ P1 ] ───┬────────► [ C1 ]
   [ P2 ] ────────────► [ C2 ]                           └────────► [ C2 ]
   [ P3 ] ────────────► [ C3 ]                 [ P2 ] ───┬────────► [ C1 ]
                                                         └────────► [ C2 ]
   - Aucune copie réseau (Pipeline en RAM)     - SHUFFLE RÉSEAU OBLIGATOIRE
   - Tolérance aux pannes locale               - Barrière de synchronisation lourde
```

* **Dépendances étroites (*Narrow Dependencies*) :** Chaque partition du parent est utilisée par au maximum une partition enfant. Les opérations sont pipelinées en un seul bloc de mémoire CPU sans toucher au disque ni au réseau. Si une partition est perdue, elle se recalcule instantanément de manière isolée.
* **Dépendances larges (*Wide Dependencies*) :** Multiples partitions enfants dépendent des données d'une même partition parent (ex. `reduceByKey`). Cela impose un **Shuffle**, où les données doivent être sérialisées, partitionnées par hachage et transférées sur le réseau vers d'autres exécuteurs. C'est l'opération la plus coûteuse dans Spark.

---

## 3. Formats de Fichiers Avancés et Représentation en Mémoire

### 3.1 Anatomie d'un fichier Apache Parquet
Un fichier Parquet n'est pas un simple tableau ; c'est un format binaire hiérarchique complexe optimisé pour les processeurs modernes :

```
┌────────────────────────────────────────────────────────┐
│                      MAGIC BYTES                       │
├────────────────────────────────────────────────────────┤
│ ROW GROUP 0 (Ex: 128 Mo - 1 000 000 lignes)            │
│  ├── Column Chunk 1 (ex: 'user_id' - Snappy compressed)│
│  │    ├── Page 1 (Dictionnaire d'encodage)             │
│  │    └── Page 2 (Données encodées RLE / Bit-Packed)   │
│  ├── Column Chunk 2 (ex: 'country')                    │
│  └── Column Chunk 3 (ex: 'revenue')                    │
├────────────────────────────────────────────────────────┤
│ ROW GROUP 1 (Ex: 128 Mo)                               │
│  └── ...                                               │
├────────────────────────────────────────────────────────┤
│ FOOTER (Métadonnées globales lues EN PREMIER)          │
│  ├── Schéma complet des colonnes                       │
│  ├── Statistiques par Row Group (Min, Max, Null Count) │
│  └── Pointeurs d'offset pour chaque Column Chunk       │
├────────────────────────────────────────────────────────┤
│ 4-byte Footer Length + MAGIC BYTES                     │
└────────────────────────────────────────────────────────┘
```

#### Techniques d'encodage de pointe intégrées dans Parquet :
1. **Dictionnaires d'encodage (*Dictionary Encoding*) :** Si une colonne contient des pays (`"Canada"`, `"USA"`, `"France"`), Parquet remplace chaque chaîne par un entier court (`0, 1, 2`), réduisant l'empreinte mémoire de 90%.
2. **Run-Length Encoding (RLE) & Bit-Packing :** Compresse des séquences de valeurs répétitives identiques (`1, 1, 1, 1, 1` $\rightarrow$ `5x1`).
3. **Encodage Delta :** Idéal pour les identifiants auto-incrémentés ou les timestamps (stocke uniquement la différence $\Delta$ entre valeurs successives).

### 3.2 Apache Arrow : La Révolution In-Memory Zéro-Copie
Pendant longtemps, chaque outil (Spark, Pandas, Python, R, C++) avait sa propre structure d'objets en mémoire. Pour passer une table Spark à une bibliothèque Python ou PyTorch, il fallait :
1. Sérialiser les objets Java JVM en octets,
2. Envoyer ces octets via un socket local,
3. Désérialiser les octets en objets Python/C++.
*Cet overhead de sérialisation représentait jusqu'à 80% du temps d'exécution dans les architectures hybrides !*

**Apache Arrow** standardise un format colonnaire unifié **directement en mémoire vive (RAM)**. Arrow utilise la mémoire partagée POSIX (*Shared Memory / Plasma*) : Spark écrit la table en mémoire vive, et Python/PyTorch lit directement la même zone de mémoire vive physique sans aucune copie d'octets (*Zero-Copy Memory Sharing*). C'est le socle des ponts ultra-rapides PySpark / Pandas (via Arrow / PyArrow).

---

## 4. De l'Architecture Lambda/Kappa au Data Lakehouse Moderne

### 4.1 La Guerre des Architectures de Flux : Lambda vs Kappa

```
ARCHITECTURE LAMBDA (Nathan Marz)
                       ┌──► [Batch Layer (Hadoop/Spark)] ──► [Batch View] ──┐
[Flux d'événements] ──┤                                                     ├──► [Serving View Unifiée]
                       └──► [Speed Layer (Storm/Flink)]   ──► [Realtime View]┘
* Inconvénient majeur : Deux bases de code distinctes à maintenir pour la même logique !

ARCHITECTURE KAPPA (Jay Kreps / Apache Kafka)
[Log Immuable (Kafka)] ──► [Moteur de Stream Unifié (Flink/Spark Streaming)] ──► [Serving View]
* Principe : Le stream est le modèle universel. Le batch n'est qu'un cas particulier de stream rejoué depuis le début.
```

### 4.2 L'Avènement du Data Lakehouse (Delta Lake, Apache Iceberg, Apache Hudi)
Pendant une décennie, les entreprises ont souffert de la dualité **Data Warehouse** (performant, SQL, ACID, mais propriétaire et cher) vs **Data Lake** (économique, formats ouverts, mais sans transactions, sans contrôle de concurrence et chaotique).

Le **Data Lakehouse** résout cette fracture en ajoutant une **couche transactionnelle de métadonnées** au-dessus du stockage objet brut (fichiers Parquet sur S3 ou HDFS) :

1. **Transactions ACID complètes :**
   - Implémentées via un journal de validation d'écritures (*Write-Ahead Log / ARIES-style transaction log*).
   - Plusieurs moteurs (Spark, Presto, DuckDB) peuvent lire et écrire en même temps sans verrouillage grâce à l'isolation par instantané (*Snapshot Isolation / MVCC*).
2. **Voyage dans le temps (*Time Travel*) et Rollback :**
   - Comme chaque modification crée un nouvel instantané sans écraser les anciens fichiers Parquet, on peut interroger les données telles qu'elles étaient il y a 3 semaines :
     ```sql
     SELECT * FROM transactions TIMESTAMP AS OF '2026-08-15 12:00:00';
     ```
3. **Partitionnement caché et évolution de schéma sans réécriture :**
   - Apache Iceberg n'utilise pas la hiérarchie classique de dossiers de répertoires (`/year=2026/month=09/`). Le partitionnement est géré dans les métadonnées JSON/Avro. Vous pouvez changer la colonne de partitionnement sans avoir à réécrire des téraoctets de données.

---

## 5. Synthèse des Évolutions Technologiques (Frise 2003–2026)

| Époque | Technologie Phare | Problème Clé Résolu | Goulot d'Étranglement Découvert |
| :--- | :--- | :--- | :--- |
| **2003–2006** | GFS / MapReduce / Bigtable | Stockage et calcul sur serveurs banalisés | I/O disque lourd, latence élevée |
| **2006–2010** | Apache Hadoop (HDFS, YARN) | Démocratisation open-source du modèle Google | Complexité opérationnelle, lenteur en Machine Learning |
| **2010–2015** | Apache Spark, Kafka | Calcul en mémoire RAM, streaming temps réel | Gestion de mémoire JVM (Garbage Collection), sérialisation |
| **2015–2020** | Spark SQL / Catalyst, Arrow | Optimisation déclarative de requêtes, format RAM unifié | Gestion du lac de données (corruption, manque d'ACID) |
| **2020–2026** | Data Lakehouse (Iceberg, Delta), Découplage Cloud | ACID sur stockage objet ouvert, séparation stricte Compute/Storage, intégration LLM/Vecteurs | Gouvernance multi-moteurs, coûts de transfert réseau cloud (*egress fees*) |

---

## 6. Questions Ouvertes et Réflexions pour la Maîtrise

1. **Sur le compromis CAP :** Si un partitionnement réseau survient entre deux datacenters pour une application de réservation de sièges d'avion à haut volume, quel choix d'architecture (CP vs AP) adopteriez-vous et comment géreriez-vous la compensation métier ?
2. **Sur l'allocation mémoire Spark :** Pourquoi le Garbage Collection (GC) de la machine virtuelle Java (JVM) a-t-il poussé les créateurs de Spark à développer le projet Tungsten (mémoire *off-heap* non gérée par le ramasse-miettes Java) ?
3. **Sur le format colonnaire :** Pour quel type d'application un format colonnaire (Parquet) serait-il nettement **moins performant** qu'un format orienté lignes (Avro ou PostgreSQL) ? *(Indice : pensez aux transactions bancaires OLTP unitaires avec `INSERT` unitaire à haute fréquence).*
