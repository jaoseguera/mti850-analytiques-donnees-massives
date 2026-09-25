# MTI850 — Analytique des données massives
## Synthèse & Récapitulatif du Cours 02 : Cluster Hadoop, HDFS, Approvisionnement Matériel et Pipeline de Données

**Enseignant :** Prof. Alessandro L. Koerich  
**Département :** Génie logiciel et des TI, École de technologie supérieure (ÉTS)  
**Session :** Automne 2026  

---

## 1. Architecture Physique et Logique d'un Cluster Hadoop

Un cluster Hadoop est une grappe de serveurs interconnectés (*nœuds*) travaillant de concert pour stocker et analyser de gigantesques volumes de données non structurées ou semi-structurées, en s'appuyant sur du matériel banalisé (*commodity hardware*) et une mise à l'échelle horizontale (*scale-out*).

```
                      ┌──────────────────────────────────────────────┐
                      │          LOGICAL HADOOP ARCHITECTURE         │
                      └──────────────────────┬───────────────────────┘
                                             │
                     ┌───────────────────────┴───────────────────────┐
                     ▼                                               ▼
      ┌─────────────────────────────┐                 ┌─────────────────────────────┐
      │         MASTER NODE         │                 │         WORKER NODE         │
      │                             │                 │                             │
      │  • HDFS NameNode            │◄──Heartbeats───►│  • HDFS DataNode            │
      │    (Namespace & Metadata)   │                 │    (Block Storage on disks) │
      │                             │                 │                             │
      │  • YARN ResourceManager     │◄──Allocations──►│  • YARN NodeManager         │
      │    (Cluster Resource Mgmt)  │                 │    (Container Monitoring)   │
      └─────────────────────────────┘                 └─────────────────────────────┘
```

### 1.1 Architecture Logique : Rôles Maîtres et Esclaves
1. **Couche de Stockage (HDFS - Hadoop Distributed File System) :**
   - **NameNode (Maître) :** 
     - Gère l'arborescence du système de fichiers (*namespace*), les droits d'accès, et la cartographie des blocs (quel bloc physique réside sur quel DataNode).
     - Conserve toutes les métadonnées **exclusivement en mémoire vive (RAM)** pour garantir des temps de réponse ultra-rapides aux clients.
   - **DataNode (Esclave / Ouvrier) :** 
     - Un démon DataNode s'exécute sur chaque machine esclave du cluster.
     - Stocke et lit les blocs de données bruts sur ses disques locaux.
     - Envoie périodiquement des signaux de vie (*Heartbeats*) et des rapports de blocs (*Block Reports*) au NameNode.
2. **Couche de Gestion des Ressources (YARN - Yet Another Resource Negotiator) :**
   - **ResourceManager (Maître) :** Arbitre l'attribution des ressources (processeurs, mémoire) entre toutes les applications concurrentes du cluster. Il se divise en :
     - *Scheduler :* Alloue les ressources sans surveiller l'état de l'application.
     - *ApplicationManager :* Instancie les coordonnateurs d'applications (*ApplicationMaster*) et les redémarre en cas de panne.
   - **NodeManager (Esclave) :** Surveille l'utilisation des ressources locales (CPU, RAM dans des conteneurs isolés) et rapporte la santé du nœud au ResourceManager.
   - **ApplicationMaster (Par application) :** Une instance par job (ex. un job Spark ou MapReduce). Il négocie les conteneurs auprès du ResourceManager et pilote l'exécution des tâches avec les NodeManagers.

---

## 2. Fonctionnement Interne d'HDFS

HDFS repose sur un principe fondateur : **il est conçu pour stocker un nombre modéré de très gros fichiers (plusieurs centaines de Mo ou Go) plutôt qu'un nombre massif de petits fichiers (quelques Ko).**

### 2.1 Découpage en Blocs et Facteur de Réplication
* **Taille de bloc par défaut :** **128 Mo** (dans Hadoop 2.x et 3.x), contre 4 Ko sur un système de fichiers classique.
* **Facteur de réplication :** Par défaut fixé à **3**. Chaque bloc est dupliqué sur 3 machines différentes.
* **Politique de placement des répliques (*Rack Awareness*) :**
  - **Réplique 1 :** Sur un nœud local du même rack que le client (ou un nœud aléatoire si le client est hors cluster).
  - **Réplique 2 :** Sur un nœud situé dans un **rack différent** (protection contre la panne d'un commutateur de rack ou d'une baie électrique entière).
  - **Réplique 3 :** Sur un nœud différent du second rack (économie de bande passante inter-rack tout en maintenant la résilience).
* **Évolution Hadoop 3.0 :** Introduction d'*Erasure Coding* (ex. Reed-Solomon), permettant de diviser le surcoût de stockage par deux par rapport à la réplication x3 brute (1,5x au lieu de 3,0x).

### 2.2 Le Protocole de Lecture (Read Path)
```
[ Client ] ──1. Demande blocs du fichier──► [ NameNode ]
[ Client ] ◄──2. Renvoie adresses nœuds/blocs───┘
[ Client ] ──3. Lecture directe du bloc le plus proche (Rack Local)──► [ DataNode ]
```
1. Le client contacte le NameNode en lui transmettant le chemin du fichier.
2. Le NameNode consulte sa table en mémoire et retourne la liste des blocs et l'adresse des DataNodes qui les hébergent, classés par proximité topologique (même nœud > même rack > rack distant).
3. Le client ouvre un flux direct (`FSDataInputStream`) avec le DataNode le plus proche et lit le bloc sans repasser par le NameNode.

### 2.3 Le Protocole d'Écriture en Pipeline (Write Path)
```
[ Client ] ──1. Création fichier──► [ NameNode ]
[ Client ] ◄──2. Renvoie liste de 3 DataNodes (DN1, DN2, DN3)──┘
[ Client ] ──3. Émet paquets (64 Ko)──► [ DataNode 1 ] ──Pipeline──► [ DataNode 2 ] ──Pipeline──► [ DataNode 3 ]
[ Client ] ◄──4. Acknowledgment (Acquittement de la chaîne)────────────────────────────────────────┘
```
1. Le client demande au NameNode l'autorisation de créer un fichier.
2. Le NameNode vérifie les autorisations et renvoie une liste de 3 DataNodes assignés pour le premier bloc.
3. Le client diffuse le bloc découpé en petits paquets de 64 Ko vers le **DataNode 1**.
4. Le **DataNode 1** enregistre le paquet sur son disque et le transmet immédiatement au **DataNode 2**, qui l'enregistre et le transmet au **DataNode 3** : c'est la **réplication en pipeline**.
5. Une fois que le dernier DataNode a écrit le paquet, une chaîne d'acquittements (*ACKs*) remonte jusqu'au client.

---

## 3. Approvisionnement Matériel et Dimensionnement (*Hardware Provisioning*)

Dimensionner un cluster pour Apache Spark et Hadoop ne relève pas de l'improvisation : c'est un compromis précis entre stockage, entrées/sorties disque, mémoire vive et bande passante réseau.

### 3.1 Règles Directrices de Configuration Matérielle

| Ressource | Recommandation Clé | Rationale / Explication Technique |
| :--- | :--- | :--- |
| **Stockage & Co-location** | Co-localiser Spark et HDFS sur les mêmes nœuds. | Élimine le trafic réseau initial en lisant les données en local (*Data Locality : NODE_LOCAL*). Découpler uniquement pour les bases NoSQL à très faible latence (HBase). |
| **Disques Locaux** | **4 à 8 disques par nœud, SANS RAID (JBOD).** | HDFS gère déjà la redondance logicielle (réplication x3). Le RAID matériel ajoute un coût et pénalise la vitesse d'écriture. Le mode JBOD (*Just a Bunch of Disks*) fournit plusieurs canaux d'E/S indépendants pour le stockage et les fichiers de *Shuffle* temporaires de Spark. |
| **Mémoire (RAM)** | **Allouer au maximum 75% de la RAM à Spark/YARN.** | Les 25% restants sont cruciaux pour l'OS, les démons système (DataNode, NodeManager) et surtout le cache de pages du noyau Linux (*OS buffer cache*). |
| **Gros Nœuds (> 200 Go)**| Déployer plusieurs travailleurs JVM par nœud. | Les ramasse-miettes de Java (GC JVM) deviennent très lents au-delà de 32 à 64 Go par machine virtuelle, entraînant de longs gels de calcul (*Stop-The-World pauses*). |
| **Réseau** | **Minimum 10 GbE (idéalement 25 GbE+).** | Dès que les données sont chargées en mémoire, les opérations Spark (`reduceByKey`, `groupBy`, `join`) deviennent **goulot d'étranglement réseau** lors de la redistribution (*Shuffle*). |
| **Processeurs (CPU)** | **8 à 16 cœurs par nœud.** | Spark excelle dans l'exécution parallèle avec peu de concurrence mémoire entre threads. Au-delà de 16 cœurs par nœud, la contention du bus mémoire peut réduire les gains. |

---

## 4. Étude de Cas : L'Infrastructure Données de Meta (Facebook)

L'étude de cas présentée en classe démontre le passage de la théorie à l'échelle industrielle mondiale :

* **Évolution de l'échelle (2021 à 2025/2026) :**
  - Plus de 3 milliards d'utilisateurs actifs mensuels (Facebook, Instagram, WhatsApp).
  - Entrepôts de données mesurés en centaines de pétaoctets (PB), migrant vers l'ordre de grandeur de l'**Exaoctet**.
  - Le plus grand cluster Hadoop au monde : plus de 4 000 machines physiques interconnectées.
* **Pipeline d'Ingestion :**
  - Des milliers de serveurs web reçoivent les actions utilisateurs (Publications, Likes, Partages, Vidéos).
  - Les serveurs **Scribe** agrègent ces flux d'événements en continu et écrivent des fichiers de logs bruts volumineux directement sur HDFS (`scribeh clusters`).
* **Moteurs de Traitement Spécialisés :**
  - **Presto (Trino) :** Moteur SQL distribué développé en interne par Meta, optimisé pour les requêtes interactives instantanées sur HDFS sans passer par le modèle lent de MapReduce.
  - **Hive :** Utilisé pour les traitements batch massifs planifiés la nuit.
  - **Spark & ML Pipelines :** Utilisé pour l'analytique en mémoire, le traitement de graphe social et l'apprentissage automatique prédictif.

---

## 5. De la Donnée Brute au Pipeline de Machine Learning Distribué

Le cas pratique présenté par le professeur Koerich détaille les étapes concrètes menant d'un log textuel brut à un modèle statistique en production :

```
[ Log Brut HDFS ] ──► [ RDD / DataFrame ] ──► [ Nettoyage & Regex ] ──► [ Extraction Schéma ] ──► [ VectorAssembler ] ──► [ Pipeline MLlib (fit/transform) ]
```

1. **Ingestion brute en mémoire :**
   ```python
   # Lecture depuis HDFS dans Spark
   df_raw = spark.read.text("hdfs://namenode:9000/data/logs/facebook_activity.log")
   ```
2. **Nettoyage et structuration (*Impose Structure*) :**
   - Utilisation d'expressions régulières ou de délimiteurs pour découper la chaîne brute en colonnes distinctes (`user`, `timestamp`, `action`, `content`).
   - Conversion des formats de chaînes vers des types stricts (ex. `to_timestamp`).
3. **Préparation des caractéristiques (*Feature Engineering*) :**
   - **Données numériques :** Regroupement des colonnes explicatives sous forme de vecteur dense grâce au `VectorAssembler`.
   - **Données non numériques (texte, catégories) :**
     - Indexation de chaînes (*StringIndexer*).
     - Encodage disjonctif complet (*One-Hot Encoding - OHE*) pour transformer les catégories en vecteurs creux (*SparseVector*).
4. **Le Pipeline Spark ML :**
   - **Estimator :** Algorithme qui s'entraîne sur un DataFrame pour produire un modèle (ex. `LinearRegression.fit(train_df)`).
   - **Transformer :** Algorithme qui transforme un DataFrame en un autre (ex. `model.transform(test_df)` pour ajouter la colonne `prediction`).
   - **Evaluator :** Mesure de la performance statistique (RMSE, précision, AUC).
5. **Déploiement sur flux continu (*Streaming DataFrames*) :**
   - Le pipeline entraîné en batch est déployé directement sur un flux temps réel (*Structured Streaming*) sans modifier une seule ligne de logique de prédiction.

---

## 6. Révision Mathématique et Fondements Python (Travaux Pratiques / PD1)

Pour réussir les laboratoires pratiques (Lab 0 et PD1), les concepts suivants doivent être maîtrisés :

* **Algèbre Linéaire Fondamentale :**
  - Produit scalaire de vecteurs : $\mathbf{u} \cdot \mathbf{v} = \sum_{i=1}^n u_i v_i$.
  - Multiplication élément par élément (*Hadamard product*) : $\mathbf{u} \odot \mathbf{v}$.
  - Multiplication matricielle : $C_{ij} = \sum_k A_{ik} B_{kj}$.
* **Objets PySpark MLlib :**
  - `DenseVector` : Tableau NumPy classique stockant toutes les valeurs en continu en mémoire.
  - `SparseVector` : Stocke uniquement les indices non nuls et leurs valeurs, indispensable pour les données catégoriques encodées (One-Hot) ou le traitement de texte (TF-IDF) afin d'économiser jusqu'à 99% de RAM.
* **Paradigme Fonctionnel Python :**
  - Fonctions anonymes `lambda` : syntaxe compacte sans instruction `return` explicite.
  - `map(fonction, iterable)` : Applique la transformation sur chaque élément.
  - `filter(predicat, iterable)` : Filtre les éléments respectant une condition booléenne.
  - *Composabilité :* Enchaînement fonctionnel fluide imitant le comportement des RDD et DataFrames dans Spark.
