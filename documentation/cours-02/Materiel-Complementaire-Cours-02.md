# MTI850 — Analytique des données massives
## Matériel Complémentaire Avancé : Haute Disponibilité HDFS, Erasure Coding, Dimensionnement Spark et Stockage Objet

**Complément au Cours 02 :** Approfondissement technique de niveau Maîtrise  
**Auteur / Référence d'étude :** Analyse avancée pour l'ingénierie des données massives (ÉTS Montréal)

---

## 1. Sous le Capot du NameNode : Persistance et Haute Disponibilité (HA)

Le rôle central du NameNode en fait le point critique le plus vulnérable de l'architecture Hadoop initiale. Comprendre sa persistance et son basculement sans perte de données est fondamental.

### 1.1 Le Modèle de Persistance : FSImage et EditLog

Le NameNode maintient toutes les métadonnées de l'espace de noms en mémoire vive (RAM). Pour survivre à une extinction ou un redémarrage, il s'appuie sur deux structures sur disque :

```
┌────────────────────────────────────────────────────────────────────────┐
│                        PERSISTANCE DU NAMENODE                         │
├────────────────────────────────────────────────────────────────────────┤
│  FSImage  : Instantané binaire figé de l'arbre complet du système      │
│             de fichiers à un instant T (lecture seule au démarrage).   │
├────────────────────────────────────────────────────────────────────────┤
│  EditLog  : Journal des modifications séquentielles (Write-Ahead Log). │
│             Chaque création, suppression ou renommage y est consigné   │
│             avant d'être appliqué en mémoire vive.                    │
└────────────────────────────────────────────────────────────────────────┘
```

#### Le Mythe du "Secondary NameNode" : Démystification
> **Erreur fréquente :** De nombreux ingénieurs pensent que le *Secondary NameNode* est un serveur de secours prêt à prendre le relais en cas de panne du NameNode. **C'est faux !**
> - Le Secondary NameNode est un simple **assistant de fusion** (*Checkpointing Helper*).
> - Sans lui, l'EditLog grossirait indéfiniment (plusieurs dizaines de gigaoctets). Au redémarrage, le NameNode mettrait des heures à rejouer chaque opération.
> - Le Secondary NameNode télécharge périodiquement la `FSImage` et l'actuel `EditLog` par HTTP, les fusionne sur son propre disque en une nouvelle `FSImage.ckpt`, et la renvoie au NameNode principal. 
> - Si le NameNode principal brûle, le Secondary NameNode **ne prend pas** le relais automatiquement.

```
       ┌─────────────────────────────────────────────────────────────┐
       │                LE CYCLE DU CHECKPOINTING                    │
       └──────────────────────────────┬──────────────────────────────┘
                                      │
  [ NameNode Principal ]              │              [ Secondary NameNode ]
  ──────────────────────              │              ──────────────────────
  EditLog actuel fermé                │
  Nouvel EditLog ouvert               │
  FSImage + EditLogs fermés ──────────┼────────────► Téléchargement HTTP
                                      │              Fusionne FSImage + EditLog
                                      │              Crée nouvelle FSImage
  Nouvelle FSImage reçue ◄────────────┼───────────── Renvoyée par HTTP
```

### 1.2 La Véritable Haute Disponibilité Moderne (Hadoop 2.x et 3.x)

Pour éliminer ce point unique de défaillance (*Single Point of Failure - SPOF*), les clusters de production utilisent une architecture **NameNode Actif / Passif (Standby)** :

```
                        ┌───────────────────────────────┐
                        │    APACHE ZOOKEEPER QUORUM    │
                        │    (Élection du Leader Actif) │
                        └───────┬───────────────┬───────┘
                                │               │
                        ┌───────▼───────┐┌──────▼───────┐
                        │   ZKFC 1      ││   ZKFC 2     │
                        └───────┬───────┘└──────┬───────┘
                                │               │
                        ┌───────▼───────┐┌──────▼───────┐
                        │ NameNode (A)  ││ NameNode (S) │
                        │    ACTIF      ││   STANDBY    │
                        └───────┬───────┘└──────┬───────┘
                                │               │
                                ▼               ▼
                        ┌───────────────────────────────┐
                        │    JOURNALNODES QUORUM (QJM)  │
                        │  (Journal partagé immuable)   │
                        └───────────────────────────────┘
```

1. **Quorum Journal Manager (QJM) :** Un ensemble impair de démons légers (*JournalNodes*, typiquement 3 ou 5). Le NameNode Actif consigne chaque modification sur la majorité stricte des JournalNodes (protocole de consensus de type Paxos/Raft). Le NameNode Standby écoute ces journaux en temps réel pour synchroniser sa mémoire.
2. **ZooKeeper Failover Controller (ZKFC) :** Un processus qui surveille la santé du NameNode local. En cas de crash, les ZKFC élisent le Standby comme nouvel Actif en moins de 3 secondes.
3. **Mécanisme de Clôture (*Fencing / STONITH*) :** Indispensable pour éviter le syndrome de « double cerveau » (*Split-Brain*). Avant de promouvoir le Standby, le système coupe l'alimentation électrique du nœud défaillant via son interface IPMI matérielle (*Shoot The Other Node In The Head* - STONITH) ou révoque ses ports réseau par SSH pour s'assurer qu'il ne pourra plus jamais émettre d'instructions contradictoires aux DataNodes.

---

## 2. Erasure Coding (EC) : La Révolution du Stockage dans Hadoop 3.x

Pendant 15 ans, le facteur de réplication x3 a été la règle d'or d'HDFS : pour stocker 100 To de données, il fallait acheter 300 To de disques (surcoût de stockage de **200%**). 

Dans les centres de données de Meta, Google ou Netflix stockant des exaoctets, ce coût matériel est astronomique. Hadoop 3.0 a introduit le support natif d'**Erasure Coding (EC)** basé sur les codes de Reed-Solomon.

### 2.1 Principes Mathématiques de Reed-Solomon $(k, m)$
Le principe consiste à découper un ensemble de données en $k$ cellules de données et à calculer $m$ cellules de parité par multiplication matricielle dans un corps de Galois $\text{GF}(2^8)$ :

$$\text{Données réparties en } k \text{ blocs} \quad \xrightarrow{\text{Générateur Matriciel}} \quad m \text{ blocs de parité}$$

Le système peut alors tolérer la perte simultanée de **n'importe quels $m$ blocs** parmi les $k + m$ générés !

```
REPRÉSENTATION DU SCHÉMA REED-SOLOMON RS(6, 3)

[ Cellule D1 ] ── DataNode 1
[ Cellule D2 ] ── DataNode 2
[ Cellule D3 ] ── DataNode 3        Peut tolérer la destruction simultanée
[ Cellule D4 ] ── DataNode 4        de N'IMPORTE QUELS 3 NŒUDS
[ Cellule D5 ] ── DataNode 5        sans perdre le moindre bit de données !
[ Cellule D6 ] ── DataNode 6
────────────────────────────
[ Parité P1  ] ── DataNode 7
[ Parité P2  ] ── DataNode 8
[ Parité P3  ] ── DataNode 9
```

### 2.2 Comparaison : Réplication x3 vs Erasure Coding

| Métrique | Réplication Classique (x3) | Erasure Coding RS(6, 3) | Erasure Coding RS(10, 4) |
| :--- | :--- | :--- | :--- |
| **Surcoût Stockage (*Overhead*)** | **+200% (3,0x)** | **+50% (1,5x)** | **+40% (1,4x)** |
| **Tolérance aux pannes** | 2 pannes de disques/nœuds | **3 pannes simultanées** | **4 pannes simultanées** |
| **Impact CPU à l'écriture** | Quasi nul | Modéré (calculs matriciels) | Modéré (accéléré par Intel ISA-L) |
| **Impact Réseau à la reconstruction** | Faible (copie d'un bloc) | Élevé (doit lire $k$ blocs sur le réseau pour recalculer le bloc perdu) | Élevé |
| **Cas d'usage optimal** | Données chaudes (*Hot data*), accès ultra-fréquents | Données tièdes/froides (*Warm/Cold data*), archives | Données volumineuses peu accédées |

---

## 3. Dimensionnement Mathématique d'un Cluster Spark en Production

Le dimensionnement des conteneurs Spark sur YARN est l'une des compétences les plus recherchées en ingénierie de données. Une mauvaise configuration conduit soit au gaspillage massif de ressources, soit à des erreurs fatales d'allocation mémoire (`java.lang.OutOfMemoryError`).

### 3.1 La Formule de l'Exécuteur Idéal (*The Sweet Spot*)

Supposons un cluster de production composé de **10 nœuds ouvriers (workers)** ayant chacun :
- **16 cœurs CPU**
- **64 Go de mémoire RAM**

#### Étape 1 : Réserver les ressources système de base
Chaque machine physique a besoin de CPU et de RAM pour le noyau Linux et le démon YARN NodeManager.
- On réserve **1 cœur par nœud** pour l'OS $\rightarrow$ $16 - 1 = 15$ cœurs disponibles pour Spark par machine.
- On réserve **4 Go de RAM par nœud** pour l'OS et le cache $\rightarrow$ $64 - 4 = 60$ Go disponibles pour Spark par machine.
- Total disponible sur le cluster (10 nœuds) : $150$ cœurs et $600$ Go de RAM.

#### Étape 2 : Déterminer le nombre optimal de cœurs par exécuteur
> **La règle empirique des 5 cœurs :**
> - Plus de 5 cœurs par exécuteur ($> 5$) entraîne une concurrence excessive sur les threads et dégrade le débit des lectures/écritures HDFS tout en compliquant le travail du ramasse-miettes Java.
> - Moins de 3 cœurs par exécuteur ($< 3$) supprime les avantages du multithreading partagé au sein d'une même JVM.
> - **Le choix parfait : 5 cœurs par exécuteur.**

$$\text{Exécuteurs par nœud} = \frac{15 \text{ cœurs disponibles}}{5 \text{ cœurs / exécuteur}} = 3 \text{ exécuteurs / nœud}$$
$$\text{Nombre total d'exécuteurs physiques} = 10 \text{ nœuds} \times 3 = 30 \text{ exécuteurs}$$

#### Étape 3 : Réserver 1 exécuteur pour l'ApplicationMaster
Sur l'ensemble du cluster, le coordonnateur YARN (`ApplicationMaster`) doit tourner dans son propre conteneur :
$$\text{Nombre final d'exécuteurs Spark} = 30 - 1 = \mathbf{29 \text{ exécuteurs}} \quad (\text{paramètre } \texttt{--num-executors 29})$$

#### Étape 4 : Calculer la mémoire par exécuteur
Chaque nœud héberge 3 exécuteurs et possède 60 Go de RAM disponible pour Spark :
$$\text{Mémoire brute par exécuteur} = \frac{60 \text{ Go}}{3} = 20 \text{ Go}$$

Dans Spark, la mémoire totale d'un conteneur se divise en mémoire JVM de l'exécuteur (`spark.executor.memory`) et en mémoire de surcharge hors-heap (`spark.executor.memoryOverhead`), fixée par défaut à $10\%$ :
$$\text{Overhead} = 20 \text{ Go} \times 0.10 = 2 \text{ Go}$$
$$\text{Mémoire d'exécution JVM} = 20 \text{ Go} - 2 \text{ Go} = \mathbf{18 \text{ Go}} \quad (\text{paramètre } \texttt{--executor-memory 18g})$$

```
COMMANDE DE LANCEMENT SPARK OPTIMISÉE POUR LE CLUSTER :
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 29 \
  --executor-cores 5 \
  --executor-memory 18g \
  --conf spark.executor.memoryOverhead=2048m \
  mon_pipeline.py
```

---

## 4. HDFS vs Stockage Objet Cloud (AWS S3, Google Cloud Storage, Azure ADLS)

Dans les architectures modernes cloud natives, la question du remplacement d'HDFS par du stockage objet distribué (comme Amazon S3 ou Google Cloud Storage) est omniprésente.

### 4.1 Comparaison des Deux Paradigmes

```
         HDFS (Hadoop)                              CLOUD OBJECT STORAGE (S3, GCS)
 ┌─────────────────────────────┐                    ┌─────────────────────────────┐
 │ Calcul et Stockage          │                    │ Stockage Découplé           │
 │ CO-LOCALISÉS sur le même    │                    │ Calcul Éphémère (K8s/EMR)   │
 │ serveur physique.           │                    │ Stockage persistant externe │
 └──────────────┬──────────────┘                    └──────────────┬──────────────┘
                ▼                                                  ▼
     Débit Local Ultra-Rapide                           Élasticité et Coût Réduit
     (Bande passante bus disque)                        (Facturation au gigaoctet réel)
```

| Propriété | HDFS (On-Premise / Dédié) | Stockage Objet Cloud (AWS S3, GCS) |
| :--- | :--- | :--- |
| **Couplage Calcul/Stockage** | Fortement couplé (*co-located*). Pour augmenter le stockage, on doit acheter des serveurs avec CPU et RAM. | **Complètement découplé.** Le stockage grandit à l'infini indépendamment des serveurs de calcul. |
| **Sémantique de Système de Fichiers** | Véritable système hiérarchique POSIX avec répertoires physiques. | Espace plat de type Clé-Valeur (`bucket/cle`). Les « dossiers » ne sont que des préfixes de chaînes de caractères. |
| **Opération de Renommage de Dossier** | **$O(1)$ atomique :** Le NameNode change simplement le pointeur du nœud dans son arbre en mémoire. | **$O(N)$ non-atomique :** Pour renommer un dossier de 10 000 fichiers, S3 doit **copier chaque objet un par un** puis supprimer les anciens ! |
| **Gestion des Petits Fichiers** | Très mauvaise (sature la RAM du NameNode). | Excellente (aucun problème pour gérer des milliards d'objets indépendants). |
| **Cohérence des données** | Cohérence stricte immédiate (*Strong Consistency*). | Historiquement cohérence éventuelle (*Eventual Consistency*), devenue stricte en lecture-après-écriture chez AWS en déc. 2020. |

### 4.2 Pourquoi le Data Lakehouse est indispensable sur le Cloud
En raison de la lenteur du renommage sur S3 ($O(N)$) et du risque d'écritures partielles lors des pannes de tâches Spark, les formats de table modernes (**Apache Iceberg**, **Delta Lake**) ont été inventés.
Ils contournent les limites du stockage objet en gérant un journal des transactions ACID au niveau des métadonnées. Au lieu de déplacer physiquement des milliers de fichiers lors d'un commit Spark, Iceberg met simplement à jour un pointeur de métadonnées JSON/Avro, rétablissant la performance et l'atomicité $O(1)$ qui existaient dans HDFS !

---

## 5. Questions d'Examen et Réflexions Approfondies

1. **Sur le NameNode :** Si un cluster perd brutalement le courant électrique pendant que des écritures étaient en cours, comment le NameNode reconstruit-il son état cohérent au redémarrage en utilisant la `FSImage` et les `EditLogs` ?
2. **Sur le réseau et le Shuffle :** Pourquoi le protocole de réplication en pipeline d'HDFS est-il plus économe en bande passante globale pour le client qu'une écriture en étoile (*star replication*) ?
3. **Sur le partitionnement :** Pourquoi attribuer 1 seul cœur par exécuteur Spark (`--executor-cores 1`) est-il généralement sous-optimal en production ? *(Indice : perte du bénéfice des variables partagées en mémoire `Broadcast` et inefficacité des threads de contrôle JVM).*
