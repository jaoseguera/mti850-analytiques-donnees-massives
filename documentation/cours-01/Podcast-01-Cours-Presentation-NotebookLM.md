# Source Audio Overview (NotebookLM) — Épisode 1 : Genèse et Fondations du Big Data
## Guide de Discussion & Contenu Pédagogique pour Podcast Audio
**Contexte :** Cours MTI850 (Analytique des données massives), École de technologie supérieure (ÉTS), Montréal.  
**Sujet de l'épisode :** Démystifier le Big Data, la trilogie des papiers Google, la faillite des SGBD classiques et l'avènement d'Apache Spark.

---

### Introduction & Accroche Narrative
Aujourd'hui, nous plongeons au cœur de l'une des transitions technologiques les plus spectaculaires des vingt-cinq dernières années : le passage de l'informatique centralisée sur serveurs uniques aux architectures distribuées planétaires. 

Tout le monde utilise le terme « Big Data », souvent comme un buzzword marketing. Mais du point de vue d'un ingénieur système ou d'un chercheur en informatique, qu'est-ce que cela signifie réellement ? Pourquoi nos bases de données traditionnelles ont-elles explosé au début des années 2000 ? Et comment trois articles de recherche écrits par des ingénieurs de Google ont-ils redessiné toute l'industrie logicielle mondiale ?

---

### Segment 1 : La question piège — 1 Tébioctet, est-ce du Big Data ?

Imaginez qu'un collègue entre dans votre bureau et vous dise : « J'ai 1 To de données clients, il nous faut absolument un cluster Hadoop ou Spark de 50 nœuds ! » Que lui répondez-vous ?

La réponse d'un bon ingénieur est toujours : **« Ça dépend de la charge de travail (workload). »**
* Si vous devez simplement lire séquentiellement ce téraoctet une seule fois (*scan* unique) pour calculer une somme ou chercher un mot-clé, un simple ordinateur portable ou un serveur standard doté d'un disque SSD NVMe moderne peut transférer et traiter ces données à 3 ou 5 Go/seconde. Le calcul prendra moins de 5 minutes. Pas besoin de cluster !
* Mais changez maintenant la nature du travail :
  - Imaginez que ce téraoctet arrive sous la forme d'un flux continu de millions de micro-transactions par seconde qu'il faut valider en moins de 10 millisecondes.
  - Imaginez qu'il faille effectuer des jointures croisées complexes à répétition entre plusieurs tables non indexées.
  - Ou encore, imaginez que vous entraîniez un modèle d'apprentissage statistique itératif (comme une régression logistique ou un réseau de neurones) qui doit parcourir ces données 100 fois d'affilée en mémoire.
  
Soudain, votre machine unique étouffe. La mémoire vive sature, le processeur passe son temps à échanger des pages mémoire avec le disque (*swapping*), et l'exécution s'effondre. Le Big Data ne se mesure donc pas en gigaoctets absolus : **les données deviennent « massives » quand leur volume, leur vitesse, leur variété ou leur complexité de calcul excèdent les capacités physiques et pratiques d'un système conventionnel unique.**

---

### Segment 2 : Le mur des années 2000 et l'échec des bases relationnelles

Pour comprendre où nous en sommes, il faut se téléporter en 2000-2003. Le Web explose. Des entreprises comme Google tentent d'indexer des milliards de pages web, d'analyser les liens hypertextes et de répondre à des millions d'utilisateurs en une fraction de seconde.

Jusqu'alors, la règle d'or en entreprise était le modèle relationnel (Oracle, IBM DB2, Microsoft SQL Server, MySQL) reposant sur les garanties **ACID** (Atomicité, Cohérence, Isolation, Durabilité). Pour gérer plus de charge, on achetait un serveur plus gros, plus puissant, avec plus de processeurs et plus de RAM : c'est ce qu'on appelle le **scaling vertical** (*scale-up*).

Mais le scaling vertical a deux défauts fatals :
1. **La limite physique et financière :** Au-delà d'une certaine taille, doubler la puissance d'une machine coûte 10 ou 100 fois plus cher, jusqu'à frapper le mur de la physique (chaleur, limites des bus mémoire).
2. **Le point de défaillance unique (*Single Point of Failure*) :** Si votre super-serveur à 500 000 $ grille son alimentation ou sa carte mère, toute votre entreprise est hors-ligne.

Google a compris avant tout le monde qu'il fallait changer de paradigme : au lieu d'acheter une machine géante et infaillible, il fallait acheter des milliers de petits serveurs bon marché et peu fiables (*commodity hardware*), et transférer l'intelligence de la fiabilité **dans la couche logicielle**.

---

### Segment 3 : La Trilogie Sacrée de Google (2003–2006)

Google ne vendait pas de logiciel d'infrastructure à l'époque ; ils ont simplement publié leurs secrets dans trois articles académiques historiques :

#### 1. GFS (Google File System, 2003) — L'art de stocker sur du matériel jetable
* **Le problème :** Comment stocker des pétaoctets de fichiers géants si les disques durs crashent tous les jours ?
* **La trouvaille :** Un système de fichiers distribué avec une séparation stricte entre les métadonnées et les données réelles :
  - Un **Master unique** qui ne stocke que la carte du monde en RAM : « Le fichier X est découpé en blocs de 64 Mo, et le bloc #1 est sur les serveurs A, B et C ».
  - Des centaines de **Chunkservers** qui stockent les blocs physiques. Chaque bloc est dupliqué 3 fois par défaut sur des machines et des racks différents.
  - Le coup de génie : Le client ne fait transiter aucune donnée lourde par le Master. Il lui demande seulement les adresses, puis va lire directement en parallèle sur les Chunkservers. Le Master n'est jamais saturé !

#### 2. MapReduce (2004) — Le calcul distribué pour les mortels
* **Le problème :** Comment faire calculer 2 000 machines en même temps sans que les développeurs ne deviennent fous à gérer les sémaphores, les sockets réseau et les crashs de serveurs ?
* **La solution :** Réduire tout calcul à deux opérations primitives empruntées à la programmation fonctionnelle :
  - **Map :** Chaque machine prend son bloc de données local et filtre/transforme les lignes pour sortir des couples `(clé, valeur)`.
  - **Shuffle & Sort (la magie invisible) :** Le système de réseau regroupe automatiquement toutes les paires ayant la même clé vers un même serveur de réduction.
  - **Reduce :** Le réducteur prend toutes les valeurs d'une clé et les agrège (ex. fait la somme, trouve la médiane).
* **Tolérance aux pannes :** Si un serveur brûle pendant son Map, le système relance simplement ce Map sur une autre machine qui possède la copie du bloc dans GFS. C'est transparent pour le programmeur !

#### 3. Bigtable (2006) — La base de données sans jointures
* Les moteurs relationnels étaient trop lents. Bigtable propose une immense table creuse, distribuée et triée, capable de monter à des milliards de lignes et des millions de colonnes, indexée par une clé unique, une famille de colonnes et un horodatage (*timestamp*). C'est le père spirituel de toutes les bases NoSQL orientées colonnes comme Apache Cassandra et Apache HBase.

---

### Segment 4 : L'épopée Open-Source — D'Apache Hadoop à Apache Spark

Ces papiers ont inspiré des ingénieurs comme Doug Cutting, qui ont recréé ces concepts en open-source sous le nom d'**Apache Hadoop** (avec HDFS pour le stockage et Hadoop MapReduce pour le calcul).

Pendant plusieurs années, Hadoop a été le roi incontesté. Mais il avait une faille architecturale majeure : **l'obsession du disque**.

#### L'analogie de la cuisine : Pourquoi MapReduce était trop lent
Imaginez un grand chef cuisinier préparant une recette en 10 étapes.
- **Avec Hadoop MapReduce :** Après avoir épluché les oignons (Map 1), le chef met ses oignons dans un sac plastique, descend à la cave, les met au congélateur (écriture sur HDFS), remonte à la cuisine, redescend à la cave, sort les oignons du congélateur (lecture disque HDFS), les fait revenir à la poêle (Reduce 1), puis remet le tout dans un sac, redescend au congélateur... Entre chaque étape, tout est écrit et relu sur disque dur ! Pour des algorithmes itératifs comme le Machine Learning, 90% du temps était gaspillé en entrées/sorties disques.
- **Avec Apache Spark (né à UC Berkeley en 2010) :** Le chef garde tous ses ingrédients préparés sur le plan de travail dans des bols en inox juste devant lui (**calcul en mémoire vive - In-Memory RAM**). Il ne descend au congélateur que lorsqu'il a fini le plat complet !
- **Et si un bol tombe par terre ?** C'est la beauté des **RDD** (*Resilient Distributed Datasets*) de Spark : Spark ne sauvegarde pas les données intermédiaires sur disque, mais il mémorise la **recette exacte** (le graphe acyclique dirigé ou DAG). Si une partition en mémoire est perdue, Spark la recalcule à la volée en quelques secondes sans recommencer tout le traitement. Résultat : Spark est 10 à 100 fois plus rapide que MapReduce !

---

### Segment 5 : Pourquoi le format de fichier peut diviser vos coûts par 10

Un autre point crucial abordé dans le cours concerne les formats de stockage :
Pourquoi les professionnels du Big Data refusent-ils catégoriquement les fichiers CSV ou JSON en production ?

Prenez une table contenant 1 milliard d'achats avec 50 colonnes (nom, adresse, carte de crédit, produit, prix, etc.).
Vous voulez exécuter une requête simple : `SELECT AVG(prix) FROM achats`.
* **En CSV / JSON (Format orienté lignes) :** Le système est obligé de scanner chaque ligne d'un bout à l'autre depuis le disque dur, lisant les 49 colonnes inutiles, parsant les chaînes de caractères, saturant le bus d'E/S pour ne récupérer qu'un pauvre chiffre flottant à la fin de chaque ligne.
* **En Parquet ou ORC (Format orienté colonnes) :**
  1. **Projection Pushdown :** Le moteur ne charge physiquement sur disque que la colonne `prix`. Les 49 autres colonnes ne sont même pas effleurées.
  2. **Predicate Pushdown :** Les fichiers colonnaires contiennent des métadonnées indiquant le minimum et le maximum de chaque bloc. Si votre requête demande `WHERE prix > 1000`, et qu'un bloc a pour max `850`, le moteur saute le bloc entier sans le lire !
  3. **Compression chirurgicale :** Comme tous les éléments d'une colonne partagent le même type de données, les algorithmes de compression (Snappy, ZSTD) réalisent des miracles avec un taux de compression de plus de 80%.

---

### Segment 6 : L'étude de cas Smart City — L'orchestre complet

Le cours illustre la puissance de cet écosystème avec l'exemple d'une métropole intelligente :
1. Des milliers de capteurs de trafic, feux, radars et GPS de citoyens transmettent des téraoctets de données chaque heure.
2. Le **Streaming (Spark Streaming)** surveille le flux instantané pour détecter les collisions ou embouteillages en 3 secondes et alerter les urgences.
3. Le **Stockage froid (HDFS / Data Lake)** accumule toutes ces données historiques sur plusieurs années.
4. Le **Batch et SQL (Spark SQL)** permettent aux analystes d'étudier l'évolution de la congestion sur une décennie.
5. Le **Machine Learning (Spark MLlib)** prédit la demande en transport en commun pour adapter la fréquence des bus demain matin.
6. Le **Graphe (Spark GraphX)** recalcule le trajet le plus rapide pour des centaines de milliers de véhicules en temps réel.

---

### Conclusion & Questions à emporter pour la discussion
Pour résumer ce premier cours :
1. Le Big Data n'est pas une question de gigaoctets arbitraires, mais d'inadéquation entre un problème de calcul et les capacités d'un système monolithique.
2. Le génie de Google a été de prouver qu'on pouvait créer un supercalculateur plus fiable et infiniment moins cher en combinant des milliers de machines banalisées avec une architecture logicielle résiliente (GFS, MapReduce, Bigtable).
3. Apache Spark a perfectionné ce modèle en passant du disque à la mémoire vive, devenant la colonne vertébrale du Machine Learning distribué moderne.

*Êtes-vous prêts à plonger sous le capot lors du prochain épisode pour disséquer les mécanismes profonds des clusters et la physique des données distribuées ?*
