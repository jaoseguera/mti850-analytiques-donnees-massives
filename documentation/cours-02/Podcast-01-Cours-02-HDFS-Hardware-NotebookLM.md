# Source Audio Overview (NotebookLM) — Épisode 3 : Les Secrets Physiques d'un Cluster Hadoop et d'HDFS
## Guide de Discussion & Contenu Pédagogique pour Podcast Audio
**Contexte :** Cours MTI850 (Analytique des données massives), Séance 2, ÉTS Montréal.  
**Sujet de l'épisode :** Plongée dans le métal et les architectures — Les rôles de NameNode et DataNode, le secret de la réplication en pipeline, pourquoi les experts bannissent le RAID, le dimensionnement matériel pour Spark et l'étude de cas du cluster géant de Meta (Facebook).

---

### Introduction & Accroche Narrative
Bienvenue dans ce nouvel épisode dédié à l'analytique des données massives. Dans la séance précédente, nous avons discuté des concepts généraux et des articles fondateurs de Google. Mais aujourd'hui, nous entrons dans la salle des serveurs. 

Nous allons parler de bruit de ventilateurs, de câbles réseau 25 Gigabits, de disques durs nus montés sans contrôleur RAID, et de la façon dont un cluster de 4 000 machines parvient à fonctionner comme s'il ne s'agissait que d'un seul ordinateur géant.

Comment un fichier de 500 gigaoctets est-il concrètement éclaté, répliqué et distribué sur des dizaines de serveurs sans que rien ne se perde ? Pourquoi les ingénieurs Big Data refusent-ils la technologie RAID que toutes les entreprises utilisaient depuis trente ans ? Et comment Meta ingère-t-elle des milliards de « likes » et de publications par jour ? C'est ce que nous allons explorer.

---

### Segment 1 : L'esprit et les muscles — NameNode vs DataNodes

Dans un cluster Hadoop et HDFS, il existe une distinction nette et sans ambiguïté entre l'esprit qui dirige et les muscles qui portent la charge.

D'un côté, nous avons le **NameNode**. C'est le cerveau unique. 
- Il ne stocke **aucun octet des fichiers réels**. 
- Son rôle exclusif est de maintenir l'arbre généalogique du système de fichiers en mémoire vive : les noms des répertoires, les droits d'accès, et surtout la table de correspondance qui dit : « Le fichier `rapport_financier.csv` est divisé en quatre blocs de 128 Mo, et le bloc #1 se trouve sur les machines 12, 45 et 89 ».
- Pourquoi en RAM ? Parce que si le NameNode devait faire des accès disque pour chaque consultation de fichier, l'ensemble du cluster s'arrêterait d'avancer !

De l'autre côté, nous avons les **DataNodes**. Ce sont les soldats ouvriers. 
- Chaque serveur du cluster exécute un démon DataNode. 
- Ils ne savent rien de la structure globale des fichiers. Tout ce qu'ils voient sur leurs disques durs locaux, ce sont des blocs binaires anonymes avec un identifiant comme `blk_1073741825`. 
- Toutes les quelques secondes, ils envoient un petit battement de cœur (*Heartbeat*) au NameNode pour lui dire : « Je suis toujours en vie, et voici la liste des blocs que je possède sur mes disques ».

#### Le cauchemar des petits fichiers (*The Small Files Problem*)
Voici un piège classique d'examen et d'ingénierie :
Pourquoi HDFS est-il incapable de gérer un milliard de fichiers de 10 kilo-octets ?
Parce que chaque fichier, chaque répertoire et chaque bloc de données stocké dans HDFS coûte environ 150 octets de métadonnées dans la mémoire vive du NameNode. 
Si vous stockez 100 millions de petits fichiers, vous saturez 30 Go de RAM du NameNode pour seulement quelques gigaoctets de données réelles ! HDFS a été conçu pour stocker de grands fichiers découpés en blocs de 128 Mo.

---

### Segment 2 : L'odyssée d'une écriture — La réplication en pipeline

Regardons ce qui se passe quand vous uploadez un gros fichier de 256 Mo sur HDFS avec un facteur de réplication de 3.
Le fichier est découpé en deux blocs de 128 Mo : le Bloc A et le Bloc B.

Comment le Bloc A se retrouve-t-il sur trois serveurs différents ?
Une personne non initiée pourrait penser que votre ordinateur portable envoie le bloc au serveur 1, puis renvoie le bloc au serveur 2, puis au serveur 3. Ce serait une catastrophe : votre carte réseau locale exploserait et le processus prendrait une éternité.

La véritable magie d'HDFS s'appelle la **réplication en pipeline** (*Pipelined Replication*) :
1. Le client demande au NameNode : « J'ai un bloc à écrire, où dois-je le mettre ? »
2. Le NameNode choisit trois DataNodes (disons DN1, DN2 et DN3) en respectant la conscience des racks (*Rack Awareness*) : DN1 est dans votre rack, DN2 et DN3 sont dans un autre rack pour résister à une coupure électrique de baie.
3. Le client découpe le bloc en petits paquets de 64 kilo-octets et commence à les envoyer au **DataNode 1**.
4. Dès que le DataNode 1 reçoit le premier paquet de 64 Ko, il l'écrit sur son disque local et le transmet **immédiatement sur le réseau interne au DataNode 2**, qui fait de même vers le **DataNode 3** !
5. C'est une chaîne de relais continue : les octets coulent comme de l'eau dans un tuyau à travers les trois serveurs en parallèle. Une fois que le dernier serveur a écrit son paquet, les accusés de réception remontent la chaîne jusqu'au client. C'est un chef-d'œuvre d'optimisation de bande passante !

---

### Segment 3 : L'hérésie du matériel — Pourquoi pas de RAID ?

Voici l'une des surprises les plus frappantes pour les administrateurs de serveurs classiques :
Dans le monde des bases de données traditionnelles (Oracle, SQL Server), la règle absolue était d'acheter des cartes contrôleur RAID matérielles coûteuses (RAID 5, RAID 6 ou RAID 10) pour protéger les disques contre les pannes physiques.

Dans un cluster Hadoop, **le RAID est formellement banni !**
On installe 4 à 8 disques durs bruts par serveur, montés chacun comme un point de montage indépendant (`/data1`, `/data2`, `/data3`...). C'est ce qu'on appelle l'architecture **JBOD** (*Just a Bunch of Disks*).

Pourquoi ?
1. **La redondance existe déjà au niveau logiciel :** HDFS réplique déjà chaque bloc 3 fois sur 3 serveurs différents. Si un disque dur grille sur le serveur 12, ce n'est pas grave : les deux autres copies sont déjà en sécurité sur les serveurs 45 et 89. Le NameNode ordonne simplement à une de ces copies de se dupliquer ailleurs.
2. **La pénalité de reconstruction du RAID :** Si un disque lâche dans une grappe RAID 5, le contrôleur matériel surcharge le processeur pendant des heures pour recalculer la parité, ralentissant considérablement toutes les applications.
3. **Le parallélisme pur des disques :** Avec JBOD, Spark et Hadoop peuvent lire et écrire sur 8 disques durs simultanément via 8 canaux de communication indépendants sans aucun goulet d'étranglement de contrôleur central.

---

### Segment 4 : Comment dimensionner un serveur Spark sans faire exploser la JVM ?

Quand on dimensionne un serveur pour un cluster Spark et Hadoop, le professeur Koerich nous donne des règles d'or capitales :

#### 1. La règle des 75% de mémoire
Si votre serveur dispose de 128 Go de RAM, combien devez-vous en attribuer à Spark et YARN ?
La tentation est de mettre 120 Go. C'est une erreur fatale !
La règle d'or est d'allouer au **maximum 75%** (soit environ 96 Go). 
Pourquoi ? Parce que le système d'exploitation Linux, les démons d'arrière-plan (DataNode, NodeManager) et surtout le **cache de pages du noyau Linux** (*OS Page Cache*) ont besoin d'oxygène. Si vous affamez Linux, le noyau déclenche le redoutable *OOM Killer* (Out Of Memory Killer) qui assassine brutalement vos processus Spark au milieu de la nuit !

#### 2. Le piège des monstres de RAM (> 200 Go)
Si vous achetez un serveur ultra-moderne avec 512 Go de mémoire vive, faut-il lancer un seul gros travailleur Spark JVM de 400 Go ?
**Absolument pas !** La machine virtuelle Java (JVM) n'a jamais été conçue pour gérer des monceaux de mémoire de 400 Go avec des pointeurs d'objets. Lorsque le ramasse-miettes (*Garbage Collector*) décide de nettoyer la mémoire, il peut figer le serveur pendant 10 minutes d'affilée !
La solution : découper ce gros serveur physique en plusieurs travailleurs virtuels plus petits (par exemple 4 exécuteurs de 64 Go chacun).

#### 3. Le réseau 10 GbE / 25 GbE
Quand vos données sont chargées en mémoire vive, votre cluster ne lit plus sur disque. Quelle est la ressource qui devient le goulot d'étranglement immédiat ? **Le réseau !** 
Chaque fois que vous faites un `reduceByKey` ou une jointure SQL (`JOIN`), des gigaoctets de données doivent traverser le switch réseau entre les serveurs (l'opération de *Shuffle*). Un réseau classique à 1 GbE étranglerait immédiatement le cluster. Le 10 GbE ou 25 GbE est le standard minimal absolu.

---

### Segment 5 : Dans les coulisses de Meta (Facebook) — Du log au Machine Learning

Pour relier toute cette théorie à la pratique industrielle, le cours examine l'architecture de Meta :
- Plus de 3 milliards d'utilisateurs actifs.
- Des clusters de plus de 4 000 serveurs Hadoop stockant des centaines de pétaoctets de données.
- Chaque publication, like, vidéo ou commentaire sur Facebook ou Instagram génère une ligne de log texte brut sur un serveur web frontal.
- Des serveurs d'agrégation appelés **Scribe** collectent ces flux en continu et les écrivent en blocs géants sur HDFS.

#### Comment passer de ce chaos textuel à un modèle d'IA prédictif ?
C'est précisément l'exercice pratique que nous réalisons dans ce cours :
1. **Lecture brute sur HDFS :** On charge les logs sous forme de lignes de texte brut dans un DataFrame Spark (`spark.read.text`).
2. **Nettoyage et structuration :** On extrait les champs grâce à des expressions régulières pour imposer un schéma strict : `utilisateur`, `horodatage`, `action`, `contenu`.
3. **Le pont vers les maths :** Les algorithmes de Machine Learning ne comprennent pas le texte ni les identifiants d'utilisateurs. Ils n'acceptent que des matrices et des vecteurs numériques !
   - Pour les nombres : on utilise un `VectorAssembler` qui assemble plusieurs colonnes en un `DenseVector`.
   - Pour les catégories et les mots : on utilise l'encodage disjonctif (*One-Hot Encoding*). Mais attention : si vous avez 100 000 mots différents, votre vecteur aura 100 000 colonnes remplies de zéros ! C'est là qu'intervient le `SparseVector`, qui ne stocke que les indices des valeurs non nulles, sauvant ainsi 99% de votre mémoire RAM.
4. **L'entraînement et le streaming :** On assemble ces étapes dans un `Pipeline` Spark ML (`Estimator.fit`), puis on applique le modèle entraîné aussi bien sur les données historiques du passé que sur le flux d'événements en direct (*Structured Streaming*) !

---

### Conclusion & Synthèse
Pour résumer ce deuxième grand cours :
1. HDFS est bâti sur une séparation parfaite des pouvoirs : le NameNode gère la carte mémoire des blocs, et les DataNodes gèrent les octets physiques sur disque.
2. La résilience matérielle n'est plus confiée à des contrôleurs RAID fragiles et coûteux, mais à une réplication logicielle distribuée intelligente en pipeline avec conscience de rack.
3. Dimensionner un cluster exige de respecter les lois physiques de la mémoire vive (règle des 75%), des limites du ramasse-miettes Java et de la bande passante réseau lors du Shuffle.
4. Enfin, toute la chaîne analytique moderne, des logs géants de Meta jusqu'aux modèles de Machine Learning prédictifs, repose sur cette capacité à transformer de la donnée brute non structurée en vecteurs distribués hautement optimisés.
