# Source Audio Overview (NotebookLM) — Épisode 6 : Les Arcanes de Spark — Catalyst, Tungsten, AQE et la Révolution Arrow
## Guide de Discussion Approfondie & Secrets d'Architecture pour Podcast Audio
**Contexte :** Cours MTI850 (Analytique des données massives), Niveau Maîtrise, ÉTS Montréal.  
**Sujet de l'épisode :** Au cœur des entrailles de Spark — Le compilateur Catalyst décortiqué, le génie matériel du Projet Tungsten, l'intelligence en temps réel d'AQE, et pourquoi Apache Arrow a sauvé Python dans le Big Data.

---

### Introduction & Accroche Narrative
Bienvenue dans ce deuxième volet technique consacré à Apache Spark. Dans l'épisode précédent, nous avons découvert la surface : l'architecture Driver/Executors, le concept élégant de l'évaluation paresseuse et l'utilisation des DataFrames.

Mais aujourd'hui, nous branchons l'oscilloscope sur le moteur. Nous allons répondre aux questions que seuls les concepteurs de systèmes et les ingénieurs de haut niveau se posent :
- Que fait réellement l'optimiseur Catalyst lorsqu'il reçoit votre requête SQL ou votre ligne de code DataFrame ?
- Pourquoi les créateurs de Spark ont-ils dû contourner la machine virtuelle Java pour réinventer la gestion de mémoire brute avec le Projet Tungsten ?
- Comment Spark 3 et Spark 4 parviennent-ils à modifier leur plan d'attaque militaire en plein vol grâce à l'Adaptive Query Execution ?
- Et pourquoi la vieille façon d'écrire des UDFs en Python était une hérésie de performance, jusqu'à ce qu'Apache Arrow vienne révolutionner l'écosystème ?

Prenez un café serré, nous plongeons dans les rouages intimes du calcul distribué de pointe.

---

### Segment 1 : Dans la tête du Grand Maître — Les 4 Vies d'une Requête Catalyst

Quand vous écrivez en Python :
```python
df.filter(df.pays == "Canada").select("nom", "salaire")
```
Vous pensez que Spark va exécuter un filtre puis une sélection. Mais pour Spark, votre code n'est qu'un souhait déclaratif. Il confie ce souhait à son cerveau : **l'optimiseur Catalyst**.

Catalyst va faire traverser à votre code quatre métamorphoses successives :

1. **Le Plan Logique Non Résolu :** Catalyst prend votre code et construit un arbre syntaxique. Mais à ce stade, il ignore si la colonne `salaire` existe vraiment ou si vous avez fait une faute de frappe.
2. **Le Plan Logique Résolu :** Catalyst va consulter le Catalogue (les métadonnées de vos tables). Il valide que la colonne `salaire` existe, que c'est bien un nombre flottant, et que la table est accessible.
3. **Le Plan Logique Optimisé (RBO - Rule-Based Optimization) :** C'est ici que commence le génie mathématique. Catalyst applique des dizaines de règles d'algèbre relationnelle :
   - **Predicate Pushdown (Poussée de prédicats) :** Si vous filtrez sur `pays == "Canada"`, Catalyst descend ce filtre tout en bas de l'arbre, au plus près des fichiers Parquet sur disque. Les lignes non canadiennes ne sont même pas lues !
   - **Projection Pruning (Élision de colonnes) :** Si votre table compte 80 colonnes, Catalyst efface les 78 colonnes inutiles dès la lecture.
   - **Constant Folding :** Si votre code contient `24 * 60 * 60`, Catalyst remplace l'opération par `86400` avant même de lancer le premier processeur.
4. **La Planification Physique et le Modèle de Coût (CBO) :** Catalyst génère plusieurs stratégies d'exécution physique concrètes et calcule leur coût prévisionnel.
   - Si une table fait moins de 10 mégaoctets, il choisit un **Broadcast Hash Join** : il envoie la petite table en mémoire à tout le monde et élimine 100% du trafic réseau de la grande table !
   - Si les deux tables font des téraoctets, il bascule sur un **Sort-Merge Join** avec tri sur disque pour éviter tout crash mémoire.

---

### Segment 2 : La Trahison de la JVM et le Sauvetage par le Projet Tungsten

Voici l'une des histoires d'ingénierie les plus fascinantes des dix dernières années.
Au milieu des années 2010, les ingénieurs de Databricks constatent un paradoxe déroutant :
Les disques NVMe deviennent ultra-rapides, les réseaux passent à 10 et 40 Gigabits... et pourtant, Spark ralentit !
Le nouveau goulot d'étranglement n'est plus le réseau, ni le disque : **c'est le processeur et la mémoire de la machine virtuelle Java (JVM) !**

#### Pourquoi Java gaspille-t-il autant de mémoire ?
En Java classique, un simple entier de 4 octets encapsulé dans un objet `Integer` consomme 16 ou 24 octets en mémoire à cause des en-têtes d'objet et de l'alignement sur 64 bits.
Pour une chaîne de 3 lettres comme `"ÉTS"`, Java consomme près de 48 octets !
Sur un cluster de 10 téraoctets, 7 téraoctets étaient gaspillés uniquement en métadonnées d'objets Java !
Et pire encore : quand le ramasse-miettes (*Garbage Collector*) devait nettoyer cette montagne d'objets, il gelait le cluster pendant plusieurs minutes d'affilée en pauses *Stop-The-World*.

#### La riposte : Le Projet Tungsten
Tungsten a dit : « Assez des objets Java ! »
1. **La mémoire hors-heap (*Off-Heap Memory*) :** Tungsten contourne complètement le ramasse-miettes. Il alloue de la mémoire binaire brute continue directement via des instructions bas-niveau (`sun.misc.Unsafe`), exactement comme on le ferait en langage C. Zéro objet Java, zéro pause de ramasse-miettes !
2. **La Sympathie Mécanique (*Mechanical Sympathy*) :** En alignant les octets de manière contiguë, les lignes de données entrent parfaitement dans les mémoires caches L1 et L2 du processeur. Plus de temps perdu à attendre que la RAM réponde !
3. **Whole-Stage Code Generation :** Au lieu d'appeler des fonctions virtuelles à chaque ligne pour chaque colonne, Tungsten utilise le compilateur **Janino** pour générer dynamiquement à la volée une seule boucle `for` en bytecode natif. Spark s'exécute avec la rapidité du C++ pur tout en restant dans l'écosystème Java !

---

### Segment 3 : L'Intelligence en Plein Vol — Adaptive Query Execution (AQE)

Pendant longtemps, le grand défaut de Spark était que le plan physique choisi par Catalyst était gravé dans le marbre avant le début du job.
Si les statistiques étaient fausses, Spark pouvait décider de lancer 5 000 tâches pour traiter 3 mégaoctets de données, ou choisir une mauvaise jointure et planter le cluster.

Avec **AQE** (introduit dans Spark 3 et perfectionné dans Spark 4), Spark acquiert un système de pilotage adaptatif en temps réel :
À chaque frontière d'étape de calcul (*Stage Boundary*, quand les données sont mélangées lors d'un Shuffle), **Spark s'arrête une fraction de seconde et regarde les données réelles qui viennent d'être produites**.

Et là, il peut prendre trois décisions magistrales :
1. **Coalescing Shuffle Partitions (Fusion des petites partitions) :** Si vous aviez configuré 200 partitions de shuffle, mais qu'après filtrage il ne reste que 1 mégaoctet de données, Spark fusionne automatiquement ces partitions pour n'exécuter que 2 tâches utiles au lieu de 200 tâches ridicules.
2. **Basculement dynamique de jointure :** Si une immense table d'un téraoctet ne contient en fait que 5 mégaoctets après l'application de vos filtres, Spark annule le lourd *Sort-Merge Join* prévu et le remplace immédiatement par un *Broadcast Hash Join* ultra-rapide !
3. **Le traitement des données biaisées (*Skew Joins*) :** Si 95% de vos données portent la même clé (par exemple un champ `null` ou le client le plus populaire), un pauvre exécuteur se retrouve à faire 10 heures de travail pendant que les autres attendent sans rien faire. AQE repère automatiquement cette partition géante et la découpe en 20 sous-morceaux traités en parallèle par le reste du cluster !

---

### Segment 4 : Le Miracle d'Apache Arrow — Pourquoi Python a enfin triomphé

Si vous avez utilisé PySpark il y a quelques années, vous vous souvenez peut-être de la déception :
Dès qu'on écrivait une fonction personnalisée en Python avec `@udf`, le script devenait d'une lenteur exaspérante.

Pourquoi ? Parce que chaque ligne de données dans la JVM devait être convertie en binaire avec l'outil `pickle` de Python, envoyée par un socket réseau interne à un interpréteur Python, calculée une ligne à la fois, puis ré-emballée et renvoyée à la JVM. Ce manège de sérialisation consommait jusqu'à 80% du temps total de traitement !

#### La révolution : Pandas UDFs et Apache Arrow
C'est ici qu'intervient **Apache Arrow**.
Arrow a défini un standard international de représentation colonnaire en mémoire vive.
Désormais, avec les **Pandas UDFs** :
- Spark regroupe des milliers de lignes sous forme de bloc de colonnes Arrow directement dans la mémoire vive partagée (*Shared Memory*).
- Le processus Python lit directement cette même adresse de mémoire sans copier un seul octet (**Zero-Copy**).
- Mieux encore : Python exécute vos calculs avec les fonctions C vectorisées de Pandas, NumPy ou PyTorch en utilisant les instructions SIMD du microprocesseur (calcul parallèle sur les registres CPU).
- Le résultat : **des accélérations de 10 à 100 fois**, rendant PySpark aussi rapide et performant que le code Scala natif !

---

### Segment 5 : La Maîtrise du Cache — Ne jouez pas à la roulette russe avec la RAM

Terminons par une question cruciale de gouvernance mémoire : comment bien utiliser le cache ?

Tout le monde connaît l'instruction `.cache()`. Mais saviez-vous que `.cache()` n'est qu'un alias pour `.persist(StorageLevel.MEMORY_AND_DISK)` ?
En production de haut niveau, un ingénieur choisit précisément son niveau de persistance :

- **`MEMORY_ONLY` :** C'est le niveau le plus rapide, mais les objets sont stockés désérialisés. Ils prennent une place gigantesque en RAM. Si la mémoire manque, Spark jette les partitions en trop et devra les recalculer plus tard.
- **`MEMORY_ONLY_SER` :** Les données sont sérialisées sous forme de tableaux d'octets compacts. Vous économisez 60% à 70% de mémoire vive, au prix d'un minuscule effort de processeur pour décompresser les octets lors de la lecture. C'est souvent le choix roi en environnement contraint !
- **`MEMORY_AND_DISK` :** Si la RAM déborde, les partitions restantes sont écrites proprement sur le disque local de l'exécuteur.

#### Le tueur silencieux : Oublier le `.unpersist()`
Le cache de Spark utilise une politique d'éviction au plus ancien (LRU). Mais si vous gardez 50 DataFrames temporaires en cache au fil d'un long pipeline, vous privez Spark de la mémoire vive nécessaire pour exécuter ses opérations de Shuffle.
**Règle d'or absolue :** Dès qu'un bloc de traitement itératif est terminé, libérez immédiatement la mémoire avec `df.unpersist()` !

---

### Conclusion & Synthèse
Pour clore cette plongée sous le capot d'Apache Spark :
1. Catalyst ne se contente pas d'exécuter votre code : il le démonte en arbre syntaxique, le simplifie par l'algèbre relationnelle (RBO), et sélectionne la meilleure stratégie physique selon les coûts (CBO).
2. Le Projet Tungsten a libéré Spark des chaînes de la JVM en gérant la mémoire binaire hors-heap et en compilant du bytecode sur-mesure au vol avec Janino.
3. Adaptive Query Execution transforme Spark en un système vivant capable de réajuster ses partitions et ses algorithmes de jointure au vu des données réelles.
4. Enfin, Apache Arrow a fait sauter le mur entre Java et Python, permettant au Data Science moderne et au Deep Learning distribué de tourner à pleine vitesse matérielle.
