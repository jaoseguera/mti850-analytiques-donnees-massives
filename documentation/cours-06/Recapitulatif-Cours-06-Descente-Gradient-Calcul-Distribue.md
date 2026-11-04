# MTI850 -- Analytique des donnees massives
## Synthese et Recapitulatif du Cours 06 : Descente de Gradient, Apprentissage Extensible et Hierarchie de Communication

Enseignant : Prof. Alessandro L. Koerich  
Departement : Genie logiciel et des TI, Ecole de technologie superieure (ETS)  
Session : Automne 2026  

---

## 1. Contexte et Motivation : Pourquoi Abandonner la Forme Fermee ?

Au cours precedent, nous avons etabli la premiere regle d'or de l'apprentissage automatique a grande echelle :
> Le calcul et le stockage doivent demeurer strictement lineaires en nombre d'observations (n) et en nombre de dimensions (d) : O(n d).

La solution analytique exacte des moindres carres, w = (X^T X)^(-1) X^T y, viole cette regle des que d devient grand :
- Calcul : O(n d^2 + d^3). Pour d = 10^6, d^3 represente 10^18 operations arithmetiques.
- Stockage : O(n d + d^2). Stocker X^T X demande 10^12 flottants, soit environ 8 Teraoctets de memoire vive pour une seule matrice intermediaire.

Pour resoudre des problemes a tres haute dimension (traitement du langage naturel, genomique, publicite en ligne), nous devons abandonner la resolution exacte matricielle au profit de methodes d'optimisation iteratives de premier ordre : la descente de gradient.

---

## 2. Fondements Mathematiques de la Descente de Gradient

Soit une fonction de perte convexe et differentiable f(w). L'algorithme de descente de gradient met a jour le vecteur de parametres w a chaque iteration t selon la direction opposee au gradient :
w^(t+1) = w^(t) - alpha_t * nabla f(w^(t))
ou alpha_t > 0 designe le taux d'apprentissage (learning rate / pas d'iteration).

### 2.1 Gradient de la Perte Quadratique
Pour la regression lineaire aux moindres carres :
f(w) = (1 / n) * sum_{i=1}^n (w^T x^(i) - y^(i))^2
Le gradient global s'ecrit comme la moyenne des gradients individuels :
nabla f(w) = (2 / n) * sum_{i=1}^n (w^T x^(i) - y^(i)) * x^(i)

### 2.2 Variantes de la Descente de Gradient
1. Descente de Gradient par Lot (Batch Gradient Descent - BGD) :
   - Calcule le gradient exact sur la totalite des n observations a chaque iteration.
   - Convergence stable et trajectoire lisse.
   - Cout par iteration : O(n d), prohibitif si n vaut plusieurs milliards.
2. Descente de Gradient Stochastique (Stochastic Gradient Descent - SGD) :
   - Estime le gradient a partir d'un seul echantillon tire au hasard : g_i(w) = 2 * (w^T x^(i) - y^(i)) * x^(i).
   - E[g_i(w)] = nabla f(w) (estimateur sans biais).
   - Cout par iteration : O(d), ultra-rapide mais trajectoire tres bruitee qui oscille autour du minimum.
3. Descente de Gradient par Mini-Lots (Mini-Batch SGD) :
   - Compromis standard dans l'industrie : calcule le gradient sur un sous-ensemble b echantillons (typiquement b = 64 a 8192).
   - Permet la vectorisation materielle (SIMD / GPU / TPU) tout en reduisant la variance du gradient.

---

## 3. Hierarchie de Communication et Goulots d'Etranglement

Dans un cluster de calcul distribue, le goulot d'etranglement n'est presque jamais le temps de calcul CPU, mais le transfert de donnees a travers la hierarchie memoire et reseau.

### 3.1 Ordres de Grandeur de Latence et Bande Passante
Les ecarts de performance sont monumentaux entre les couches de la machine et du reseau :
- Registres CPU et cache L1 : ~0.5 a 1 nanoseconde, bande passante > 1000 Go/s.
- Memoire vive locale (RAM DDR4/DDR5) : ~50 a 100 nanosecondes, bande passante ~100 a 400 Go/s.
- Disque NVMe local : ~10 a 100 microsecondes, bande passante ~3 a 7 Go/s.
- Reseau intra-datacenter (10 Gbps / 100 Gbps) : ~0.5 a 2 millisecondes, bande passante ~1 a 10 Go/s.
- Lecture distante sur stockage cloud (S3 / ADLS / GCS) : ~50 a 200 millisecondes de latence initiale.

Lire une donnee sur le reseau est environ 1 000 000 de fois plus lent que d'y acceder dans le cache du processeur.

### 3.2 Principes de Communication pour le ML Distribue
Puisque les algorithmes d'apprentissage sont iteratifs (10 a 1000 passages sur les donnees), relire les donnees depuis le disque ou repartitionner le reseau a chaque iteration detruit la performance :
1. Mise en cache memoire : Les DataFrames / RDDs d'apprentissage doivent etre persistes en RAM des executeurs (`cache()` ou `persist(StorageLevel.MEMORY_AND_DISK)`).
2. Agregations arborescentes (Tree-Aggregate) : Au lieu de faire converger tous les gradients partiels vers un seul Driver (ce qui sature sa carte reseau), Spark MLlib utilise une reduction en arbre (Tree-Reduce) en O(log k) etapes, ou k est le nombre de partitions.

---

## 4. Le Changement de Paradigme : Du Cluster HDFS au Lakehouse

Historiquement, l'ecosysteme Hadoop/HDFS reposait sur la localite absolue des donnees : le code de calcul etait envoye sur le serveur physique qui possedait les blocs de disque locaux (DataNode + TaskTracker co-localises).

### 4.1 Le Deouplage Calcul et Stockage
Dans les architectures modernes (AWS EMR, Databricks, Google Dataproc, Azure Synapse) :
- Le stockage est externalise sur du stockage objet bon marche et hautement disponible (Amazon S3, Azure Data Lake Storage, Google Cloud Storage).
- Les grappes de calcul Spark sont ephemeres et dimensionnees dynamiquement.
- La localite des donnees au sens classique n'existe plus au niveau machine.

### 4.2 Les Trois Regles d'Or du Lakehouse ML
Pour maintenir une efficacite maximale en architecture deouplee :
1. Lire Moins (Read Less) :
   - Stocker en format colonnaire compresse (Parquet / Delta Lake).
   - Exploiter le predicate pushdown (elagage des partitions et statistiques min/max des colonnes) pour ne charger que les lignes et variables strictement requises par le modele.
2. Deplacer Moins (Move Less) :
   - Minimiser le Shuffle reseau entre executeurs.
   - Diffuser les petits tableaux ou hyperparametres via broadcast (`sc.broadcast()`).
3. Reutiliser Plus (Reuse More) :
   - Mettre en cache les donnees preprocesses (Delta Cache, persistance RDD).
   - Utiliser des magasins de caracteristiques (Feature Stores) pour eviter de recalculer les memes transformations couteuses.
