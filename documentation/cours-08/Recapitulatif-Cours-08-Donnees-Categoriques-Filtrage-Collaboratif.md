# MTI850 -- Analytique des donnees massives
## Synthese et Recapitulatif du Cours 08 : Donnees Categoriques, Encodage Creux et Filtrage Collaboratif (ALS)

Enseignant : Prof. Alessandro L. Koerich  
Departement : Genie logiciel et des TI, Ecole de technologie superieure (ETS)  
Session : Automne 2026 (Seance 8 / 7e seance magistrale)  

---

## 1. Traitement des Donnees Categoriques a Grande Echelle

Dans les applications reelles de science des donnees (tarification Airbnb, publicite en ligne, commerce electronique), la majorite des caracteristiques ne sont pas numeriques mais categorielles (types de logements, quartiers, identifiants d'utilisateurs).

### 1.1 One-Hot Encoding (OHE) et Piege de la Variable Muette
Pour transformer une variable qualitative comptant C categories en nombres exploitables par un algorithme lineaire :
- Chaque modalite devient un vecteur binaire de dimension C ne contenant qu'un seul 1 et C - 1 zeros.
- Le piege de la variable muette (Dummy Variable Trap) :
  Si l'on conserve les C colonnes binaires tout en incluant une colonne de biais (intercept x_0 = 1), la somme de ces colonnes est exactement egale au vecteur de biais :
  sum_{c=1}^C x_c = 1
  Il y a collinearite parfaite, la matrice X^T X devient singuliere et non inversible. Pour y remedier, on omet soit l'une des modalites (C - 1 colonnes), soit on applique une regularisation Ridge qui leve l'indetermination.

### 1.2 Representation Creuse (Sparse Vectors) dans Spark
Quand une variable possede des milliers de modalites (ex. 10 000 codes postaux), un vecteur plein (DenseVector) stockerait 9 999 zeros inutiles en memoire pour chaque ligne de donnees.
Spark MLlib utilise des vecteurs creux (SparseVector) sous la forme d'un triplet :
`SparseVector(size, indices, values)`
Exemple : Un vecteur de taille 10 000 ayant la valeur 1 a l'indice 42 s'encode par :
`SparseVector(10000, [42], [1.0])`
Gain memoire : L'empreinte passe de 80 Ko par observation a seulement 16 octets !

### 1.3 Le Pipeline de Transformation Categorieelle dans PySpark
Le carnet pratique `AirBnBRentalPrices2` illustre l'enchainement standard des transformateurs Spark MLlib :
1. `StringIndexer` : Assigne a chaque chaine de caracteres un index numerique flottant [0, C - 1], ordonne par frequence d'apparition decroissante.
2. `OneHotEncoder` : Convertit les index numeriques en vecteurs creux binaires.
3. `VectorAssembler` : Concatene les variables numeriques continues et les vecteurs creux dans un unique vecteur de caracteristiques final.

```python
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler

# Indexation des chaines
indexer = StringIndexer(inputCol="room_type", outputCol="room_type_idx", handleInvalid="keep")

# Encodage One-Hot
encoder = OneHotEncoder(inputCol="room_type_idx", outputCol="room_type_vec")

# Assemblage vectoriel
assembler = VectorAssembler(inputCols=["bedrooms", "bathrooms", "room_type_vec"], outputCol="features")
```

---

## 2. Systemes de Recommandation et Filtrage Collaboratif

L'objectif du filtrage collaboratif est de predire l'interet d'un utilisateur u pour un article i a partir des interactions historiques de l'ensemble de la communaute, sans dependre du contenu textuel des articles.

### 2.1 La Matrice Utilisateurs-Articles et la Sparsete Extreme
Soit R de taille m par n la matrice des interactions (m utilisateurs, n articles) :
- Dans un catalogue comme Netflix ou Amazon, chaque utilisateur ne note qu'une infime fraction des articles disponibles.
- Le taux de sparsete (pourcentage de cellules vides) depasse generalement 99.5 % a 99.99 %.
- L'objectif mathematique est de predire les valeurs des cellules non observees.

### 2.2 Factorisation Matricielle par Facteurs Latents
On postule que les preferences des utilisateurs et les attributs des articles peuvent etre resumes dans un espace vectoriel latent de petite dimension k (ou k vaut typiquement 10 a 100) :
- Chaque utilisateur u est represente par un vecteur latent u_u dans R^k.
- Chaque article i est represente par un vecteur latent v_i dans R^k.
- L'estimation de la note est donnee par le produit scalaire :
  r_hat_{ui} = u_u^T v_i

---

## 3. L'Algorithme ALS (Alternating Least Squares)

### 3.1 Fonction Objectif Regularisee
Soit K l'ensemble des couples (u, i) pour lesquels une note reelle r_{ui} a ete observee. On cherche les matrices U (taille m par k) et V (taille n par k) qui minimisent :
min_{U, V} sum_{(u, i) in K} (r_{ui} - u_u^T v_i)^2 + lambda * (sum_u ||u_u||_2^2 + sum_i ||v_i||_2^2)

Cette fonction objectif est globalement non convexe car elle contient des produits croises u_u^T v_i. En revanche, elle est biconvexe :
- Si la matrice des articles V est fixee, la fonction devient quadratique et strictement convexe par rapport a U.
- Si la matrice des utilisateurs U est fixee, la fonction devient quadratique et strictement convexe par rapport a V.

### 3.2 Principe d'Alternance et Parallellisme Naturel
1. Etape 1 (Mise a jour de U a V fige) :
   Pour chaque utilisateur u independamment des autres :
   u_u = (V_{I_u}^T V_{I_u} + lambda * I_k)^(-1) * V_{I_u}^T r_{u, I_u}
   ou I_u est l'ensemble des articles notes par l'utilisateur u.
   Puisque les calculs pour chaque utilisateur sont totalement independants, ils s'executent en parallele absolu sur les executeurs du cluster Spark !
2. Etape 2 (Mise a jour de V a U fige) :
   Pour chaque article i independamment des autres :
   v_i = (U_{U_i}^T U_{U_i} + lambda * I_k)^(-1) * U_{U_i}^T r_{U_i, i}
   La resolution est a nouveau totalement parallele.
3. On itere entre ces deux etapes jusqu'a convergence (generalement 10 a 20 iterations).

---

## 4. Pratique avec Spark MLlib (`ALS`)

L'algorithme est implemente dans `pyspark.ml.recommendation.ALS` :
```python
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

als = ALS(userCol="userId",
          itemCol="movieId",
          ratingCol="rating",
          rank=10,               # dimension latente k
          maxIter=15,            # nombre d'alternances
          regParam=0.1,          # parametre lambda
          coldStartStrategy="drop", # ignore les nouveaux utilisateurs en test
          seed=42)

model = als.fit(trainingDF)
predictions = model.transform(testDF)

evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
rmse = evaluator.evaluate(predictions)
```

> [!IMPORTANT]
> Le probleme du demarrage a froid (Cold Start) :
> Si un utilisateur ou un article present dans l'ensemble de test n'a jamais ete observe pendant l'entrainement, ALS ne dispose d'aucun vecteur latent pour calculer la prediction, produisant une valeur NaN. Le reglage `coldStartStrategy="drop"` supprime automatiquement ces lignes lors de l'evaluation pour eviter de fausser les metriques.
