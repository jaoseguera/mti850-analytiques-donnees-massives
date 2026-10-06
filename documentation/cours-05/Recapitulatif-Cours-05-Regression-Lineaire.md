# MTI850 — Analytique des données massives
## Synthèse & Récapitulatif du Cours 05 : Régression Linéaire, Apprentissage Distribué et Pipelines Spark MLlib

**Enseignant :** Prof. Alessandro L. Koerich  
**Département :** Génie logiciel et des TI, École de technologie supérieure (ÉTS)  
**Session :** Automne 2026  

---

## 1. Fondements Mathématiques de la Régression Linéaire

L'objectif de la régression est d'apprendre une fonction prédictive reliant des observations (vecteur de caractéristiques / *features* $\mathbf{x} \in \mathbb{R}^d$) à une étiquette continue (*label* $y \in \mathbb{R}$) à partir d'un ensemble d'apprentissage étiqueté (apprentissage supervisé).

### 1.1 Le Modèle Linéaire et l'Augmentation du Biais
Pour chaque observation $\mathbf{x} = [x_1, x_2, \dots, x_d]^T$, on postule une relation linéaire :
$$y \approx w_0 + w_1 x_1 + w_2 x_2 + \dots + w_d x_d$$
Pour vectoriser élégamment le terme de biais (ordonnée à l'origine / *offset* $w_0$), on augmente le vecteur $\mathbf{x}$ d'une composante constante unitaire $x_0 = 1$ :
$$\mathbf{x}_{\text{aug}} = [1, x_1, x_2, \dots, x_d]^T, \quad \mathbf{w} = [w_0, w_1, \dots, w_d]^T \implies y' = \mathbf{w}^T \mathbf{x}$$

**Pourquoi privilégier un modèle linéaire ?**
- **Simplicité conceptuelle & Efficacité calculatoire :** Très rapide à entraîner et à déployer à grande échelle.
- **Interprétabilité directe :** Le poids $w_j$ quantifie directement l'impact marginal de la caractéristique $x_j$ sur la prédiction.
- **Modélisation non-linéaire possible :** On peut modéliser des courbes en injectant des transformations non linéaires dans le vecteur de caractéristiques (ex. polynômes, interactions).

---

## 2. Optimisation par les Moindres Carrés et Solution Analytique Fermée

Pour $n$ observations d'apprentissage, on organise les données dans une matrice de conception (*design matrix*) $\mathbf{X} \in \mathbb{R}^{n \times d}$ et un vecteur d'étiquettes réelles $\mathbf{y} \in \mathbb{R}^n$.

### 2.1 Fonction de Perte Quadratique (Residual Sum of Squares - RSS)
On cherche le vecteur de poids $\mathbf{w}$ qui minimise la somme des carrés des résidus :
$$\min_{\mathbf{w}} f(\mathbf{w}) = \|\mathbf{X}\mathbf{w} - \mathbf{y}\|_2^2 = \sum_{i=1}^n (\mathbf{w}^T \mathbf{x}^{(i)} - y^{(i)})^2$$

### 2.2 Dérivation de la Solution Analytique (*Closed-Form Solution*)
En annulant le gradient de la fonction de perte :
$$\nabla_{\mathbf{w}} f(\mathbf{w}) = 2 \mathbf{X}^T (\mathbf{X}\mathbf{w} - \mathbf{y}) = 0 \implies \mathbf{X}^T \mathbf{X}\mathbf{w} = \mathbf{X}^T \mathbf{y}$$
Si la matrice $\mathbf{X}^T \mathbf{X}$ est inversible, on obtient les équations normales :
$$\mathbf{w}^* = (\mathbf{X}^T \mathbf{X})^{-1} \mathbf{X}^T \mathbf{y}$$

---

## 3. Généralisation, Surapprentissage et Régularisation Ridge

Le modèle aux moindres carrés purs minimise l'erreur sur les données d'entraînement. Cependant, un modèle trop complexe s'adapte au bruit statistique et perd sa capacité de généralisation sur des données inconnues (*Overfitting*).

```
   SOUS-APPRENTISSAGE (Underfitting)          BON COMPROMIS (Good Fit)             SURAPPRENTISSAGE (Overfitting)
      Erreur élevée sur Train & Test           Erreur faible sur Train & Test       Erreur quasi nulle sur Train,
         Modèle trop rigide.                    Modèle équilibré & robuste.          Erreur catastrophique sur Test.
```

### 3.1 Régularisation Ridge ($L_2$)
Selon le principe du **Rasoir d'Ockham**, un modèle aux coefficients plus petits est plus simple et généralise mieux. La régression Ridge pénalise la norme $L_2$ des poids :
$$\min_{\mathbf{w}} \|\mathbf{X}\mathbf{w} - \mathbf{y}\|_2^2 + \lambda \|\mathbf{w}\|_2^2$$
Où $\lambda \ge 0$ est l'hyperparamètre contrôlant le compromis entre ajustement aux données et parcimonie des poids.
La solution analytique devient :
$$\mathbf{w}_{\text{ridge}} = (\mathbf{X}^T \mathbf{X} + \lambda \mathbf{I}_d)^{-1} \mathbf{X}^T \mathbf{y}$$
> **Propriété fondamentale :** Même si $\mathbf{X}^T \mathbf{X}$ est non inversible (en cas de fortes collinéarités), l'ajout de $\lambda \mathbf{I}_d$ garantit mathématiquement une matrice symétrique strictement définie positive, donc toujours inversible.

---

## 4. Apprentissage Distribué : Complexité Calcul et Stockage

La solution analytique $\mathbf{w} = (\mathbf{X}^T \mathbf{X})^{-1} \mathbf{X}^T \mathbf{y}$ confronte le concepteur de systèmes distribués aux limites d'échelle.

### 4.1 Analyse des Coûts
* **Nombre d'opérations arithmétiques :**
  - Multiplication matricielle $\mathbf{X}^T \mathbf{X}$ : $\mathcal{O}(n d^2)$ opérations.
  - Inversion de matrice $(\mathbf{X}^T \mathbf{X})^{-1}$ : $\mathcal{O}(d^3)$ opérations.
  - *Total calcul :* $\mathcal{O}(n d^2 + d^3)$.
* **Empreinte mémoire (stockage en flottants 64 bits / 8 octets) :**
  - Matrice des données $\mathbf{X}$ : $\mathcal{O}(n d)$ flottants.
  - Matrice de covariance $\mathbf{X}^T \mathbf{X}$ et son inverse : $\mathcal{O}(d^2)$ flottants.
  - *Total stockage :* $\mathcal{O}(n d + d^2)$.

---

### 4.2 Cas d'Étude 1 : Grand $n$ et Petit $d$ (*Big $n$, Small $d$*)
*Supposition :* On a des milliards d'observations ($n = 10^{10}$), mais seulement quelques dizaines ou centaines de caractéristiques ($d = 100$).
- $d^2 = 10\,000$ flottants $\approx 80$ Ko (tient dans le cache L2 du processeur !).
- $d^3 = 10^6$ opérations (s'exécute en quelques millisecondes sur un seul cœur).
- **Le goulot d'étranglement réside dans le stockage de $\mathbf{X}$ ($\mathcal{O}(nd)$) et le calcul de $\mathbf{X}^T \mathbf{X}$ ($\mathcal{O}(nd^2)$).**

#### La Décomposition en Somme de Produits Externes (*Outer Products*)
Au lieu de multiplier des matrices par blocs géants, on décompose le produit matriciel ligne par ligne :
$$\mathbf{X}^T \mathbf{X} = \sum_{i=1}^n \mathbf{x}^{(i)} (\mathbf{x}^{(i)})^T \quad \text{et} \quad \mathbf{X}^T \mathbf{y} = \sum_{i=1}^n y^{(i)} \mathbf{x}^{(i)}$$

```
                   PARALLÉLISATION EN GRAND n ET PETIT d (MapReduce / Spark)
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│ 1. STOCKAGE DISTRIBUÉ (Map) :                                                               │
│    Chaque travailleur (Worker) héberge un sous-ensemble de lignes de X (O(nd) distribué).   │
├─────────────────────────────────────────────────────────────────────────────────────────────┤
│ 2. CALCUL PARALLÈLE LOCAL (Map) :                                                           │
│    Chaque Worker calcule la somme locale des produits externes x_i * x_i^T (taille d x d).  │
├─────────────────────────────────────────────────────────────────────────────────────────────┤
│ 3. AGRÉGATION CENTRALE (Reduce) :                                                           │
│    Les Workers renvoient leurs matrices partielles (d x d) au Driver qui les additionne.    │
│    Le trafic réseau total est minuscule : seulement O(d^2) flottants !                      │
├─────────────────────────────────────────────────────────────────────────────────────────────┤
│ 4. RÉSOLUTION FINALE LOCALE (Driver) :                                                      │
│    Le Driver résout (X^T X)^-1 X^T y en O(d^3) localement en une fraction de seconde.       │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

### 4.3 Cas d'Étude 2 : Grand $n$ et Grand $d$ (*Big $n$, Big $d$*)
*Supposition :* En génomique ou NLP, $d = 1\,000\,000$ et $n = 10^8$.
- Stocker $\mathbf{X}^T \mathbf{X}$ exige $d^2 = 10^{12}$ flottants, soit **8 Téraoctets de mémoire vive pour une seule matrice** !
- Inverser cette matrice exigerait $\mathcal{O}(d^3) = 10^{18}$ opérations arithmétiques, ce qui prendrait des années de calcul.
- **Règle d'or de l'ingénierie distribuée (*1st Rule of thumb*) :**  
  > *Le calcul et le stockage doivent demeurer strictement linéaires en $n$ et en $d$ : $\mathcal{O}(n d)$.*
- **Conséquence directe :** La forme fermée analytique est abandonnée au profit d'algorithmes itératifs d'optimisation de premier ordre : la **Descente de Gradient Stochastique (SGD)** et les méthodes quasi-Newton distribuées (L-BFGS).

---

## 5. Mise en Pratique : Les Pipelines MLlib (`pyspark.ml`)

Le laboratoire pratique explore la prédiction des prix de location Airbnb à San Francisco (`AirBnBRentalPrices.ipynb.txt`).

### 5.1 Architecture des Pipelines Spark ML
Spark MLlib s'inspire de scikit-learn en structurant les étapes d'apprentissage autour de deux briques immuables :
1. **Les Transformateurs (*Transformers*) :** Prennent un DataFrame et produisent un nouveau DataFrame enrichi (ex. `VectorAssembler`). Méthode : `.transform()`.
2. **Les Estimateurs (*Estimators*) :** Algorithmes qui apprennent sur les données et produisent un Transformateur (ex. `LinearRegression` produit un `LinearRegressionModel`). Méthode : `.fit()`.

```python
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

# 1. Préparation des caractéristiques
vecAssembler = VectorAssembler(inputCols=["bedrooms", "bathrooms", "accommodates"], outputCol="features")

# 2. Estimateur de régression
lr = LinearRegression(featuresCol="features", labelCol="price")

# 3. Encapsulation dans un Pipeline
pipeline = Pipeline(stages=[vecAssembler, lr])

# 4. Partitionnement Train / Test
trainDF, testDF = airbnbDF.randomSplit([0.8, 0.2], seed=42)

# 5. Entraînement et Prédiction
model = pipeline.fit(trainDF)
predDF = model.transform(testDF)

# 6. Évaluation multi-métriques
evaluator = RegressionEvaluator(labelCol="price", predictionCol="prediction")
rmse = evaluator.setMetricName("rmse").evaluate(predDF)
r2 = evaluator.setMetricName("r2").evaluate(predDF)
print(f"RMSE: {rmse:.2f}, R2: {r2:.4f}")
```

### 5.2 Réglage des Hyperparamètres et Validation Croisée ($k$-Fold)
Pour éviter le surapprentissage et sélectionner les meilleurs hyperparamètres (ex. profondeur de l'arbre, paramètre de régularisation), on applique la validation croisée parallélisée :
```python
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

paramGrid = ParamGridBuilder() \
    .addGrid(lr.regParam, [0.01, 0.1, 1.0]) \
    .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0]) \
    .build()

cv = CrossValidator(estimator=pipeline,
                    estimatorParamMaps=paramGrid,
                    evaluator=evaluator,
                    numFolds=3,
                    parallelism=4)  # Entraîne 4 modèles en parallèle sur le cluster

cvModel = cv.fit(trainDF)
```
