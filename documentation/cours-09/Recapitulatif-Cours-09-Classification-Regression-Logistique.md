# MTI850 -- Analytique des donnees massives
## Synthese et Recapitulatif du Cours 09 : Classification Lineaire, Regression Logistique, Interpretation Probabiliste et Prediction de CTR

Enseignant : Prof. Alessandro L. Koerich  
Departement : Genie logiciel et des TI, Ecole de technologie superieure (ETS)  
Session : Automne 2026 (Seance 9)  

---

## 1. Fondements de la Classification Lineaire

L'objectif de la classification supervisee est d'apprendre une regle de decision reliant un vecteur d'observations x dans R^d a une etiquette discrete y dans {0, 1} (classification binaire).

### 1.1 L'Hyperplan Separateur
Dans l'espace des caracteristiques, un classificateur lineaire definit une frontiere de decision par un hyperplan affine :
w^T x + b = 0
- Pour toute observation d'un cote de l'hyperplan (w^T x + b > 0), la classe predite est 1.
- De l'autre cote (w^T x + b < 0), la classe predite est 0.

### 1.2 Pourquoi la Regression Lineaire par Moindres Carres Echoue-t-elle ?
Si l'on tente d'ajuster une regression lineaire standard y' = w^T x sur des etiquettes binaires :
1. Predictions non bornees : Le modele lineaire predit des valeurs inferieures a 0 ou superieures a 1, impossibles a interpreter comme des probabilites.
2. Sensibilite excessive aux valeurs extremes (Outliers) : Une observation de classe 1 tres eloignee de la frontiere de decision (ex. x >> 0) genere un residu eleve (w^T x - 1)^2 qui fait pivoter l'hyperplan et degrade dramatiquement le pouvoir de discrimination pres de la frontiere.

---

## 2. La Regression Logistique et la Fonction Sigmoide

Pour contraindre les sorties du modele dans l'intervalle strictement probabiliste ]0, 1[, on applique la fonction d'activation logistique (Sigmoide) au produit scalaire z = w^T x :
sigma(z) = 1 / (1 + e^(-z)) = e^z / (1 + e^z)

### 2.1 Proprietes Mathematiques Remarquables de la Sigmoide
- Image bornee : lim_{z -> -infty} sigma(z) = 0  et  lim_{z -> +infty} sigma(z) = 1.
- Symetrie : sigma(-z) = 1 - sigma(z).
- Derivee analytique elegante :
  sigma'(z) = d/dz [ (1 + e^(-z))^(-1) ] = e^(-z) / (1 + e^(-z))^2 = sigma(z) * (1 - sigma(z))
  Cette propriete rend le calcul du gradient extremement leger sur un cluster distribue.

### 2.2 Rapport de Cotes (Odds) et Espace Logit
Soit P = sigma(w^T x) la probabilite de la classe positive :
- Le rapport de cotes (Odds) represente la probabilite de succes divisee par la probabilite d'echec :
  Odds = P / (1 - P) = e^(w^T x)
- Le logarithme du rapport de cotes (Logit) redevient strictement lineaire :
  logit(P) = log(P / (1 - P)) = w^T x
La regression logistique est donc un modele lineaire dans l'espace des log-cotes !
Interpretation d'un poids w_j : Une augmentation d'une unite de la caracteristique x_j multiplie la cote de succes par e^(w_j).

---

## 3. Interpretation Probabiliste et Estimation par Maximum de Vraisemblance (MLE)

On modelise l'etiquette conditionnelle comme une loi de Bernoulli :
P(Y = 1 | x; w) = sigma(w^T x) = p_i  et  P(Y = 0 | x; w) = 1 - sigma(w^T x) = 1 - p_i
Ce qui s'exprime sous forme compacte :
P(Y = y^(i) | x^(i); w) = (p_i)^(y^(i)) * (1 - p_i)^(1 - y^(i))

### 3.1 La Fonction de Perte Logistique (Log-Loss / Cross-Entropy)
Pour n observations independantes, la fonction de vraisemblance globale est :
L(w) = prod_{i=1}^n (p_i)^(y^(i)) * (1 - p_i)^(1 - y^(i))
Maximiser la vraisemblance revient a minimiser la log-vraisemblance negative (Negative Log-Likelihood) :
f(w) = - log L(w) = - sum_{i=1}^n [ y^(i) * log(p_i) + (1 - y^(i)) * log(1 - p_i) ]

### 3.2 Gradient et Convexite Stricte
En derivant f(w) par rapport a w grace a sigma'(z) = sigma(z)(1 - sigma(z)) :
nabla f(w) = sum_{i=1}^n (p_i - y^(i)) * x^(i) = X^T (p - y)
Remarque : L'equation du gradient possede la meme structure formelle que les moindres carres, avec le vecteur de probabilites predites p a la place de y' !

La matrice Hessienne s'ecrit :
H(w) = nabla^2 f(w) = X^T W X
ou W = diag(p_1 (1 - p_1), ..., p_n (1 - p_n)).
Puisque 0 < p_i < 1 pour tout z fini, chaque element diagonal p_i (1 - p_i) > 0.
La matrice W est strictement definie positive, ce qui implique que H(w) est semi-definie positive.
Conclusion mathematique capitale : La perte logistique est strictement convexe. Il n'existe aucun minimum local sous-optimal ; tout minimum trouve par descente de gradient ou L-BFGS est le minimum global absolu !

---

## 4. Application Industrielle : Prediction du Taux de Clic (CTR) et Publicite en Ligne

Dans le commerce web et la publicite programmatique (contexte du projet dirige PD5 et donnees Criteo), la regression logistique est l'algorithme roi.

### 4.1 Le Defi du Desequilibre de Classes (Severe Class Imbalance)
Dans la publicite en ligne, le taux de clic reel (CTR) est souvent de l'ordre de 0.1 % (1 clic pour 1 000 impressions).
- L'exactitude brute (Accuracy) est une metrique trompeuse : un modele naif qui predit systematiquement 0 aura 99.9 % d'exactitude tout en etant totalement inutile.
- Metriques d'evaluation indispensables :
  1. Log-Loss : Mesure la qualite de la calibration probabiliste.
  2. ROC-AUC (Area Under ROC Curve) : Mesure la capacite a classer les impressions cliquees au-dessus des impressions non cliquees, insensible au desequilibre de classes.
  3. PR-AUC (Precision-Recall Curve) : Metrique critique pour les classes ultra-minoritaires.

### 4.2 Implementation PySpark MLlib
```python
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator

lr = LogisticRegression(featuresCol="features",
                        labelCol="clicked",
                        regParam=0.01,
                        elasticNetParam=0.5, # ElasticNet pour selectionner les variables
                        maxIter=20)

model = lr.fit(trainDF)
predictions = model.transform(testDF)

# Evaluation ROC-AUC
evaluator = BinaryClassificationEvaluator(labelCol="clicked",
                                          rawPredictionCol="rawPrediction",
                                          metricName="areaUnderROC")
auc = evaluator.evaluate(predictions)
```
