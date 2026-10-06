# MTI850 — Analytique des données massives
## Matériel Complémentaire Avancé : Algèbre Linéaire Distribuée, Conditionnement Matriciel, Régularisation ElasticNet et Optimisation BLAS

**Complément au Cours 05 :** Approfondissement technique de niveau Maîtrise  
**Auteur / Référence d'étude :** Analyse avancée pour l'ingénierie des données massives (ÉTS Montréal)

---

## 1. Dérivation Rigoureuse et Analyse du Conditionnement Matriciel

### 1.1 Dérivation Formelle en Calcul Matriciel
Soit la fonction objectif des moindres carrés ordinaires (*Ordinary Least Squares - OLS*) :
$$f(\mathbf{w}) = \|\mathbf{X}\mathbf{w} - \mathbf{y}\|_2^2 = (\mathbf{X}\mathbf{w} - \mathbf{y})^T (\mathbf{X}\mathbf{w} - \mathbf{y})$$
En développant l'expression matricielle :
$$f(\mathbf{w}) = \mathbf{w}^T \mathbf{X}^T \mathbf{X} \mathbf{w} - 2 \mathbf{y}^T \mathbf{X} \mathbf{w} + \mathbf{y}^T \mathbf{y}$$
En calculant le gradient par rapport au vecteur de paramètres $\mathbf{w}$ en utilisant les règles d'algèbre matricielle ($\nabla_{\mathbf{w}} (\mathbf{w}^T \mathbf{A} \mathbf{w}) = 2\mathbf{A}\mathbf{w}$ pour $\mathbf{A}$ symétrique) :
$$\nabla_{\mathbf{w}} f(\mathbf{w}) = 2 \mathbf{X}^T \mathbf{X} \mathbf{w} - 2 \mathbf{X}^T \mathbf{y}$$
Pour trouver le minimum global (la fonction étant strictement convexe si $\mathbf{X}^T \mathbf{X}$ est de plein rang), on annule le gradient :
$$2 \mathbf{X}^T \mathbf{X} \mathbf{w} - 2 \mathbf{X}^T \mathbf{y} = 0 \iff \mathbf{X}^T \mathbf{X} \mathbf{w} = \mathbf{X}^T \mathbf{y} \implies \mathbf{w}^* = (\mathbf{X}^T \mathbf{X})^{-1} \mathbf{X}^T \mathbf{y}$$

---

### 1.2 Le Problème du Conditionnement Matriciel et Instabilité Numérique
Dans les applications de données massives, la formule fermée souffre d'un écueil redoutable : **le mauvais conditionnement numérique**.

Le **nombre de conditionnement** d'une matrice symétrique $\mathbf{A} = \mathbf{X}^T \mathbf{X}$ est défini par :
$$\kappa(\mathbf{A}) = \frac{\lambda_{\max}(\mathbf{A})}{\lambda_{\min}(\mathbf{A})}$$
où $\lambda_{\max}$ et $\lambda_{\min}$ sont respectivement la plus grande et la plus petite valeur propre de la matrice.

```
       MATRICE BIEN CONDITIONNÉE (κ ≈ 1)              MATRICE MAL CONDITIONNÉE (κ >> 10^6)
 ┌───────────────────────────────────────────┐      ┌───────────────────────────────────────────┐
 │ • Caractéristiques non corrélées.         │      │ • Variables fortement collinéaires.       │
 │ • Surfaces de niveau circulaires.         │      │ • Vallée allongée et étroite.             │
 │ • Inversion numérique stable.             │      │ • Inversion numérique instable (erreurs   │
 │ • Petite variation de X ➔ petite          │      │   d'arrondi flottant IEEE 754 amplifiées).│
 │   variation de w.                         │      │ • Petite variation de X ➔ EXPLOSION de w !│
 └───────────────────────────────────────────┘      └───────────────────────────────────────────┘
```

Si $\kappa(\mathbf{X}^T \mathbf{X}) = 10^8$, une imprécision d'arrondi flottant de $10^{-16}$ (limite de précision double IEEE 754) peut fausser les poids calculés jusqu'à $10^{-8}$, rendant le modèle imprévisible et chaotique.

---

### 1.3 Stabilisation par le Théorème Spectral et Régularisation Ridge
La régularisation Ridge résout ce problème de manière fondamentale. Par décomposition spectrale d'une matrice symétrique réelle :
$$\mathbf{X}^T \mathbf{X} = \mathbf{Q} \mathbf{\Lambda} \mathbf{Q}^T = \mathbf{Q} \operatorname{diag}(\sigma_1^2, \sigma_2^2, \dots, \sigma_d^2) \mathbf{Q}^T$$
En ajoutant le terme de régularisation $\lambda \mathbf{I}_d$ :
$$\mathbf{X}^T \mathbf{X} + \lambda \mathbf{I}_d = \mathbf{Q} (\mathbf{\Lambda} + \lambda \mathbf{I}_d) \mathbf{Q}^T = \mathbf{Q} \operatorname{diag}(\sigma_1^2 + \lambda, \sigma_2^2 + \lambda, \dots, \sigma_d^2 + \lambda) \mathbf{Q}^T$$
Le nouveau nombre de conditionnement devient :
$$\kappa_{\text{ridge}} = \frac{\sigma_{\max}^2 + \lambda}{\sigma_{\min}^2 + \lambda}$$
Même si la plus petite valeur propre $\sigma_{\min}^2 = 0$ (matrice singulière non inversible), le dénominateur vaut désormais au minimum $\lambda > 0$. **L'inversion est mathématiquement garantie et numériquement stable.**

---

## 2. L'Algèbre Linéaire Distribuée dans Apache Spark

Pour manipuler des matrices géantes, Spark MLlib fournit des structures de données distribuées dédiées.

### 2.1 Les Abstractions de Matrices Distribuées
1. **`RowMatrix` :**
   - Stocke une matrice où chaque ligne est un vecteur local `Vector`, distribué sur les exécuteurs sous forme de `RDD[Vector]`.
   - Idéal lorsque le nombre de lignes $n$ est massif mais que le nombre de colonnes $d$ tient en mémoire locale ($d \le 10\,000$).
   - Fournit la méthode native `computeGramianMatrix()` qui implémente exactement la somme des produits externes $\sum_{i=1}^n \mathbf{x}_i \mathbf{x}_i^T$ sans aucun Shuffle réseau !
2. **`BlockMatrix` :**
   - Découpe une matrice 2D en sous-matrices locales (*blocks*) de taille $1024 \times 1024$.
   - Nécessaire lorsque la dimension $d$ est trop grande pour tenir sur une seule machine ($d > 50\,000$), permettant d'effectuer des multiplications matricielles distribuées par algorithmes de type Cannon ou Fox.

```
       ROWMATRIX (Grand n, Petit d)                     BLOCKMATRIX (Grand n, Grand d)
 ┌───────────────────────────────────────────┐      ┌─────────────────────┬─────────────────────┐
 │ Exécuteur 1 : Lignes 1 à 1 000 000        │      │ Bloc (0,0) [1024x1024]│ Bloc (0,1) [1024x1024]│
 ├───────────────────────────────────────────┤      ├─────────────────────┼─────────────────────┤
 │ Exécuteur 2 : Lignes 1 000 001 à 2 000 000│      │ Bloc (1,0) [1024x1024]│ Bloc (1,1) [1024x1024]│
 └───────────────────────────────────────────┘      └─────────────────────┴─────────────────────┘
```

---

### 2.2 Le Gouffre de Performance : JVM pure vs Accélération Matérielle BLAS
Un piège classique en ingénierie Big Data concerne l'exécution du calcul numérique dans la JVM :
- Par défaut, la JVM exécute des boucles Java standard qui ne tirent pas parti des registres vectoriels modernes des processeurs (AVX-512, SIMD).
- Spark MLlib utilise la couche d'abstraction **Netlib-Java** pour déléguer les calculs d'algèbre linéaire à des bibliothèques C/Fortran natives ultra-optimisées : **OpenBLAS**, **Intel MKL** ou **Apple Accelerate**.

> **Impact en production :**  
> Si les bibliothèques BLAS natives ne sont pas installées sur les nœuds du cluster, Spark bascule silencieusement sur une implémentation de repli en pur Java (*F2J - Fortran to Java*). Les multiplications matricielles et la convergence des algorithmes deviennent alors **5 à 20 fois plus lentes** !

---

## 3. Lasso ($L_1$) vs Ridge ($L_2$) vs ElasticNet : Géométrie et Optimisation

Spark MLlib unifie les approches de régularisation à travers l'estimateur `LinearRegression` avec les paramètres `regParam` ($\lambda$) et `elasticNetParam` ($\alpha$).

$$\mathcal{L}(\mathbf{w}) = \frac{1}{2n} \|\mathbf{X}\mathbf{w} - \mathbf{y}\|_2^2 + \lambda \left( \alpha \|\mathbf{w}\|_1 + \frac{1 - \alpha}{2} \|\mathbf{w}\|_2^2 \right)$$

```
     RÉGULARISATION RIDGE (L2) : α = 0                 RÉGULARISATION LASSO (L1) : α = 1
 ┌───────────────────────────────────────────┐      ┌───────────────────────────────────────────┐
 │ • Pénalité : ||w||_2^2 (sphère lisse).    │      │ • Pénalité : ||w||_1 (losange à pointes). │
 │ • Réduit uniformément les poids vers zéro.│      │ • Force certains poids à EXACTEMENT ZÉRO. │
 │ • Ne fait pas de sélection de variables.  │      │ • Sélection automatique de caractéristiques│
 │ • Solution analytique directe possible.   │      │ • Pas de forme fermée (non dérivable en 0)│
 └───────────────────────────────────────────┘      └───────────────────────────────────────────┘
```

### 3.1 Pourquoi Lasso ($L_1$) impose-t-il la Parcimonie (*Sparsity*) ?
Géométriquement, l'espace des contraintes de la norme $L_1$ forme un polyèdre pointu (un losange en 2D, un octaèdre en 3D). Les ellipses de la fonction de coût des moindres carrés ont une probabilité maximale de toucher la zone de contrainte sur l'un de ses sommets, où une ou plusieurs coordonnées sont **strictement nulles**.
- **Conséquence algorithmique :** Comme la norme $L_1$ n'est pas différentiable au point zéro, on ne peut pas annuler le gradient. Spark utilise alors des méthodes d'optimisation itératives comme la **descente par coordonnées (*Coordinate Descent*)** ou **L-BFGS avec opérateur proximal**.

### 3.2 ElasticNet : Le Meilleur des Deux Mondes
- Si des variables sont fortement corrélées entre elles, Lasso a tendance à en sélectionner une seule au hasard et à éliminer les autres.
- Ridge conserve toutes les variables avec de petits coefficients.
- **ElasticNet ($\alpha \in ]0, 1[$)** combine les deux : il effectue une sélection de variables tout en maintenant un regroupement stable des caractéristiques collinéaires.

---

## 4. Prévention Rigoureuse des Fuites de Données (*Data Leakage*) dans les Pipelines Spark

L'erreur la plus fréquente et destructrice commise par les scientifiques de données débutants est le **Data Leakage** (fuite d'information du futur vers le passé).

### 4.1 L'Exemple Typique du Scaler Faux
Considérons la normalisation des variables avec `StandardScaler` (centrage et réduction par soustraction de la moyenne et division par l'écart-type) :
* **Pratique erronée (Fuite) :**
  ```python
  # ERREUR FATALE : Normaliser l'ensemble du DataFrame avant le découpage !
  scaled_df = standard_scaler.fit(df).transform(df)
  train_df, test_df = scaled_df.randomSplit([0.8, 0.2])
  ```
  *Pourquoi est-ce fatal ?* La moyenne et l'écart-type ont été calculés en utilisant les données de test. Le modèle bénéficie d'une connaissance implicite du futur, produisant des scores de précision artificiellement élevés qui s'effondrent lamentablement en production réelle !

### 4.2 La Solution Architecturale : Les Pipelines Spark ML
```python
# PRATIQUE PROFESSIONNELLE STRICTE
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

# Le Pipeline encapsule le transformateur et l'estimateur
pipeline = Pipeline(stages=[standard_scaler, vec_assembler, lr])

# .fit() calcule la moyenne et la variance UNIQUEMENT sur train_df
pipeline_model = pipeline.fit(train_df)

# .transform() applique ces paramètres figés sur test_df
predictions = pipeline_model.transform(test_df)
```
Dans l'architecture Spark, un `Pipeline` garantit que tous les états statistiques des étapes de préparation sont appris strictement sur le sous-ensemble d'apprentissage et propagés de manière étanche aux étapes ultérieures.

---

## 5. Synthèse des Concepts d'Ingénierie pour l'Examen

1. **Complexité de la solution fermée :** Calcul en $\mathcal{O}(nd^2 + d^3)$ et stockage en $\mathcal{O}(nd + d^2)$.
2. **Produit externe distribué (Grand $n$, Petit $d$) :** Chaque machine calcule une matrice $d \times d$ locale en $\mathcal{O}(nd^2)$, puis le Driver centralise un volume minuscule $\mathcal{O}(d^2)$ pour inverser en local.
3. **Limite de passage à l'échelle (Grand $n$, Grand $d$) :** Si $d = 10^6$, $\mathbf{X}^T \mathbf{X}$ sature la RAM (8 To) et l'inversion en $\mathcal{O}(d^3)$ devient impossible. La règle d'or impose des algorithmes en $\mathcal{O}(nd)$ linéaires (Descente de gradient).
4. **Ridge et conditionnement :** L'ajout de $\lambda \mathbf{I}$ garantit mathématiquement que la matrice est définie positive et réduit le nombre de conditionnement $\kappa$.
5. **ElasticNet :** Permet de concilier la sélection de variables ($L_1$) et la stabilité face à la collinéarité ($L_2$).
