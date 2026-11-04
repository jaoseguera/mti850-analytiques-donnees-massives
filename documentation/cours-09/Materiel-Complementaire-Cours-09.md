# MTI850 -- Analytique des donnees massives
## Materiel Complementaire Avance : Algorithme IRLS, Calibration Probabiliste en Encheres Temps Reel et Algorithme FTRL-Proximal

Complement au Cours 09 : Approfondissement technique de niveau Maitrise  
Auteur / Reference d'etude : Analyse avancee pour l'ingenierie des donnees massives (ETS Montreal)

---

## 1. Derivation Formelle de l'Algorithme IRLS (Iteratively Reweighted Least Squares)

Bien que Spark MLlib utilise L-BFGS pour le passage a l'echelle, la theorie statistique classique de la regression logistique repose sur la methode de Newton-Raphson, qui prend une forme algebrique remarquable appelee IRLS.

### 1.1 Derivation de la Mise a Jour de Newton
Rappelons le gradient et la Hessienne de la log-vraisemblance negative :
nabla f(w) = X^T (p - y)
H(w) = X^T W X,  ou W = diag(p_1(1 - p_1), ..., p_n(1 - p_n))
La mise a jour de second ordre de Newton-Raphson s'ecrit :
w^(t+1) = w^(t) - H(w^(t))^(-1) * nabla f(w^(t))
w^(t+1) = w^(t) - (X^T W_t X)^(-1) * X^T (p_t - y)

### 1.2 Equivalence avec les Moindres Carres Ponderes
En factorisant par (X^T W_t X)^(-1) X^T W_t :
w^(t+1) = (X^T W_t X)^(-1) * X^T W_t * [ X w^(t) - W_t^(-1) (p_t - y) ]
Definissons la variable ajustee (working response) z_t dans R^n par :
z_t = X w^(t) + W_t^(-1) (y - p_t)
L'equation de mise a jour devient exactement :
w^(t+1) = (X^T W_t X)^(-1) X^T W_t z_t

Portee mathematique fondamentale :
A chaque iteration t, la regression logistique resout exactement un probleme de regression lineaire aux moindres carres ponderes (Weighted Least Squares), ou la matrice de poids W_t et la cible z_t sont reevaluees dynamiquement !
Pourquoi Spark MLlib abandonne-t-il IRLS au profit de L-BFGS ?
Parce que resoudre (X^T W_t X)^(-1) reclame O(d^3) a chaque iteration de Newton, ce qui est infaisable des que d depasse quelques milliers de variables.

---

## 2. Calibration Probabiliste contre Discrimination en Encheres Temps Reel (RTB)

Dans l'industrie publicitaire (AdTech), la discrimination (mesuree par le ROC-AUC) ne suffit pas : la calibration exacte des probabilites est une exigence financiere absolue.

### 2.1 Le Mecanisme d'Enchere au Second Prix (GSP / Vickrey)
Lorsqu'un utilisateur charge une page web, une enchere en temps reel s'execute en moins de 100 millisecondes. Une plateforme acheteuse calcule son offre selon l'esperance de valeur :
Enchere = Valeur_par_clic * P(Clic | x) = CPC * p_predit
- Si le modele a un excellent AUC (il ordonne bien les publicites) mais surestime systematiquement les probabilites d'un facteur 2 (ex. predit 2 % au lieu de 1 %) :
  L'entreprise mise deux fois trop cher, remporte l'enchere a perte et dilapide son budget en quelques heures.
- Si le modele sous-estime les probabilites :
  L'entreprise perd systematiquement les encheres et ne diffuse aucune annonce.

### 2.2 Evaluation et Recalibration
1. Metrique de calibration : Le Brier Score :
   BS = (1 / n) * sum_{i=1}^n (p_i - y_i)^2
2. Diagramme de fiabilite (Reliability Diagram) : Decoupage des predictions en quantiles et comparaison de la probabilite moyenne predite avec la frequence empirique reelle de clics.
3. Methodes de recalibration post-traitement :
   - Mise a l'echelle de Platt (Platt Scaling) : Ajustement d'une regression logistique unidimensionnelle sur les scores bruts.
   - Regression Isotonique (Isotonic Regression) : Ajustement d'une fonction croissante par morceaux non parametrique, tres efficace sur de grands volumes de donnees.

---

## 3. L'Algorithme FTRL-Proximal (Follow-The-Regularized-Leader Proximal)

Pour predire le taux de clic sur des flux continus de publicite a grande echelle, Google a developpe et publie en 2013 l'algorithme FTRL-Proximal (McMahan et al.).

### 3.1 Les Limites du SGD Standard avec Regularisation L1
En theorie, la penalite L1 (Lasso) cree des poids strictement nuls.
Mais lorsqu'on applique la descente stochastique standard (SGD), le terme de sous-gradient ne met quasiment jamais les poids a exactement zero : les coefficients oscillent autour de zero avec des valeurs minuscules (ex. 10^(-7)), ce qui empeche la compression memoire du modele.

### 3.2 La Regle de Mise a Jour FTRL-Proximal
L'algorithme FTRL maintient deux accumulateurs par coordonnee j : la somme des gradients historiques z_j et la somme des carres des gradients n_j (taux d'apprentissage adaptatif a la AdaGrad) :
Si |z_j| <= lambda_1 :
  w_j = 0  (Le poids est force STRICTEMENT a zero !)
Sinon :
  w_j = - (1 / ((beta + sqrt(n_j)) / alpha + lambda_2)) * (z_j - sign(z_j) * lambda_1)

Avantages industriels majeurs :
1. Parcimonie reelle (Sparsity) : Plus de 80 % a 95 % des poids d'un modele a un milliard de caracteristiques deviennent strictement nuls, reduisant la taille du modele de 100 Go a moins de 5 Go de RAM.
2. Taux d'apprentissage par caracteristique : Les mots ou categories rares recoivent un pas d'apprentissage plus eleve, tandis que les caracteristiques ultra-frequentes sont stabilisees.

---

## 4. Croisement de Caracteristiques (Feature Crosses) et Machines de Factorisation (FM)

La regression logistique est lineaire : elle ne peut pas apprendre d'interactions non lineaires sans aide (ex. "Appareil = Mobile" ET "Heure = Nuit").

1. Croisement explicite (Feature Cross) : Creation manuelle d'une nouvelle variable binaire x_{ij} = x_i * x_j.
   Probleme : L'espace combinatoire explose en O(d^2), rendant la majorite des paires totalement non observees dans l'historique.
2. Machines de Factorisation (Factorization Machines - Steffen Rendle, 2010) :
   Au lieu d'apprendre un poids scalaire independant w_{ij} pour chaque paire, chaque variable j est associee a un petit vecteur latent v_j dans R^k.
   L'interaction s'ecrit : sum_{i < j} <v_i, v_j> * x_i * x_j.
   On peut ainsi estimer l'impact de combinaisons de variables rares meme si elles n'ont jamais ete vues ensemble, grace a la similarite dans l'espace latent !
