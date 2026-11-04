# MTI850 -- Analytique des donnees massives
## Materiel Complementaire Avance : Hashing Trick a Echelle Infinie, Formulation Implicite de Hu-Koren-Volinsky et Partitionnement Distribue d'ALS

Complement au Cours 08 : Approfondissement technique de niveau Maitrise  
Auteur / Reference d'etude : Analyse avancee pour l'ingenierie des donnees massives (ETS Montreal)

---

## 1. L'Astuce du Hachage (Hashing Trick) pour Flux Categorielles Massifs

Dans les systemes de production a grande echelle (clics publicitaires, flux de journaux), les methodes standard comme `StringIndexer` rencontrent une limite fondamentale :
- `StringIndexer` exige une passe globale prealable sur l'ensemble du jeu de donnees pour construire un dictionnaire d'indexation.
- Dans un flux temps reel ou l'espace des modalites est infini ou dynamique (nouveaux identifiants, mots-cles arbitraires), le dictionnaire grossit indefiniment en memoire vive et ne peut pas etre fige.

### 1.1 Formulation Mathematique du Feature Hashing
L'astuce du hachage (proposee par Weinberger et al., 2009) projette directement une chaine de caracteres vers un espace vectoriel de dimension fixe B = 2^b sans aucun dictionnaire en memoire :
Soit h : Chaines -> {0, 1, ..., B - 1} une fonction de hachage uniforme (typiquement MurmurHash3) et xi : Chaines -> {-1, +1} une fonction de hachage de signe independante.
L'encodage phi(x) d'une caracteristique est defini par :
phi_j(x) = sum_{k : h(x_k) = j} xi(x_k)

Propriete fondamentale de conservation du produit scalaire :
L'esperance du produit scalaire entre deux vecteurs haches est egale au produit scalaire exact dans l'espace d'origine :
E[<phi(x), phi(y)>] = <x, y>
La fonction de signe xi garantit que l'esperance des collisions croisees est nulle, minimisant l'impact du biais statistique :
Var[<phi(x), phi(y)>] = (1 / B) * (||x||_2^2 * ||y||_2^2 + <x, y>^2)
En pratique, avec B = 2^20 (~1 million d'indices), le taux de collision est negligeable pour un modele lineaire.

---

## 2. Factorisation Matricielle pour Retroaction Implicite (Formulation HKV)

Dans la majorite des plateformes (YouTube, Spotify, Amazon), les utilisateurs ne donnent pas de notes explicites (1 a 5 etoiles). Ils ecoutent, cliquent, visionnent ou achetent. C'est la retroaction implicite (Implicit Feedback).

### 2.1 Preference et Confiance (Hu, Koren et Volinsky, 2008)
On transforme l'observation brute r_{ui} (ex. nombre d'ecoutes d'une chanson) en deux grandeurs mathematiques :
1. Une preference binaire p_{ui} :
   p_{ui} = 1 si r_{ui} > 0,  et  p_{ui} = 0 si r_{ui} = 0
2. Un niveau de confiance c_{ui} :
   c_{ui} = 1 + alpha * r_{ui}
   ou alpha est un hyperparametre de ponderation (typiquement alpha = 40).
   Si r_{ui} = 0, la confiance est minimale (c_{ui} = 1) : l'utilisateur n'a pas ecoute le titre, soit parce qu'il ne l'aime pas, soit simplement parce qu'il ne sait pas qu'il existe.
   Si r_{ui} = 100, la confiance que l'utilisateur aime le titre est massive (c_{ui} = 4001).

### 2.2 La Fonction Objectif Implicite et le Goulot de Calcul
La fonction objectif s'etend desormais a TOUTES les paires (u, i), y compris les zeros :
min_{U, V} sum_{u=1}^m sum_{i=1}^n c_{ui} * (p_{ui} - u_u^T v_i)^2 + lambda * (sum_u ||u_u||_2^2 + sum_i ||v_i||_2^2)

Goulot d'etranglement : La matrice contient m * n cellules (ex. 10^7 utilisateurs * 10^6 articles = 10^13 elements) ! Calculer cette somme naivement est impossible.

### 2.3 L'Astuce Algebrique d'Acceleration
La solution pour chaque utilisateur s'ecrit :
u_u = (V^T C^u V + lambda * I_k)^(-1) * V^T C^u p_u
ou C^u est une matrice diagonale contenant les confiances c_{ui}.
Remarquons que C^u = I_n + (C^u - I_n). La matrice (C^u - I_n) ne possede des elements non nuls QUE pour les articles reellement consommes par l'utilisateur u !
On peut donc factoriser :
V^T C^u V = V^T V + sum_{i : r_{ui} > 0} (c_{ui} - 1) * v_i v_i^T

L'impact algorithmique est prodigieux :
- On precalcule la matrice V^T V de taille k par k une seule fois par iteration globale en O(n k^2).
- Pour chaque utilisateur, on ne met a jour que les articles non nuls |R_u| : complexite O(k^3 + |R_u| k^2).
- La complexite globale par iteration passe de O(m n k^2) a O(m k^3 + |R| k^2), rendant le probleme traitable en quelques minutes sur Spark MLlib via `ALS(implicitPrefs=True)` !

---

## 3. Architecture Distribuee et Biais de Donnees (Data Skew) dans Spark ALS

Dans l'implementation interne de Spark MLlib (`org.apache.spark.ml.recommendation.ALS`) :
- Les interactions sont partitionnees en blocs 2D de sous-matrices (`InLinkBlock` et `OutLinkBlock`).
- Les facteurs latents des articles v_i doivent etre envoyes aux partitions contenant les utilisateurs qui les ont notes.

Le piege de production : La disparite de degre (Heavy Tail / Data Skew) :
- Un utilisateur moyen note 20 films.
- Un robot ou un compte corporatif note 500 000 articles.
- De meme, un film block-buster est note par 10 millions d'utilisateurs.
La partition recevant ces entites sur-representees subit un afflux massif de messages reseau, saturant la memoire de l'executeur (Java OutOfMemoryError).
Solution d'ingenierie : Filtrer prealablement les utilisateurs anormaux lors de l'etape de nettoyage ETL ou augmenter le partitionnement de blocs (`numUserBlocks` et `numItemBlocks`).
