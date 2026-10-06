# Source Audio Overview (NotebookLM) — Épisode 9 : Du Calcul Matriciel au Machine Learning Distribué
## Guide de Discussion & Contenu Pédagogique pour Podcast Audio
**Contexte :** Cours MTI850 (Analytique des données massives), Séance 5, ÉTS Montréal.  
**Sujet de l'épisode :** La traversée du miroir — Comment passer de la manipulation de données SQL à l'entraînement de modèles d'apprentissage automatique sur des pétaoctets de données, les pièges de l'inversion matricielle, le génie du produit externe distribué, et la prédiction des prix Airbnb avec Spark MLlib.

---

### Introduction & Accroche Narrative
Bienvenue dans ce neuvième épisode de notre série consacrée aux données massives. Jusqu'à présent, nous avons appris à charger des données, à les nettoyer, à optimiser des requêtes avec Catalyst et à dompter le stockage objet dans le cloud.

Mais aujourd'hui, nous franchissons une frontière majeure : **nous entrons dans le monde du Machine Learning distribué !**

Dans les manuels universitaires de statistiques, la régression linéaire est souvent présentée comme l'algorithme le plus simple du monde. Il y a une belle formule mathématique élégante, on inverse une matrice, et le tour est joué.
Mais que se passe-t-il lorsque votre jeu de données compte 10 milliards de lignes ?
Pourquoi cette formule mathématique apparemment innocente peut-elle faire exploser les mémoires de vos serveurs et nécessiter des milliers d'années de calcul si l'on ne comprend pas la physique du calcul distribué ?
Et comment des entreprises comme Airbnb entraînent-elles des modèles prédictifs complexes sur des clusters Spark en quelques minutes ?

Attachez votre ceinture, nous plongeons au cœur de l'algèbre linéaire à l'échelle du Big Data !

---

### Segment 1 : La Beauté et la Rigueur du Modèle Linéaire

Commençons par le commencement : pourquoi les ingénieurs en données massives continuent-ils d'adorer la régression linéaire à l'ère des réseaux de neurones profonds ?

La réponse tient en trois mots : **simplicité, interprétabilité et efficacité**.
Quand vous écrivez que le prix d'un logement est une combinaison linéaire de ses caractéristiques :
$$\text{Prix} \approx w_0 + w_1 \times \text{chambres} + w_2 \times \text{salles\_de\_bain}$$
Chaque poids $w$ a une signification économique immédiate. Si $w_1$ vaut 50, cela signifie qu'une chambre supplémentaire augmente le prix moyen de 50 dollars par nuit. Dans le secteur bancaire, médical ou réglementé, cette transparence est indispensable !

Et pour les ingénieurs logiciels, il y a une astuce mathématique sublime :
On ajoute une composante constante égale à 1 au début de chaque ligne de données. Grâce à cette petite augmentation, l'ordonnée à l'origine (le biais $w_0$) se fond naturellement dans le produit scalaire $\mathbf{w}^T \mathbf{x}$. Une seule opération matricielle remplace toutes les équations !

---

### Segment 2 : La Tentation et le Piège de la Formule Fermée

Tout étudiant en mathématiques apprend la célèbre « solution fermée » des moindres carrés :
$$\mathbf{w} = (\mathbf{X}^T \mathbf{X})^{-1} \mathbf{X}^T \mathbf{y}$$
Sur une feuille de papier ou dans un carnet Jupyter avec 1 000 lignes de données, cette formule est un rêve. Vous cliquez sur "Exécuter", et le résultat apparaît en deux millisecondes.

Mais le professeur Koerich et les chercheurs de Databricks posent la question qui fâche : **quelle est la complexité algorithmique réelle de cette formule ?**

Faisons les comptes :
1. Multiplier la matrice transposée $\mathbf{X}^T$ par $\mathbf{X}$ demande $\mathcal{O}(n d^2)$ opérations arithmétiques, où $n$ est le nombre de lignes et $d$ le nombre de variables.
2. Inverser la matrice résultante demande $\mathcal{O}(d^3)$ opérations !
3. En mémoire, stocker $\mathbf{X}$ prend $\mathcal{O}(nd)$ nombres flottants, et stocker $\mathbf{X}^T \mathbf{X}$ prend $\mathcal{O}(d^2)$ flottants.

Tant que vous êtes sur votre ordinateur portable avec de petits jeux de données, vous ne remarquez rien. Mais sur un cluster Big Data, cette formule cache deux destins complètement opposés !

---

### Segment 3 : Le Coup de Génie du Cas « Grand $n$, Petit $d$ »

Examinons le premier scénario, très fréquent dans l'industrie : vous avez **des milliards d'observations ($n = 10^{10}$)**, mais seulement **100 caractéristiques ($d = 100$)**. Par exemple, 10 milliards de clics web décrits par 100 variables démographiques.

Regardons les chiffres :
- $d = 100 \implies d^2 = 10\,000$ nombres flottants. Cela représente à peine **80 kilo-octets de mémoire** ! Cela tient largement dans la mémoire cache du processeur !
- Et inverser une matrice $100 \times 100$ prend $\mathcal{O}(d^3) = 10^6$ opérations, ce qui prend moins d'un millième de seconde sur un seul cœur de CPU !

Alors, où est le problème ? Le problème, c'est que la matrice de données $\mathbf{X}$ contient 10 milliards de lignes. Elle fait plusieurs téraoctets et ne rentre dans la mémoire d'aucun ordinateur individuel !

#### Comment Spark résout-il l'impossible ?
C'est ici qu'intervient la magie de la **décomposition en produits externes (*Outer Products*)** :
Au lieu d'essayer de multiplier les matrices de manière monolithique, les mathématiques nous disent que :
$$\mathbf{X}^T \mathbf{X} = \sum_{i=1}^n \mathbf{x}_i \mathbf{x}_i^T$$
C'est une simple addition !
1. Chaque machine ouvrière (*Worker*) conserve un fragment des milliards de lignes sur son disque ou sa RAM.
2. Localement, chaque machine calcule la somme de ses petits produits externes. Le résultat local est une minuscule matrice de $100 \times 100$ !
3. Chaque machine envoie ses 80 kilo-octets au coordinateur central (*Driver*). Le trafic réseau sur tout le cluster est quasiment nul !
4. Le Driver additionne ces petites matrices et calcule l'inversion en un clin d'œil.
En comprenant l'algèbre linéaire, nous venons de paralléliser un calcul de 10 milliards de lignes sans saturer le réseau !

---

### Segment 4 : Le Mur Infranchissable du Cas « Grand $n$, Grand $d$ »

Mais attention : le paradis s'arrête brutalement dès que vous entrez dans le deuxième scénario : **Grand $n$ ET Grand $d$**.
C'est le monde du traitement automatique du langage naturel (NLP avec des sacs de mots) ou de la génomique, où l'on a **un million de variables ($d = 1\,000\,000$)**.

Calculons ce qui se passe si vous essayez d'utiliser la formule fermée :
- Pour simplement stocker la matrice $\mathbf{X}^T \mathbf{X}$, il vous faut $d^2 = 10^{12}$ flottants de 8 octets. Cela représente **8 Téraoctets de mémoire vive pour une seule matrice intermédiaire !**
- Et pour l'inverser ? $\mathcal{O}(d^3) = 10^{18}$ opérations. Même avec le supercalculateur le plus rapide du monde, votre programme tournerait pendant des siècles !

C'est ici que le cours énonce la **Première Règle d'Or du Machine Learning Distribué (*1st Rule of Thumb*)** :
> *Tout algorithme viable à l'échelle massive doit avoir un coût en calcul et en stockage strictement linéaire en $n$ et en $d$ : $\mathcal{O}(n d)$.*

Puisque la formule fermée est cubique en $d$, **elle est morte à grande échelle**. Et c'est précisément pour cela que le monde moderne a abandonné les formules exactes au profit d'algorithmes d'approximation itératifs comme la descente de gradient, que nous explorerons au prochain cours !

---

### Segment 5 : La Pratique Industrielle — Le Laboratoire Airbnb avec Spark MLlib

Pour clore cette séance, nous avons mis les mains dans le moteur avec le jeu de données des prix de location Airbnb à San Francisco (`sf-airbnb`).

Comment construit-on un pipeline de Machine Learning professionnel avec Spark MLlib ?
1. **Le `VectorAssembler` :** Spark n'accepte pas des colonnes éparpillées pour ses modèles. Il exige que toutes les variables prédictives (`bedrooms`, `bathrooms`, `accommodates`) soient fusionnées dans un vecteur unique et compact. C'est le rôle du Transformateur `VectorAssembler`.
2. **L'architecture des Pipelines :** Pour éviter les erreurs humaines et automatiser le déploiement, on assemble les étapes dans un `Pipeline(stages=[vecAssembler, lr])`. Le pipeline garantit que les mêmes transformations sont appliquées de manière strictement identique sur les données d'entraînement et les futures données de production.
3. **La Régularisation Ridge contre le surapprentissage :** Pour éviter que le modèle ne mémorise par cœur le bruit des données, on applique la régularisation $L_2$ avec le paramètre `regParam`.
4. **La Validation Croisée Parallèle :** Grâce à `CrossValidator(..., parallelism=4)`, Spark entraîne 4 combinaisons d'hyperparamètres simultanément sur différents exécuteurs du cluster, divisant le temps de tuning par quatre !

---

### Conclusion & Synthèse
Pour récapituler cette séance charnière :
1. La régression linéaire reste le pilier de l'apprentissage supervisé grâce à son interprétabilité et son efficacité.
2. La solution analytique fermée $(\mathbf{X}^T \mathbf{X})^{-1} \mathbf{X}^T \mathbf{y}$ est redoutable quand $d$ est petit, car elle se distribue parfaitement par sommes de produits externes avec un trafic réseau minime.
3. Mais dès que la dimension $d$ devient gigantesque, la complexité en $\mathcal{O}(d^3)$ impose de respecter la règle d'or de la linéarité $\mathcal{O}(nd)$.
4. Enfin, Spark MLlib standardise ce flux de travail grâce à l'abstraction puissante des Transformers, Estimators et Pipelines reproductibles.
