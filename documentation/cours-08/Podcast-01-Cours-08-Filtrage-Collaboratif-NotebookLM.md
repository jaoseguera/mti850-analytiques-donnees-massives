# Source Audio Overview (NotebookLM) -- Episode : La Magie de la Recommandation et la Factorisation Matricielle
## Guide de Discussion et Contenu Pedagogique pour Podcast Audio
Contexte : Cours MTI850 (Analytique des donnees massives), Seance 8, ETS Montreal.  
Sujet de l'episode : Dompter les millions de categories sans saturer la memoire, la factorisation matricielle a l'echelle de Netflix, et comment l'algorithme ALS resout un probleme non convexe grace a une danse algebrique alternee.

---

### Introduction et Accroche Narrative
Bienvenue dans ce nouvel episode consacre aux grands defis des donnees massives.
Dans les seances precedentes, nous avons travaille sur des nombres : des temperatures, des puissances electriques, des coordonnees.
Mais dans le commerce reel, la majorite des donnees sont des mots : des quartiers, des genres de films, des identifiants d'utilisateurs.

Comment un moteur comme celui de Netflix ou de Spotify peut-il deviner avec une precision troublante quel film ou quelle chanson vous allez adorer ce soir, alors que vous ne lui avez jamais donne votre avis sur ce titre ?
Comment transforme-t-on une grille gigantesque remplie a 99.9 % de vide en un moteur de recommandation hautement rentable ?
Et quel est ce tour de passe-passe mathematique qui permet a l'algorithme ALS de contourner l'un des problemes les plus complexes du Machine Learning en alternant simplement deux calculs lineaires ?

Attachez votre ceinture, nous entrons dans la mecanique secrete des systemes de recommandation industriels !

---

### Segment 1 : L'Explosion des Categories et les Vecteurs Creux

Commencons par un piege technique dans lequel tombent tous les debutants : le passage des categories aux nombres.
Prenez la ville de San Francisco sur Airbnb. Il y a des dizaines de quartiers et des dizaines de types de proprietes.
Pour que ces categories soient comprises par un algorithme lineaire, on utilise le One-Hot Encoding : on cree une colonne binaire pour chaque categorie possible.

Mais que se passe-t-il si vous travaillez sur dix mille codes postaux ?
Si vous creez dix mille colonnes pleines de zeros, votre tableau de donnees devient monstrueusement lourd. Pour un million de clients, stocker tous ces zeros occuperait des dizaines de gigaoctets de memoire pour rien !

C'est ici qu'interviennent les vecteurs creux (Sparse Vectors) de Spark MLlib.
Au lieu d'ecrire dix mille zeros et un seul un, Spark ne stocke que trois informations : la taille globale du vecteur, l'indice ou se trouve le un, et la valeur un.
En une seule ligne, l'empreinte memoire est divisee par cinq mille !
Et grace au pipeline standard compose de StringIndexer, OneHotEncoder et VectorAssembler, cette transformation s'execute de maniere fluide et reproductible sur l'ensemble du cluster.

---

### Segment 2 : Le Mystere de la Matrice Netflix

Passons maintenant au coeur de la seance : le filtrage collaboratif et le fameux defi Netflix.
Imaginez une matrice geante : les lignes representent cent millions d'utilisateurs, et les colonnes representent cent mille films.
Chaque case contient la note de un a cinq etoiles attribuee par un utilisateur a un film.

Voici la realite statistique de cette matrice :
Meme le cinephile le plus devoue n'a vu que quelques centaines de films dans sa vie. La matrice est donc vide a plus de 99.9 % !
Comment peut-on predire la note d'une case vide sans connaitre le resume du film ni la biographie de l'utilisateur ?

La reponse repose sur la decouverte des facteurs latents.
On postule qu'il existe un petit nombre de dimensions cachees (par exemple vingt facteurs latents), comme le degre d'action, la dose d'humour ou la presence d'effets speciaux.
Chaque utilisateur est decrit par un petit vecteur de vingt nombres exprimant ses gouts, et chaque film est decrit par un petit vecteur de vingt nombres exprimant ses caracteristiques.
La note predite est tout simplement le produit scalaire entre le profil de l'utilisateur et le profil du film !

---

### Segment 3 : La Danse Algebrique de l'Algorithme ALS

Mais comment apprend-on simultanement le profil de millions d'utilisateurs et de centaines de milliers de films quand on ne connait aucun des deux au depart ?

Si vous regardez l'equation d'erreur, c'est un cauchemar mathematique : on multiplie deux inconnues entre elles, ce qui cree une fonction non convexe remplie de pieges et de minima locaux.
C'est ici que l'algorithme des moindres carres alternes (ALS) realise un coup de maitre.

Les mathématiciens ont remarque une propriete fascinante :
Si vous faites semblant de connaitre le profil des films et que vous les figez, le probleme pour trouver le profil des utilisateurs redevient une simple regression lineaire standard, parfaitement convexe et facile a resoudre !
Et mieux encore : chaque utilisateur etant independant, vous pouvez calculer les profils de cent millions d'utilisateurs en parallele absolu sur les serveurs de votre cluster !

Puis, une fois les utilisateurs calcules, vous inversez les roles : vous figez les profils utilisateurs, et vous resolvez le profil de chaque film, a nouveau en parallele absolu !
On alterne ainsi d'avant en arriere : on fige les films pour apprendre les utilisateurs, puis on fige les utilisateurs pour apprendre les films.
A chaque demi-etape, l'erreur globale diminue de maniere mathematiquement prouvee. En une quinzaine d'allers-retours, le modele converge vers une precision remarquable !

---

### Segment 4 : Le Piege du Demarrage a Froid en Production

Pour finir, la seance a mis en garde contre le piege du demarrage a froid (le fameux Cold Start).
Qu'arrive-t-il quand un nouvel abonné s'inscrit sur Netflix ?
Il n'a encore note aucun film. Il n'a aucun historique.
Si vous demandez au modele de predire ce qu'il va aimer, le modele ne dispose d'aucun vecteur latent pour cet utilisateur et renvoie une erreur ou une valeur manquante.

Dans Spark MLlib, si vous n'y prenez pas garde lors de votre validation croisee, ces valeurs manquantes vont faire planter tout votre pipeline d'evaluation.
D'ou l'importance du parametre `coldStartStrategy="drop"` que nous avons utilise dans le laboratoire : il demande a Spark d'ignorer proprement les utilisateurs non vus dans les metriques, pendant que les equipes produit utilisent des regles de repli par popularite pour accueillir les nouveaux arrivants.

---

### Conclusion et Synthese
Pour recapituler cette lecon essentielle :
1. Les donnees categorielles a haute cardinalite exigent des vecteurs creux (SparseVector) pour ne pas noyer la memoire sous des oceans de zeros.
2. Le filtrage collaboratif compresse une matrice geante et vide en un petit espace de facteurs latents utilisateurs et articles.
3. L'algorithme ALS transforme un probleme non convexe impossible en une serie de regressions lineaires paralleles grace a l'alternance biconvexe.
4. Le probleme du demarrage a froid exige une strategie explicite pour eviter les plantages en production.
