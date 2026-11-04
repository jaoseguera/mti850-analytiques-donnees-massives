# Source Audio Overview (NotebookLM) -- Episode : Secrets d'Encheres AdTech, Calibration et Algorithme FTRL
## Guide de Discussion Approfondie et Secrets d'Architecture pour Podcast Audio
Contexte : Cours MTI850 (Analytique des donnees massives), Niveau Maitrise, ETS Montreal.  
Sujet de l'episode : La formule secrete d'IRLS cachée derriere Newton, pourquoi une mauvaise calibration probabiliste peut ruiner une entreprise en encheres temps reel, l'algorithme FTRL-Proximal de Google pour eliminer les poids inutiles, et les Machines de Factorisation.

---

### Introduction et Accroche Narrative
Bienvenue dans cette session d'approfondissement technique consacree aux rouages les plus sophistiques de la classification probabiliste et de la publicite numerique.
Dans l'episode precedent, nous avons vu comment la regression logistique transforme des additions lineaires en probabilites fiables.

Mais aujourd'hui, nous passons de l'autre cote du miroir, la ou chaque milliseconde et chaque decimales de probabilite se traduisent par des millions de dollars de profits ou de pertes :
- Quel lien secret unit la regression logistique aux moindres carres ordinaires a travers l'algorithme IRLS ?
- Pourquoi un modele qui classe parfaitement les utilisateurs peut-il quand meme faire faillite en salle des marches publicitaires s'il n'est pas calibree ?
- Comment les ingenieurs de Google ont-ils reussi a comprimer des modeles d'un milliard de parametres dans un simple serveur de production grace a l'algorithme FTRL-Proximal ?
- Et comment capture-t-on les interactions complexes entre variables sans faire exploser l'espace mathematique ?

Ouvrons la boite noire de l'AdTech de pointe !

---

### Segment 1 : IRLS ou le Masque Algebrique de la Regression Logistique

Commencons par une decouverte theorique eblouissante.
Nous avons dit que la regression logistique n'a pas de solution analytique directe et qu'elle necessite des algorithmes iteratifs.
Mais si vous appliquez la methode de second ordre de Newton-Raphson a la regression logistique, que se passe-t-il mathematiquement ?

L'equation se transforme en un algorithme celebre appele IRLS : les moindres carres reponderes iterativement (Iteratively Reweighted Least Squares).
A chaque iteration, l'algorithme resout litteralement une regression lineaire ordinaire aux moindres carres, mais avec une matrice de ponderation qui donne plus ou moins d'importance a chaque observation selon le degre d'incertitude de la prediction !
Puis on recalcule les probabilites, on change les poids, et on recommence.
C'est la preuve elegante que la classification logistique et la regression lineaire ne sont pas deux mondes separes : la logistique n'est rien d'autre qu'une suite infinie de regressions lineaires ponderes qui s'auto-ajustent a chaque pas !

---

### Segment 2 : Le Piege Mortel de la Calibration en Encheres Temps Reel (RTB)

Parlons maintenant d'un drame financier tres reel vecu par des plateformes d'achat publicitaire.
Quand vous chargez un article sur un site web, les annonceurs participent a une enchere en direct en moins de cent millisecondes.
Le montant qu'un algorithme propose est strictement egal au prix du clic multiplie par la probabilite predite que vous cliquiez.

Voici le piege :
Beaucoup de scientifiques de donnees ne regardent que le score ROC-AUC. Ils disent : « Mon modele a un AUC de zero virgule quatre-vingt-quinze, il est parfait pour classer les utilisateurs ! »
Mais l'AUC ne mesure que le classement relatif, pas la valeur absolue des probabilites.
Si votre modele classe parfaitement les utilisateurs, mais qu'il predit systematiquement deux pour cent de chances au lieu de un pour cent, que se passe-t-il sur le marche ?
Votre algorithme mise deux fois plus cher qu'il ne devrait sur chaque enchere.
Il remporte toutes les encheres a perte et brule le budget mensuel de votre client en trois heures chrono !

C'est pourquoi, en production AdTech, la calibration probabiliste (evaluee par le score de Brier et corrigee par la regression isotonique) est cent fois plus vitale que le simple pouvoir de discrimination !

---

### Segment 3 : FTRL-Proximal, ou la Compression Extreme de Google

Comment entraine-t-on un modele de clic quand on a des milliards de caracteristiques issues de croisements de mots-cles, de pays et d'heures ?
Si vous utilisez la regularisation Lasso L1 avec la descente stochastique standard, vous avez une mauvaise surprise :
En theorie, Lasso met les poids inutiles a zero. En pratique avec SGD, les poids oscillent a dix puissance moins sept, mais ne valent jamais exactement zero !
Votre modele continue d'occuper cent gigaoctets de memoire sur vos serveurs.

En 2013, l'equipe de recherche de Google a publie un algorithme revolutionnaire : FTRL-Proximal (Follow-The-Regularized-Leader).
Grace a un operateur de proximalite mathematique, si le gradient cumule d'une caracteristique ne depasse pas un seuil de penalite strict, le poids est force a zero absolu, sans la moindre hesitation !
Le resultat en production a ete spectaculaire :
Plus de quatre-vingt-dix pour cent des coefficients du modele deviennent exactement nuls. Le modele se compresse tout seul en direct, divisant par vingt son empreinte memoire tout en attribuant un taux d'apprentissage individualise a chaque mot rare du dictionnaire !

---

### Segment 4 : Les Machines de Factorisation et les Croisements Rares

Pour clore cette seance, posons-nous une derniere question :
Un modele lineaire traite chaque variable de facon isolee. Mais dans la vraie vie, c'est l'interaction qui compte : par exemple, un utilisateur sur mobile a minuit ne reagit pas du tout comme un utilisateur sur ordinateur a dix heures du matin.
Comment capturer ces croisements ?

Si vous creez une variable binaire pour chaque paire possible, vous provoquez une explosion combinatoire : un million de variables donne mille milliards de paires. La plupart ne seront jamais observees.
En 2010, Steffen Rendle a invente les Machines de Factorisation (FM) :
Au lieu d'apprendre un coefficient rigide pour chaque paire, on attribue un petit vecteur latent a chaque variable, exactement comme dans le filtrage collaboratif de Netflix !
L'impact d'une interaction devient le produit scalaire entre deux facteurs latents.
Grace a cela, meme si une combinaison precise n'a jamais ete observee dans l'historique, le modele peut estimer son taux de clic avec precision grace aux proximites dans l'espace latent !

---

### Conclusion et Synthese
Pour recapituler ces secrets de maitrise :
1. L'algorithme IRLS prouve que la regression logistique est mathematiquement equivalente a une suite de moindres carres ponderes.
2. En encheres publicitaires temps reel, la calibration exacte des probabilites est une obligation financiere pour eviter les encheres a perte.
3. L'algorithme FTRL-Proximal permet une parcimonie exacte des poids en flux continu avec des taux d'apprentissage adaptatifs.
4. Les Machines de Factorisation concilient l'interpretabilite lineaire et la puissance de capture des interactions croisees a grande echelle.
