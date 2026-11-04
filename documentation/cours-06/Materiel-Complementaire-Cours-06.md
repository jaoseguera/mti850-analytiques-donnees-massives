# MTI850 -- Analytique des donnees massives
## Materiel Complementaire Avance : Optimisation Convexe, Algorithmes L-BFGS Distribues et Modeles de Communication Reseau

Complement au Cours 06 : Approfondissement technique de niveau Maitrise  
Auteur / Reference d'etude : Analyse avancee pour l'ingenierie des donnees massives (ETS Montreal)

---

## 1. Theoriematique de Convergence et Optimisation Convexe

Pour concevoir des algorithmes robustes a grande echelle, il est indispensable de comprendre la geometrie de la fonction objectif f(w).

### 1.1 Gradient L-Lipschitzien
Une fonction differentiable f : R^d -> R possede un gradient L-Lipschitzien s'il existe une constante L > 0 telle que :
||nabla f(w_1) - nabla f(w_2)||_2 <= L * ||w_1 - w_2||_2,  pour tous w_1, w_2
Cette propriete garantit que la courbure de la fonction ne varie pas de maniere infinie. Par le lemme de descente :
f(w - alpha * nabla f(w)) <= f(w) - alpha * (1 - (alpha * L) / 2) * ||nabla f(w)||_2^2

Condition sur le taux d'apprentissage :
Si le pas d'iteration respecte 0 < alpha < 2 / L (typiquement alpha = 1 / L), la valeur de la fonction de perte decroit de facon strictement monotone a chaque iteration :
f(w^(t+1)) <= f(w^(t)) - (1 / (2 L)) * ||nabla f(w^(t))||_2^2

### 1.2 Taux de Convergence Comparatifs
- Fonction convexe standard : La descente de gradient atteint une precision epsilon (f(w^(t)) - f(w^*) <= epsilon) en O(1 / epsilon) iterations (taux sous-lineaire O(1 / t)).
- Fonction mu-fortement convexe (ex. regression Ridge avec lambda > 0) :
  Il existe mu > 0 tel que f(w_2) >= f(w_1) + nabla f(w_1)^T (w_2 - w_1) + (mu / 2) * ||w_2 - w_1||_2^2.
  La convergence devient lineaire (au sens de l'analyse numerique, c'est-a-dire exponentiellement rapide) :
  ||w^(t) - w^*||_2 <= (1 - mu / L)^t * ||w^(0) - w^*||_2
  On atteint une precision epsilon en O(log(1 / epsilon)) iterations !
  
> [!NOTE]
> C'est une justification mathematique fondamentale de la regularisation Ridge (L2) : en ajoutant lambda * ||w||_2^2, on rend la fonction mu-fortement convexe avec mu = 2 * lambda, ce qui accelere drastiquement la convergence de la descente de gradient !

---

## 2. Au-dela du Premier Ordre : Pourquoi Spark MLlib Utilise L-BFGS

La descente de gradient standard souffre d'oscillations des que les courbes de niveau sont etirees (mauvais conditionnement). Les methodes de second ordre (Newton-Raphson) exploitent la matrice Hessienne H = nabla^2 f(w) pour effectuer des pas optimaux :
w^(t+1) = w^(t) - H^(-1) * nabla f(w^(t))

### 2.1 L'Impossibilite de Newton Pur a Grande Echelle
- Stocker la Hessienne H demande O(d^2) espace memoire. Pour d = 100 000, H compte 10^10 elements (~80 Go de RAM pour une seule matrice).
- Inverser H demande O(d^3) calculs a chaque iteration.

### 2.2 L-BFGS (Limited-Memory Broyden-Fletcher-Goldfarb-Shanno)
L'algorithme L-BFGS approxime directement l'inverse de la Hessienne sans jamais materialiser de matrice complete en memoire :
- On conserve uniquement les m derniers vecteurs de deplacements :
  s_k = w^(k+1) - w^(k)  et  y_k = nabla f(w^(k+1)) - nabla f(w^(k))
  ou m est un petit hyperparametre (typiquement m = 5 a 20).
- Empreinte memoire : O(m d), ce qui est strictement lineaire en d !
- Direction de descente : Calculee par un algorithme a deux boucles (two-loop recursion) tres efficace en temps O(m d).

Dans Spark MLlib, `LogisticRegression` et `LinearRegression` utilisent L-BFGS comme solveur par defaut car il offre la vitesse de convergence quasi-quadratique des methodes de second ordre avec l'empreinte memoire legere du premier ordre.

---

## 3. Topologies et Modeles de Communication pour l'Entrainement Distribue

Le transfert de gradients entre noeuds obeit a des modeles formels de communication reseau.

### 3.1 Modele de Communication LogP / Alpha-Beta
Le temps de transfert d'un message de taille S octets sur le reseau se modelise par :
T_comm = alpha + beta * S
ou :
- alpha represente la latence fixe d'etablissement de connexion (overhead TCP/IP).
- beta represente l'inverse de la bande passante (secondes par octet).

### 3.2 Comparaison des Topologies d'Agregation
1. Driver Centralise (Client-Serveur) :
   - Tous les k executeurs envoient leur gradient partiel (taille d) au Driver.
   - Trafic entrant sur le Driver : O(k * d).
   - Goulot d'etranglement reseau fatal des que k > 50 executeurs.
2. Agregation en Arbre (Tree-Aggregate dans Spark) :
   - Les executeurs fusionnent leurs gradients localement par groupes hierarchiques.
   - Temps total : O(log k * (alpha + beta * d)).
   - Evite la saturation de la carte reseau du Driver.
3. Ring All-Reduce (utilise dans PyTorch / Horovod / Deep Learning) :
   - Les noeuds sont disposes en anneau logique. Chaque noeud echange des blocs de taille d / k avec son voisin direct.
   - Temps total : 2 * (k - 1) * alpha + 2 * ((k - 1) / k) * beta * d.
   - Independant du nombre de noeuds pour le terme de bande passante !

---

## 4. Asynchronisme et Probleme des Gradients Perimes (Stale Gradients)

Pour eviter que les serveurs rapides n'attendent les serveurs lents (stragglers), certaines architectures utilisent la descente asynchrone (Hogwild! / Parameter Server asynchrone) :
- Les executeurs lisent les poids w, calculent un gradient local, et l'appliquent au serveur sans barriere de synchronisation.
- Probleme : Quand un executeur lent envoie son gradient g(w^(t)), les parametres globaux ont deja progresse vers w^(t + tau).
- Le retard tau (staleness) injecte une derive qui peut empecher la convergence ou rendre le modele instable si le taux d'apprentissage alpha n'est pas reduit en proportion (alpha_t = alpha / sqrt(1 + tau)).

C'est pourquoi Spark MLlib privilegie le modele Bulk Synchronous Parallel (BSP), mathematiquement plus rigoureux et deterministe pour la convergence sur donnees massives.
