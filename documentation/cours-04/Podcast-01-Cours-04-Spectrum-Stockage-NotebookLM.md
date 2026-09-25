# Source Audio Overview (NotebookLM) — Épisode 7 : Le Spectre des Données, le Stockage Objet Cloud et les Secrets de Spark SQL
## Guide de Discussion & Contenu Pédagogique pour Podcast Audio
**Contexte :** Cours MTI850 (Analytique des données massives), Séance 4, ÉTS Montréal.  
**Sujet de l'épisode :** Explorer la réalité brute des données — Du mythe de la table propre au chaos non structuré, le match HDFS contre AWS S3, la commande `DROP TABLE` qui peut détruire vos données à jamais (Tables gérées vs non gérées), et l'analyse en direct des urgences de San Francisco.

---

### Introduction & Accroche Narrative
Bienvenue dans ce nouvel épisode de notre série consacrée aux données massives. Dans les épisodes précédents, nous avons exploré la mécanique interne d'Apache Spark : son compilateur Catalyst, son moteur Tungsten et sa philosophie de l'évaluation paresseuse.

Mais aujourd'hui, nous nous posons une question fondamentale : **qu'est-ce que nous donnons réellement à manger à ce moteur ?**

Beaucoup de gens s'imaginent que les ingénieurs Big Data passent leurs journées devant de magnifiques tableaux Excel ou des bases de données SQL parfaitement alignées. La réalité est brutale : **plus de 80% des données produites sur Terre sont sales, complexes, semi-structurées ou totalement dépourvues de structure !**

Comment fait-on pour analyser des millions d'appels d'urgence, des flux vidéo de caméras de surveillance, des e-mails ou des tweets ? Pourquoi les entreprises abandonnent-elles leurs systèmes de fichiers classiques pour adopter le stockage objet dans le cloud comme Amazon S3 ? Et quelle est cette commande SQL apparemment anodine qui a déjà coûté des millions de dollars à des entreprises en effaçant par accident l'intégralité de leurs disques durs ?

Préparez vos casques, nous entrons dans le grand spectre des données !

---

### Segment 1 : Le Spectre de la Structure — Du Schéma Rigide au Chaos Pur

Commençons par démonter une illusion : celle de la « donnée tabulaire universelle ».
Dans ce cours, le professeur Koerich nous présente une classification limpide : **le spectre de la structure**.

À gauche du spectre, nous avons les **Données Structurées (*Schema-First*)** :
- Ce sont les tables relationnelles classiques (PostgreSQL, Oracle), les fichiers Parquet, ou les fichiers CSV bien formés.
- La règle d'or ici est : **le schéma d'abord !** Avant d'écrire la moindre ligne, on définit que la colonne A est un entier et la colonne B est du texte. Si une donnée ne rentre pas dans le moule, la base la rejette.
- Le problème ? Elles ne représentent qu'environ 20% du volume mondial, et cette proportion diminue chaque année !

Au milieu du spectre, nous trouvons les **Données Semi-Structurées (*Schema-Later*)** :
- Ce sont les fichiers JSON, XML, HTML, mais aussi les e-mails et les publications sur les réseaux sociaux.
- Prenez un e-mail : les en-têtes (`Expéditeur`, `Destinataire`, `Date`) sont parfaitement structurés. Mais le corps du message est du texte libre !
- Prenez un post sur Facebook ou un tweet : vous avez des compteurs de likes, un horodatage précis, mais le contenu peut être un gif, un texte en slang, un lien web ou une vidéo.
- On dit que ce sont des données **auto-descriptives** : la structure voyage avec la donnée, mais le schéma peut changer d'un enregistrement à l'autre.

Enfin, à droite du spectre, c'est le grand large : les **Données Non Structurées (*Schema-Never*)** :
- Des flux vidéo de caméras de sécurité, des signaux acoustiques, des images satellites, des séquences ADN.
- Aucun schéma n'existe à l'origine. Le rôle des ingénieurs est ce qu'on appelle en anglais le **WYOC** : *Write Your Own Code* (Écrivez votre propre code) pour extraire des caractéristiques numériques et imposer une structure artificielle là où régnait le chaos !

---

### Segment 2 : Le Grand Duel du Stockage — HDFS contre AWS S3

Pour stocker ce déluge de données, les architectures ont vécu une métamorphose radicale au cours de la dernière décennie.

Pendant longtemps, le roi incontesté était **HDFS** (*Hadoop Distributed File System*) :
- Vos fichiers étaient découpés en blocs de 128 Mo, distribués sur les disques de serveurs physiques que votre entreprise possédait dans son propre centre de données.
- C'était rapide parce que le calcul Spark tournait sur les mêmes machines que les disques durs.
- Mais quel casse-tête de maintenance ! Si vous manquiez d'espace disque, il fallait commander de nouveaux serveurs physiques, les installer dans des baies, brancher les câbles et gérer les pannes de matériel.

Et puis est arrivé le **Stockage Objet Cloud** : Amazon S3, Google Cloud Storage (GCS) et Azure Blob Storage.
Ici, tout change :
1. **La fin de la hiérarchie de dossiers :** Sur Amazon S3, les vrais dossiers n'existent pas ! C'est un espace de stockage totalement **plat**. Vous avez un panier géant appelé un **Bucket**, et à l'intérieur, chaque fichier est un objet identifié par une clé textuelle unique. Quand vous voyez `clients/2026/facture.pdf`, le mot `clients/2026/` n'est pas un répertoire : c'est juste un préfixe textuel dans le nom de l'objet !
2. **La promesse des 11 Neufs de durabilité :** AWS garantit une durabilité de 99,999999999% pour vos données sur S3. Pour perdre un fichier, il faudrait qu'une pluie de météorites anéantisse simultanément plusieurs centres de données indépendants situés dans des villes différentes !
3. **L'élasticité absolue :** Plus besoin de prévoir la taille de vos disques. Vous stockez 1 gigaoctet aujourd'hui et 10 pétaoctets demain, et vous ne payez qu'à la seconde pour ce que vous consommez réellement.

---

### Segment 3 : La Bombe à Retardement de Spark SQL — Tables Gérées vs Non Gérées

Passons maintenant à l'une des leçons d'ingénierie les plus cruciales de tout le cours MTI850.
Dans Spark SQL, quand vous enregistrez un DataFrame sous forme de table, vous avez deux options : une **Table Gérée (*Managed Table*)** ou une **Table Non Gérée (*Unmanaged / External Table*)**.

La différence a l'air purement théorique, mais elle peut sauver ou ruiner votre carrière !

#### Le piège de la Table Gérée (*Managed*)
Quand vous écrivez :
```python
df.write.saveAsTable("ma_table_clients")
```
Spark crée une table gérée. Cela signifie que Spark prend la responsabilité de tout : il enregistre les métadonnées dans son catalogue Hive, et il copie les données physiques dans son répertoire système interne.
Mais voici le drame : si un matin, un analyste se dit : « Tiens, je vais nettoyer cette table temporaire » et tape la commande :
```sql
DROP TABLE ma_table_clients;
```
**Spark efface instantanément le catalogue ET TOUS LES FICHIERS PHYSIQUES SUR LE DISQUE DUR !**
Vos téraoctets de données disparaissent dans le néant. Si vous n'avez pas de sauvegarde externe, vous venez de détruire des mois de travail !

#### La sécurité de la Table Non Gérée (*External Table*)
En production sérieuse, on utilise presque toujours des tables externes :
```python
df.write.option("path", "s3://mon-bucket/donnees_clients/").saveAsTable("ma_table_clients")
```
Ici, la table pointe vers un emplacement de stockage persistant externe.
Si quelqu'un exécute `DROP TABLE ma_table_clients` par erreur, **seul le pointeur de métadonnées est effacé !** Vos précieux fichiers Parquet restent parfaitement en sécurité sur votre stockage S3. Il suffit de réenregistrer la table pour que tout réapparaisse en une seconde. Retenez bien cette distinction pour vos futurs examens et votre pratique professionnelle !

---

### Segment 4 : Vues Locales contre Vues Globales — Qui peut voir mes données ?

Dans le même esprit de gouvernance des données, Spark SQL propose deux manières de créer des vues temporaires sans dupliquer les octets :

1. **La Vue Temporaire Locale (`createOrReplaceTempView`) :**
   - Elle est attachée exclusivement à votre `SparkSession` courante.
   - Si vous travaillez dans un notebook Jupyter et qu'un collègue ouvre une autre session Spark dans la même application, il ne pourra pas voir votre vue. Dès que votre session se ferme, la vue s'évapore.
2. **La Vue Temporaire Globale (`createOrReplaceGlobalTempView`) :**
   - Elle est partagée entre toutes les sessions Spark actives de votre application.
   - Mais attention à son accès : pour la requêter en SQL, il faut obligatoirement préfixer son nom par la base de données réservée `global_temp`, par exemple :
   ```sql
   SELECT * FROM global_temp.vols_en_retard_sfo;
   ```
   C'est la technique reine pour partager des tables de travail en mémoire entre plusieurs modules d'un pipeline sans recalculer les transformations.

---

### Segment 5 : Du Format Brut à l'Action — Les Pompiers de San Francisco

Pour ancrer toute cette théorie dans la pratique, le laboratoire du cours 04 nous plonge dans un cas d'étude réel et volumineux : l'historique complet des appels d'urgence des pompiers de San Francisco (`sf-fire-calls.csv`).

Que fait un ingénieur de données face à ce fichier brut de plusieurs centaines de mégaoctets ?
1. **Bannir l'inférence automatique de schéma :** Si vous écrivez `inferSchema=True`, Spark doit lire l'intégralité du fichier CSV deux fois : une première fois pour deviner si chaque colonne est un entier ou du texte, et une deuxième fois pour charger les données. Sur un fichier de 500 Go, c'est une perte de temps intolérable !
2. **Définir un schéma DDL ou `StructType` strict :** On impose immédiatement le typage : `CallNumber` en entier, `UnitID` en chaîne, `Delay` en nombre flottant.
3. **Le casse-tête des dates :** Dans le fichier brut, la date de l'incendie n'est qu'une chaîne de caractères comme `"01/15/2024"`. Spark ne peut pas savoir quel jour de la semaine c'était !
   - On applique la transformation `to_timestamp(col("CallDate"), "MM/dd/yyyy")`.
   - Et instantanément, on débloque toute la boîte à outils temporelle de Spark : `year()`, `month()`, `dayofweek()`.
   - En deux lignes de `groupBy("CallType").count().orderBy("count", ascending=False)`, on découvre en quelques millisecondes que la majorité des interventions ne sont pas des incendies géants, mais des urgences médicales de quartier !

---

### Conclusion & Synthèse
Pour clore ce quatrième épisode riche en révélations :
1. Les données du monde réel vivent sur un spectre : des tables structurées aux flux multimédias non structurés, en passant par la flexibilité des documents semi-structurés (JSON, e-mails).
2. Le stockage moderne a déplacé le centre de gravité vers le stockage objet cloud (S3, GCS, Azure Blob) grâce à son élasticité infinie, son coût à l'usage et sa durabilité quasi-parfaite.
3. Dans Spark SQL, maîtriser la différence entre Tables Gérées et Non Gérées est une question de survie pour vos données.
4. Enfin, transformer de la donnée brute en information exploitable exige de bannir l'inférence paresseuse au profit de schémas explicites et de transformations vectorisées rigoureuses.
