# FAQ utilisateur

## Pourquoi un job peut echouer avec 0 lien ?

Ca arrive si la cle de linking est trop faible ou mal mappee, ou si le filtre de classe endpoint est trop restrictif.

## Pourquoi un build apparait mais des jobs tournent encore ?

Un build termine correspond a un run fini. D'autres jobs (autres runs) peuvent etre encore en cours en parallele.

## Difference entre `sameAs` et `property` ?

- `sameAs`: lien direct via URL/identifiant de ressource.
- `property`: alignement via valeur de propriete (ex: IATA, ISBN, telephone).

## Quand utiliser `Ignore align cache` ?

Quand la logique de matching a change (proprietes, filtres, endpoint), pour forcer un nouvel align.

## Comment lire vite la qualite d'un build ?

Regarder:

- nombre de liens,
- distribution des sources de linking,
- coherence de classe,
- presence de fichiers `attr_*` et `rel_*` non vides.
