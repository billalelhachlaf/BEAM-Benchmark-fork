# Recettes utilisateur (pratiques)

## 1. Run de validation rapide

Objectif: valider config sans cout eleve.

- `parts_spec`: petit subset (`0`, `0-1`).
- endpoint: `wikidata`.
- mode: `property` avec une cle robuste.

## 2. Augmenter le rappel (OU)

Utiliser des regles OR:

- `sameAs` OU `property`.
- Ex: `sameAs =>` + `telephone => P1329`.

## 3. Stabiliser precision

- Ajouter filtre de classe endpoint (`target_class`).
- Garder normalisation caractere (`ignore_chars`) coherent.

## 4. Quand relancer sans cache

Utiliser `Ignore align cache` si:

- changement de patterns,
- changement endpoint/target class,
- changement logique de mapping.

## 5. Diagnostic faible nb de liens

Verifier:

- valeur de cle presente cote WDC,
- propriete equivalente cote endpoint,
- filtre de classe trop strict,
- biais sur parts inadaptées.
