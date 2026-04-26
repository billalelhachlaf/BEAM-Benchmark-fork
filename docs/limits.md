# Limites actuelles de BEAM-App

## 1. Limites donnees

- Donnees WDC heterogenes et bruit (format, langue, schemas variables).
- Presence de blank nodes et graphes profonds pouvant complexifier l'extraction.
- Certaines classes ont peu de proprietes discriminantes.

## 2. Limites endpoints

- Endpoints SPARQL soumis a timeouts, quotas, throttling.
- DBpedia/YAGO peuvent avoir couverture/type differents de Wikidata.
- Disponibilite reseau externe non garantie.

## 3. Limites performance

- Jobs lourds sur grandes classes (`all parts`) peuvent etre longs.
- Consommation disque importante (`Download/`, `data/`, cache).
- SAKEY peut etre couteux en CPU/RAM sur gros volumes.

## 4. Limites fonctionnelles

- SAKEY Explorer est assist-only: pas d'application auto au pipeline.
- La qualite du linking depend fortement des cles et mappings fournis.
- Certains runs legacy peuvent ne pas contenir toutes les stats recentes.

## 5. Contournements recommandes

- Iterer d'abord sur petits subsets (`parts_spec` reduit).
- Utiliser des regles OR (`sameAs` + property) pour augmenter rappel/precision.
- Prioriser des proprietes robustes (codes normalises, identifiants stables).
- Monitorer et nettoyer regulierement jobs/builds.
