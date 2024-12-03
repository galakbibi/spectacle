/*
Table qui permet de regrouper les datasets fragmentés 2020 à 2024
A faire ?
    vérifier les sources et les fichiers de destination (cf doc + formation)
    remplacer les noms de table en format jinja ??
    choisir une méthode entre:
        la réécriture de la table lic_spec_full dans le dossier spectacle
        l'écriture d'une table dans le dossier dbt
    inclure airbyte_extracted pour avoir une date de l'ingestion
    caster les dates

Commentaires : 
    table , avec uniquement les personnes morales
    NB inclure 2025


*/


WITH lic_2020 AS (
  SELECT 
  _airbyte_extracted_at
    , numero_de_recepisse
  , date_de_depot_de_la_declaration_inscrite_sur_le_recepisse
  , date_de_validite_du_recepisse_sauf_opposition_de_l_administration
  , categorie
  , type
  , region
    , geoloc_cp
  , nom_du_lieu
  , code_naf_ape
  , departement
  , type_de_declarant
  , code_postal_du_lieu
  , statut_du_recepisse
  , siren_personne_physique_siret_personne_morale
  , raison_sociale_personne_morale_ou_nom_personne_physique
  , code_postal_de_l_etablissement_principal_personne_morale_ou_de_la_personne_physique
  
  FROM licences-spectacles.spectacle.lic_spec_2020
  WHERE type_de_declarant = "Personne morale"
)
,
lic_2021 AS (
  SELECT _airbyte_extracted_at
    , numero_de_recepisse
  , date_de_depot_de_la_declaration_inscrite_sur_le_recepisse 
  , date_de_validite_du_recepisse_sauf_opposition_de_l_administration 
  , categorie
  , type
  , region
    , geoloc_cp
  , nom_du_lieu
  , code_naf_ape
  , departement
  , type_de_declarant
  , code_postal_du_lieu
  , statut_du_recepisse
  , siren_personne_physique_siret_personne_morale
  , raison_sociale_personne_morale_ou_nom_personne_physique
  , code_postal_de_l_etablissement_principal_personne_morale_ou_de_la_personne_physique

  FROM licences-spectacles.spectacle.lic_spec_2021
  WHERE type_de_declarant = "Personne morale"
)
,
lic_2022 AS (
  SELECT _airbyte_extracted_at
    , numero_de_recepisse
  , date_de_depot_de_la_declaration_inscrite_sur_le_recepisse 
  , date_de_validite_du_recepisse_sauf_opposition_de_l_administration 
  , categorie
  , type
  , region
    , geoloc_cp
  , nom_du_lieu
  , code_naf_ape
  , departement
  , type_de_declarant
  , code_postal_du_lieu
  , statut_du_recepisse
  , siren_personne_physique_siret_personne_morale
  , raison_sociale_personne_morale_ou_nom_personne_physique
  , code_postal_de_l_etablissement_principal_personne_morale_ou_de_la_personne_physique
 
  FROM licences-spectacles.spectacle.lic_spec_2022
  WHERE type_de_declarant = "Personne morale"
)
,
lic_2023 AS (
  SELECT _airbyte_extracted_at
    , numero_de_recepisse
  , date_de_depot_de_la_declaration_inscrite_sur_le_recepisse
  , date_de_validite_du_recepisse_sauf_opposition_de_l_administration
  , categorie
  , type
  , region
    , geoloc_cp
  , nom_du_lieu
  , code_naf_ape
  , departement
  , type_de_declarant
  , code_postal_du_lieu
  , statut_du_recepisse
  , siren_personne_physique_siret_personne_morale
  , raison_sociale_personne_morale_ou_nom_personne_physique
  , code_postal_de_l_etablissement_principal_personne_morale_ou_de_la_personne_physique
  
  FROM licences-spectacles.spectacle.lic_spec_2023
  WHERE type_de_declarant = "Personne morale"
  )
,
lic_2024 AS (
  SELECT _airbyte_extracted_at
    , numero_de_recepisse
  , date_de_depot_de_la_declaration_inscrite_sur_le_recepisse
  , date_de_validite_du_recepisse_sauf_opposition_de_l_administration
  , categorie
  , type
  , region
    , geoloc_cp
  , nom_du_lieu
  , code_naf_ape
  , departement
  , type_de_declarant
  , code_postal_du_lieu
  , statut_du_recepisse
  , siren_personne_physique_siret_personne_morale
  , raison_sociale_personne_morale_ou_nom_personne_physique
  , code_postal_de_l_etablissement_principal_personne_morale_ou_de_la_personne_physique
  
  FROM licences-spectacles.spectacle.lic_spec_2024
  WHERE type_de_declarant = "Personne morale"
)
,
final AS (
  SELECT * FROM lic_2020
  UNION ALL
  SELECT * FROM lic_2021
  UNION ALL
  SELECT * FROM lic_2022
  UNION ALL
  SELECT * FROM lic_2023
  UNION ALL
  SELECT * FROM lic_2024
)

SELECT * FROM final
QUALIFY ROW_NUMBER() OVER (PARTITION BY numero_de_recepisse ORDER BY _airbyte_extracted_at DESC) = 1
ORDER BY date_de_depot_de_la_declaration_inscrite_sur_le_recepisse DESC

/*
ORDER BY numero_de_recepisse
*/