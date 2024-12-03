SELECT 
  numero_de_recepisse
  , CAST (date_de_depot_de_la_declaration_inscrite_sur_le_recepisse AS DATE) AS date_depot
  , CAST (date_de_validite_du_recepisse_sauf_opposition_de_l_administration AS DATE) AS date_validite
  , categorie
  , type
  , region
    , CONCAT( JSON_EXTRACT_SCALAR(geoloc_cp, '$.lat'), ',', JSON_EXTRACT_SCALAR(geoloc_cp, '$.lon')  ) AS lat_lon
  , nom_du_lieu
  , code_naf_ape
  , departement
  , type_de_declarant
  , code_postal_du_lieu
  , statut_du_recepisse
  , siren_personne_physique_siret_personne_morale
  , raison_sociale_personne_morale_ou_nom_personne_physique
  , code_postal_de_l_etablissement_principal_personne_morale_ou_de_la_personne_physique
  
FROM {{ ref('raw_lic_spec') }}
WHERE statut_du_recepisse = "Valide"