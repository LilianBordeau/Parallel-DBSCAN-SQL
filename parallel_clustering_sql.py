# Lilian Bordeau - Septembre 2021 - SFR
# SFR - Équipe Data et Analyse
# Traitement 2092 - Segmentation des transactions pour la sécurisation des ventes

import teradatasql
import datetime
import time

# Connexion
host     = ''
user     = ''
password = ''
url      = '{"host":"'+host+'","user":"'+user+'","password":"'+password+'"}'

# Répertoire SQL
repertoire = r'SQL/'

# Données
table_set_1 = 'XXX'
table_set_2 = 'XXX'

# Dates
date_min_set_1 = '2021-09-01'
date_max_set_1 = 'CURRENT_DATE'
date_min_set_2 = '2021-09-01'
date_max_set_2 = 'CURRENT_DATE'

# Hyperparametres
distance = 3 # epsilon (distance minimale entre 2 points pour appartenir au même cluster)
MinPts   = 2 # MinPts (nombre de points minimum dans le voisinage d'un point pour appartenir au meme cluster)
Min_Common_Pts = 1 # Nombre de points en commun minimum pour fusionner deux cellules

# Connexion
connexion = teradatasql.connect(url)
curseur   = connexion.cursor()

# Correspondance données / features engineering / threshold distances

mapping_data = {
    'id_commande':{
        'set_1': 'ORDER_PACKAGE_NUMBER',
        'set_2': 'ORDER_PACKAGE_NUMBER',
        'join': False
    },
    'type_commande':{
        'set_1': 'CONCAT_TYPE_ORDER',
        'set_2': 'CONCAT_TYPE_ORDER',
        'join': False
    },
    'date_commande':{
        'set_1': 'RECORDED_DATE',
        'set_2': 'RECORDED_DATE',
        'join': False
    },
    'prenom':{
        'set_1': 'TITULAIRE_FIRST_NAME',
        'set_2': 'TITULAIRE_FIRST_NAME',
        'join': False
    },
    'nom':{
        'set_1': 'TITULAIRE_LAST_NAME',
        'set_2': 'TITULAIRE_LAST_NAME',
        'join': False
    },
    'type_livraison':{
        'set_1': 'LOGISTICS_DELIVERY_MODE',
        'set_2': 'LOGISTICS_DELIVERY_MODE',
        'join': False
    },
    'prenom_nom':{
        'set_1': '(TITULAIRE_FIRST_NAME || TITULAIRE_LAST_NAME)',
        'set_2': '(TITULAIRE_FIRST_NAME || TITULAIRE_LAST_NAME)',
        'join': True,
        'where': """prenom_nom IS NOT NULL
                  and prenom_nom NOT LIKE '%SFR TEST%'""",
        'seuil': '3'
    },
    'prenom_nom_inv':{
        'set_1': '(TITULAIRE_LAST_NAME || TITULAIRE_FIRST_NAME)',
        'set_2': '(TITULAIRE_FIRST_NAME || TITULAIRE_LAST_NAME)',
        'join': True,
        'where': """prenom_nom_inv IS NOT NULL
                  and prenom_nom_inv NOT LIKE '%SFR TEST%'""",
        'seuil': '3'
    },
    'adresse_postale_facturation':{
        'set_1': '(TITULAIRE_ADDRESS || \' - \' || TITULAIRE_ZIP_CODE)',
        'set_2': '(TITULAIRE_ADDRESS || \' - \' || TITULAIRE_ZIP_CODE)',
        'join': True,
        'where': """adresse_postale_facturation IS NOT NULL
                    and adresse_postale_facturation not in (SELECT adresse_postale_facturation FROM TOP_20)
                    and adresse_postale_facturation NOT IN ('16 R DU GEN A DE BOISSIEU - 75015',
                    '83 BOULEVARD DU REDON - 13009',
                    '16 R DU GEN A DE BOISSIEU - 75015',
                    '71 RUE ARCHEREAU - 75019',
                    '12 RUE J-PHILIPPE RAMEAU - 93634',
                    'R DU GEN A DE BOISSIEU - 75015',
                    '83 BOULEVARD DU REDON - 13009',
                    '16 R DU GEN A DE BOISSIEU - 75741',
                    '102 AVENUE DE PARIS - 91300',
                    '12 RUE J PHILIPPE RAMEAU - 93634',
                    '38 CHEMIN DE LA BIGOTTE - 13015',
                    '48 AVENUE FOURNACLE - 13013',
                    '54 AVENUE DU RAY - 06100'
                    )""",
        'seuil': '6'
    },
    'adresse_postale_livraison':{
        'set_1': '(ADDRESS || \' - \' || ZIPCODE)',
        'set_2': '(ADDRESS || \' - \' || ZIPCODE)',
        'join': True,
        'where': """adresse_postale_livraison IS NOT NULL
                    and type_livraison NOT IN ('ESPACE_SFR_DELIVERY', 'ESPACE_SFR_WITHDRAWAL')
                    and adresse_postale_livraison not in (SELECT adresse_postale_livraison FROM TOP_20)
                    and adresse_postale_livraison NOT IN ('16 R DU GEN A DE BOISSIEU - 75015',
                    '83 BOULEVARD DU REDON - 13009')""",
        'seuil': '6'
    },
    'adresse_electronique':{
        'set_1': 'TITULAIRE_EMAIL',
        'set_2': 'TITULAIRE_EMAIL',
        'join': False
    },
    'corps_adresse_electronique':{
        'set_1': 'StrTok(TITULAIRE_EMAIL, \'@\', 1)',
        'set_2': 'StrTok(TITULAIRE_EMAIL, \'@\', 1)',
        'join': True,
        'where': """corps_adresse_electronique IS NOT NULL
                    and canal_fin_vente <> 'DIST'
                    and Length(corps_adresse_electronique) > 3
                    and corps_adresse_electronique not in (SELECT corps_adresse_electronique FROM TOP_20)
                    and corps_adresse_electronique NOT IN ('sfr', 'contact', 'nomail', 'pasdemail', 'noreply', 'noadresse', 'noadress')""",
        'seuil': '3'
    },
    'telephone_mobile':{
        'set_1': 'TITULAIRE_PHONE_NUMBER',
        'set_2': 'TITULAIRE_PHONE_NUMBER',
        'join': True,
        'where': """telephone_mobile IS NOT NULL
                    and telephone_mobile not in (SELECT telephone_mobile FROM TOP_20)
                    and telephone_mobile not in ('0600000000', '0601020304', '0600000001', '0612345678', '0601010101')
                    and canal_fin_vente <> 'DIST'
                    and length(telephone_mobile) = 10""",
        'seuil': '2'
    },
    'telephone_fixe':{
        'set_1': 'CONTRACT_NDI',
        'set_2': 'CONTRACT_NDI',
        'join': True,
        'where': """telephone_fixe IS NOT NULL
                    and telephone_fixe not in (SELECT telephone_fixe FROM TOP_20)
                    and telephone_fixe <> '0000000000'
                    and canal_fin_vente <> 'DIST'
                    and length(telephone_fixe) = 10""",
        'seuil': '2'
    },
    'iban':{
        'set_1': 'IBAN',
        'set_2': 'IBAN',
        'join': True,
        'where': """iban IS NOT NULL 
                    and iban not in (SELECT iban FROM TOP_20)
                    and iban <> 'FR7630004008190001109089261'""",
        'seuil': '4'
    },
    'canal_debut_vente':{
        'set_1': 'INIT_SALES_CONTEXT_SALES_CH',
        'set_2': 'INIT_SALES_CONTEXT_SALES_CH',
        'join': False
    },
    'canal_fin_vente':{
        'set_1': 'END_SALES_CONTEXT_SALES_CH',
        'set_2': 'END_SALES_CONTEXT_SALES_CH',
        'join': False
    },
    'code_point_vente':{
        'set_1': 'CODE_POINT_VENTE',
        'set_2': 'CODE_POINT_VENTE',
        'join': False
    },
    'id_point_livraison':{
        'set_1': 'DELIVERY_POINT_ID',
        'set_2': 'DELIVERY_POINT_ID',
        'join': False,
        'where': 'id_point_livraison IS NOT NULL',
        'seuil': '0'
    },
    'epropal_provider':{
        'set_1': 'StrTok(EPROPAL_EMAIL, \'@\', 2)',
        'set_2': 'StrTok(EPROPAL_EMAIL, \'@\', 2)',
        'join': False
    },
    'email_proposition_commerciale':{
        'set_1': 'StrTok(EPROPAL_EMAIL, \'@\', 1)',
        'set_2': 'StrTok(EPROPAL_EMAIL, \'@\', 1)',
        'join': True,
        'where': """email_proposition_commerciale IS NOT NULL
                    and Length(email_proposition_commerciale) > 3
                    and epropal_provider not like '%teleperformance%'
                    and email_proposition_commerciale not in (SELECT email_proposition_commerciale FROM TOP_20)
                    and NOT(email_proposition_commerciale LIKE ANY ('%sfr%', '%adc%', '%hassan.mranji%', 
                    '%ghizlane.taibi%', '%mohammedmoncef.doulaf%'))""",
        'seuil': '3'
    },
    'msisdn_proposition_commerciale':{
        'set_1': 'EPROPAL_MSISDN',
        'set_2': 'EPROPAL_MSISDN',
        'join': True,
        'where': """msisdn_proposition_commerciale IS NOT NULL 
                    AND msisdn_proposition_commerciale <> 'NONE'
                    and msisdn_proposition_commerciale not in (SELECT msisdn_proposition_commerciale FROM TOP_20)
                    AND msisdn_proposition_commerciale NOT IN ('0600000000', '0601020304', '0600000001', '0612345678', '0601010101')""",
        'seuil': '2'
    },
    'id_dossier_demat':{
        'set_1': 'DEMAT_ID',
        'set_2': 'DEMAT_ID',
        'join': False,
        'where': 'id_dossier_demat IS NOT NULL AND id_dossier_demat <> \'0\'',
        'seuil': '2'
    },
    'ip_contexte_debut':{
        'set_1': 'INIT_CTXT_IP_TITULAIRE',
        'set_2': 'INIT_CTXT_IP_TITULAIRE',
        'join': False
    },
    'ip_contexte_fin':{
        'set_1': 'END_CTXT_IP_TITULAIRE',
        'set_2': 'END_CTXT_IP_TITULAIRE',
        'join': False
    }
}

# create volatile table data

volatile_data_table = """
CREATE MULTISET VOLATILE TABLE """+user+"""._2092_CLUSTER_DATA AS (
SELECT """+",\n".join([value['set_2']+' AS '+key for key, value in mapping_data.items()])+"""
FROM """+table_set_1+""" T1
WHERE concat_type_order IN ('CONQUETE', 'VLA', 'FIXE_CONQUETE', 'FIXE_VLA', 'RM', 'DEVICE')
AND CAST(RECORDED_DATE AS DATE) BETWEEN """+date_min_set_2+""" AND """+date_max_set_1+"""
QUALIFY Count(*) Over (PARTITION BY id_commande) = 1
) WITH DATA UNIQUE PRIMARY INDEX (id_commande) ON COMMIT PRESERVE ROWS;"""

#print(volatile_data_table)
with connexion.cursor() as cur:
    cur.execute(volatile_data_table)
    connexion.commit()

# Collect statistics

with connexion.cursor() as cur:
    for key, value in mapping_data.items():
        print(key)
        cur.execute("""COLLECT STATISTICS COLUMN("""+key+""") ON """+user+"""._2092_CLUSTER_DATA;""")
        connexion.commit()

# Sous requête set1, set2

subquery_set_1 = """SELECT * FROM """+user+"""._2092_CLUSTER_DATA T1 
                    WHERE CAST(date_commande AS DATE) BETWEEN """+date_min_set_1+""" AND """+date_max_set_1+""" 
                    AND CONDITION_WHERE"""
subquery_set_2 = """SELECT * FROM """+user+"""._2092_CLUSTER_DATA T1 
                    WHERE CAST(date_commande AS DATE) BETWEEN """+date_min_set_2+""" AND """+date_max_set_2+""" 
                    AND CONDITION_WHERE"""

# Création de la table de distance
print(datetime.datetime.now().strftime('%H:%M'), 'Création de la table de distance')

liste_features_distance = [key for key, value in mapping_data.items() if value['join']]

query = open(repertoire + 'Create_Volatile_Table_Pairwise_Distances.sql', 'r').read()
query = query.replace('USER_LOGIN', user)

dist_edit = ['dist_'+x+' BYTEINT' for x in liste_features_distance if x != 'prenom_nom_inv']
norm_edit = ['norm_'+x+' FLOAT' for x in liste_features_distance if x != 'prenom_nom_inv']
bool_edit = ['bool_'+x+' BYTEINT' for x in liste_features_distance if x != 'prenom_nom_inv']
alls_edit = ',\n'.join(dist_edit + norm_edit + bool_edit + ['DISTANCE_MAX BYTEINT'])

query = query.replace('EDIT_DISTANCES',alls_edit)

#print(query)

with connexion.cursor() as cur:
        cur.execute(query)
        connexion.commit()

# Calcul et insertion des distances dans la table
print(datetime.datetime.now().strftime('%H:%M'), 'Calcul et insertion des distances dans la table')

query_pairwise_distances = open(repertoire + 'Insert_Into_Pairwise_Distances.sql', 'r').read()
query_pairwise_distances = query_pairwise_distances.replace('USER_LOGIN', user)

sql_edit_dist = ['EditDistance(Upper(SET_1.'+feat+'), Upper(SET_2.'+feat+')) AS edit_'+feat for feat in liste_features_distance if feat not in ('prenom_nom', 'prenom_nom_inv')]
sql_edit_nomp = ['LEAST(EditDistance(Upper(SET_1.prenom_nom), Upper(SET_2.prenom_nom)), EditDistance(Upper(SET_1.prenom_nom_inv), Upper(SET_2.prenom_nom_inv))) AS edit_prenom_nom']
sql_norm_dist = ['CAST(edit_'+feat+' AS FLOAT) / CAST(GREATEST(LENGTH(SET_1.'+feat+'), LENGTH(SET_2.'+feat+')) AS FLOAT) AS norm_'+feat for feat in liste_features_distance if feat != 'prenom_nom_inv']
sql_bool_dist = ['(CASE WHEN (edit_'+key+' < '+value['seuil']+') OR edit_'+key+' IS NULL THEN 0 ELSE 1 END) AS bool_'+key for key, value in mapping_data.items() if value['join'] and key != 'prenom_nom_inv']
sql_dist_maxm = ['+'.join(['(CASE WHEN SET_1.'+feat+' IS NOT NULL AND SET_2.'+feat+' IS NOT NULL THEN 1 ELSE 0 END)' for feat in liste_features_distance if feat != 'prenom_nom_inv']) + ' AS DISTANCE_MAX']
sql_alls_dist = sql_edit_nomp + sql_edit_dist + sql_bool_dist + sql_dist_maxm + sql_norm_dist 
max_distance  = len(liste_features_distance)

t1 = time.time()

tmp_card = 0

for key, value in mapping_data.items():
    if value['join']:

        condition_jointure = 'SET_1.'+key+'=SET_2.'+key
        query = query_pairwise_distances
        query = query.replace('QUERY_SET_1', subquery_set_1)
        query = query.replace('QUERY_SET_2', subquery_set_2)
        query = query.replace('QUERY_FEATURE_DISTANCE', ",\n".join(sql_alls_dist))
        query = query.replace('CONDITION_JOINTURE', condition_jointure)
        query = query.replace('CONDITION_WHERE', value['where'])
        query = query.replace('ID_SET_1', 'id_commande')
        query = query.replace('ID_SET_2', 'id_commande')
        query = query.replace('KEY', key)
        
        with connexion.cursor() as cur:
            cur.execute(query)
            connexion.commit()

        t2 = time.time() - t1
        t1 = time.time()
        print('\t-', key,':', int(t2), 'secondes (execution)')

# rajoute une colonne avec le distance (en terme de nb de champs en commun)

bools = ['bool_'+x for x in liste_features_distance if x != 'prenom_nom_inv']

query = """CREATE TABLE DB_DATALAB_DAF.ML_CLUSTERS_PAIRWISE_DIST AS (
                    SELECT T1.*,
                    ("""+'+'.join(bools)+""") AS BOOL_TOT,
                    (DISTANCE_MAX - BOOL_TOT) as CHAMPS_COMMUN,
                    CAST(BOOL_TOT AS FLOAT) / CAST(DISTANCE_MAX AS FLOAT) as DISTANCE_NORM
                    FROM u165983._2092_PAIRWISE_DISTANCES T1
                    )  WITH DATA UNIQUE PRIMARY INDEX (ID1,ID2);"""

#print(query)
with connexion.cursor() as cur:
        cur.execute(query)
        connexion.commit()

# Table résultat

query1 = """
CREATE MULTISET TABLE DB_DATALAB_DAF.ML_CLUSTERS
(
  CELLULE INTEGER,
  COMMANDE VARCHAR(255) CHARACTER SET Latin NOT CaseSpecific,
  PROFONDEUR INTEGER
) PRIMARY INDEX (CELLULE, COMMANDE);
"""

# Initialise avec les points centraux (core points)
# Avec un voisinage dense défini par MinPts
# Et une distance comprise entre le seuil et inférieure à la distance maximum

query2 = """
INSERT INTO DB_DATALAB_DAF.ML_CLUSTERS

WITH R1 AS (SELECT DISTINCT
T1.ID1 AS COMMANDE,
0 AS PROFONDEUR
FROM DB_DATALAB_DAF.ML_CLUSTERS_PAIRWISE_DIST T1
WHERE CHAMPS_COMMUN BETWEEN """+str(distance)+""" AND (DISTANCE_MAX - 1)
QUALIFY Count(*) Over(PARTITION BY id1) >= """+str(MinPts)+""")

SELECT Dense_Rank() Over (ORDER BY COMMANDE ASC) AS CELLULE,
    COMMANDE,
    PROFONDEUR
FROM R1
GROUP BY 2,3
"""

with connexion.cursor() as cur:
        cur.execute(query1)
        connexion.commit()
        cur.execute(query2)
        connexion.commit()
        
sql_nb_points = """SELECT Count(*) FROM DB_DATALAB_DAF.ML_CLUSTERS"""

nb_points = 0

with connexion.cursor() as cur:
    cur.execute(sql_nb_points)
    connexion.commit()
    nb_nvx_points = cur.fetchone()[0]
    
profondeur = 0


# Explore les voisinages
# Tant que de nouveaux points sont découverts dans les clusters
# Et insère les nouveaux poinst dans la table res
while (nb_nvx_points - nb_points) > 0:
    print('Profondeur',profondeur,' --> ',(nb_nvx_points - nb_points), 'nouveaux points.')

    query = """
    INSERT INTO DB_DATALAB_DAF.ML_CLUSTERS
    WITH R1 AS (
    SELECT  T0.CELLULE AS CELLULE,
            T1.ID2 AS COMMANDE,
            """+str(profondeur+1)+""" AS PROFONDEUR

    FROM DB_DATALAB_DAF.ML_CLUSTERS T0

    INNER JOIN (
        SELECT ID1, ID2
        FROM DB_DATALAB_DAF.ML_CLUSTERS_PAIRWISE_DIST
        WHERE CHAMPS_COMMUN BETWEEN """+str(distance)+""" AND (DISTANCE_MAX - 1)
        QUALIFY Count(*) Over(PARTITION BY id1) >= """+str(MinPts)+"""
        ) T1

    ON T1.ID1 = T0.COMMANDE

    WHERE T0.PROFONDEUR = """+str(profondeur)+"""
    )

    SELECT DISTINCT R1.*
    FROM R1

    LEFT JOIN DB_DATALAB_DAF.ML_CLUSTERS R2
    ON R1.CELLULE = R2.CELLULE
    AND R1.COMMANDE = R2.COMMANDE

    WHERE R2.COMMANDE IS NULL
    """
    
    profondeur = profondeur + 1
    
    with connexion.cursor() as cur:
        cur.execute(query)
        connexion.commit()
        cur.execute(sql_nb_points)
        nb_points = nb_nvx_points
        nb_nvx_points = cur.fetchone()[0]
        connexion.commit()


# Explores les points frontières (pas de critere de densité)
        
query = """
INSERT INTO DB_DATALAB_DAF.ML_CLUSTERS

WITH R1 AS (
SELECT  T0.CELLULE AS CELLULE,
        T1.ID2 AS COMMANDE,
        """+str(profondeur+1)+""" AS PROFONDEUR

FROM DB_DATALAB_DAF.ML_CLUSTERS T0

INNER JOIN (
    SELECT ID1, ID2
    FROM DB_DATALAB_DAF.ML_CLUSTERS_PAIRWISE_DIST
    WHERE CHAMPS_COMMUN BETWEEN """+str(distance)+""" AND (DISTANCE_MAX - 1)
    ) T1

ON T1.ID1 = T0.COMMANDE
)

SELECT DISTINCT R1.*
FROM R1

LEFT JOIN DB_DATALAB_DAF.ML_CLUSTERS R2
ON R1.CELLULE = R2.CELLULE
AND R1.COMMANDE = R2.COMMANDE

WHERE R2.COMMANDE IS NULL
"""

profondeur = profondeur + 1

with connexion.cursor() as cur:
    cur.execute(query)
    connexion.commit()
    cur.execute(sql_nb_points)
    nb_points = nb_nvx_points
    nb_nvx_points = cur.fetchone()[0]
    connexion.commit()
    
print('Profondeur',profondeur,' (border points) --> ',(nb_nvx_points - nb_points), 'nouveaux points.')

overlapping_points_table = """CREATE MULTISET VOLATILE TABLE """+user+""".OVERLAPPING_POINTS AS
                                (
                                        SELECT 

                                        T1.cellule AS cellule
                                        , T2.cellule AS nouvelle_cellule
                                        , Count(*) AS points_commun 
                                        , Row_Number() Over (PARTITION BY T1.cellule ORDER BY points_commun DESC) AS rang

                                        FROM DB_DATALAB_DAF.ML_CLUSTERS T1

                                        INNER JOIN DB_DATALAB_DAF.ML_CLUSTERS T2
                                        ON T1.commande = T2.commande
                                        AND T1.cellule < T2.cellule
                                        GROUP BY 1,2
                                        HAVING points_commun >= """+str(Min_Common_Pts)+"""
                                        QUALIFY rang = 1
                                ) WITH DATA UNIQUE PRIMARY INDEX(cellule) ON COMMIT PRESERVE ROWS;"""

overlapping_points_count = """SELECT count(*) FROM """+user+""".OVERLAPPING_POINTS;"""

overlapping_points_updat = """UPDATE S1
                                FROM DB_DATALAB_DAF.ML_CLUSTERS S1, """+user+""".OVERLAPPING_POINTS S2
                                SET CELLULE = S2.nouvelle_cellule
                                WHERE S1.CELLULE = S2.CELLULE;"""

overlapping_points_dropp = """DROP TABLE """+user+""".OVERLAPPING_POINTS;"""

with connexion.cursor() as cur:
    cur.execute(overlapping_points_table)
    connexion.commit()
    cur.execute(overlapping_points_count)
    overlapping = cur.fetchone()[0]
    connexion.commit()
    cur.execute(overlapping_points_dropp)
    connexion.commit()
    
while overlapping > 0:
    
    print(overlapping, 'overlapping cells --> merging')
    
    query4 = """CREATE MULTISET TABLE DB_DATALAB_DAF.ML_CLUSTERS_2 AS(
                        SELECT CELLULE, COMMANDE, Min(PROFONDEUR) AS PROFONDEUR
                        FROM DB_DATALAB_DAF.ML_CLUSTERS
                        GROUP BY 1, 2
            ) WITH DATA PRIMARY INDEX(cellule, commande);"""

    query5 = """DROP TABLE DB_DATALAB_DAF.ML_CLUSTERS;"""

    query6 = """RENAME TABLE DB_DATALAB_DAF.ML_CLUSTERS_2 TO DB_DATALAB_DAF.ML_CLUSTERS;"""
    
    with connexion.cursor() as cur:
        cur.execute(overlapping_points_table)
        connexion.commit()
        cur.execute(overlapping_points_updat)
        connexion.commit()
        cur.execute(query4)
        connexion.commit()
        cur.execute(query5)
        connexion.commit()
        cur.execute(query6)
        connexion.commit()
        cur.execute(overlapping_points_count)
        overlapping = cur.fetchone()[0]
        connexion.commit()
        cur.execute(overlapping_points_dropp)
        connexion.commit()

# Mise en forme pour avoir des numéros de cellule et des profondeurs qui se suivent

query1 = """CREATE SET TABLE DB_DATALAB_DAF.ML_CLUSTERS_2 AS(
               SELECT Dense_Rank() Over (ORDER BY CELLULE) AS CELLULE, 
                      COMMANDE,
                      Dense_Rank() Over (PARTITION BY CELLULE ORDER BY PROFONDEUR) AS PROFONDEUR
                      FROM DB_DATALAB_DAF.ML_CLUSTERS
        ) WITH DATA UNIQUE PRIMARY INDEX(cellule, commande);"""

query2 = """DROP TABLE DB_DATALAB_DAF.ML_CLUSTERS;"""

query3 = """RENAME TABLE DB_DATALAB_DAF.ML_CLUSTERS_2 TO DB_DATALAB_DAF.ML_CLUSTERS;"""

with connexion.cursor() as cur:
    cur.execute(query1)
    connexion.commit()
    cur.execute(query2)
    connexion.commit()
    cur.execute(query3)