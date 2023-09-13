-- Build a view of useful FERC Plant & Utility information.
CREATE VIEW denorm_plants_utilities_ferc1 AS
SELECT *
FROM core_pudl__assn_plants_ferc1
    INNER JOIN core_pudl__assn_utilities_ferc1 USING(utility_id_ferc1);
