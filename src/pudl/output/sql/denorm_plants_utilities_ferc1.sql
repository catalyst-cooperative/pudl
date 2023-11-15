-- Build a view of useful FERC Plant & Utility information.
CREATE VIEW denorm_plants_utilities_ferc1 AS
SELECT *
FROM core_pudl__assn_ferc1_pudl_plants
    INNER JOIN core_pudl__assn_ferc1_pudl_utilities USING(utility_id_ferc1);
