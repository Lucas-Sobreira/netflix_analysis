CREATE OR REFRESH STREAMING TABLE br_ratings_for_additional_users
COMMENT "Tabela da Landing Zone com os dados de avaliações de filmes para usuários adicionais"
AS 
SELECT *,
       _metadata.file_name,
       _metadata.file_path,
       _metadata.file_modification_time as load_ts
FROM STREAM read_files(
    '/Volumes/netflix/00_landing/csv/ratings_for_additional_users/', 
    format => 'csv'
);