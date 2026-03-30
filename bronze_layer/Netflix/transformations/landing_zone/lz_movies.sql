CREATE OR REFRESH STREAMING TABLE br_movies
COMMENT "Tabela da Landing Zone com os dados de filmes da Netflix"
AS 
SELECT *,
       _metadata.file_name,
       _metadata.file_path,
       _metadata.file_modification_time as load_ts
FROM STREAM read_files(
    '/Volumes/netflix/00_landing/csv/movies/', 
    format => 'csv'
);