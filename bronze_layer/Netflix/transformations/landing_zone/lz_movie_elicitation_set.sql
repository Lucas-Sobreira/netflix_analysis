CREATE OR REFRESH STREAMING TABLE br_movie_elicitation_set
COMMENT "Tabela da Landing Zone com os dados de elicitação de filmes"
AS 
SELECT *,
       _metadata.file_name,
       _metadata.file_path,
       _metadata.file_modification_time as load_ts
FROM STREAM read_files(
    '/Volumes/netflix/00_landing/csv/movie_elicitation_set/', 
    format => 'csv'
);