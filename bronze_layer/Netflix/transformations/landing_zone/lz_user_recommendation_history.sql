CREATE OR REFRESH STREAMING TABLE br_user_recommendation_history
COMMENT "Tabela da Landing Zone com os dados históricos de recomendações de filmes"
AS 
SELECT *,
       _metadata.file_name,
       _metadata.file_path,
       _metadata.file_modification_time as load_ts
FROM STREAM read_files(
    '/Volumes/netflix/00_landing/csv/user_recommendation_history/', 
    format => 'csv'
);