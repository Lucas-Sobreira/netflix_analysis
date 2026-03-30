CREATE OR REFRESH STREAMING TABLE br_user_rating_history
COMMENT "Tabela da Landing Zone com os dados históricos de avaliações dos usuários"
AS 
SELECT *,
       _metadata.file_name,
       _metadata.file_path,
       _metadata.file_modification_time as load_ts
FROM STREAM read_files(
    '/Volumes/netflix/00_landing/csv/user_rating_history/', 
    format => 'csv'
);