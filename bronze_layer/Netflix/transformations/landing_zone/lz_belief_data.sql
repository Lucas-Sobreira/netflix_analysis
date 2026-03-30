CREATE OR REFRESH STREAMING TABLE br_belief_data
COMMENT "Tabela da Landing Zone com os dados de previsão de Rating"
AS 
SELECT *,
       _metadata.file_name,
       _metadata.file_path,
       _metadata.file_modification_time as load_ts
FROM STREAM read_files(
    '/Volumes/netflix/00_landing/csv/belief_data/', 
    format => 'csv'
);