mc alias set myminio http://localhost:9000 minio_user 123456789

{"date": "2023-10-03"}

dbt run --project-dir ./dbt_data_pipeline --models spending_by_naf_code --vars 'target_date: 2023-10-01'

psql -h localhost -U dw -d dw

dbt test --project-dir ./dbt_data_pipeline --models spending_by_naf_code


git config --global user.name "Charhrouchni"
git config --global user.email "mounir.serghouchni@gmail.com"
