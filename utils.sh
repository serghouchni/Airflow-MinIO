mc alias set myminio http://localhost:9000 minio_user 123456789

{"date": "2023-10-03"}

dbt run --project-dir ./dbt_data_pipeline --models spending_by_naf_code --vars 'target_date: 2023-10-01'

psql -h localhost -U dw -d dw

dbt test --project-dir ./dbt_data_pipeline --models spending_by_naf_code


git config --global user.name "Charhrouchni"
git config --global user.email "mounir.serghouchni@gmail.com"
git commit -m "feat: all"
git remote add origin https://github.com/serghouchni/Airflow-MinIO.git
git push -u origin main

dbt deps
dbt run --models spending_by_naf_code --vars 'target_date: 2023-10-01'

UPDATE curated.customers SET email = 'mounir.charhrouchni@example.com' WHERE first_name = 'Mounir';

INSERT INTO curated.customers (first_name, last_name, email) VALUES
('Mounir', 'Charhrouchni', 'mounir.charhrouchni@example.com');