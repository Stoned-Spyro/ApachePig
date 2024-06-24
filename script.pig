-- Завантаження даних з файлу
data = LOAD 'hdfs://sandbox-hdp.hortonworks.com:8020/uhadoop/bootcamp5.csv' USING PigStorage(';') AS (
    index:int,
    age:int,
    gender:int,
    height:int,
    weight:float,
    ap_hi:int,
    ap_lo:int,
    cholesterol:int,
    gluc:int,
    smoke:int,
    alco:int,
    active:int,
    cardio:int
);

female_data = FILTER data BY gender == 2;

--Task 1
avg_active = FOREACH ( GROUP data BY active ) GENERATE group, AVG(data.weight);

--Task 2
female_helthy = FILTER female_data BY gluc == 1 AND  cholesterol == 1
extra_data = FOREACH female_helthy GENERATE ((alco == true) ? 1 : 0) as cond;
result = FOREACH (GROUP extra_data ALL) GENERATE AVG(extra_data.cond) as avarage;

DUMP result;
