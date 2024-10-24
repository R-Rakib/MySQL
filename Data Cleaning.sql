-- Data Cleaning
select *
from layoffs;

-- 1.Remove the duplicates
-- 2.Standardize the data
-- 3.Null values or blank values
-- 4.Remove Unneccesary column

create table layoffs_stagging
like layoffs;

select *
from layoffs_stagging;

insert layoffs_stagging
select * 
from layoffs;

select*,
row_number() over(
partition by company,industry,total_laid_off,percentage_laid_off,stage,country,funds_raised_millions,'date') as row_num
from layoffs_stagging;


with duplicate_cte as
(select*,
row_number() over(
partition by company,industry,total_laid_off,percentage_laid_off,stage,country,funds_raised_millions,'date') as row_num
from layoffs_stagging)
select * 
from duplicate_cte 
where row_num>1;

select*
from layoffs_stagging
where company='olist';

CREATE TABLE `layoffs_stagging3` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * from layoffs_stagging3
where row_num>1;

insert into layoffs_stagging3
select*,
row_number() over(
partition by company,industry,total_laid_off,percentage_laid_off,stage,`date`,country,funds_raised_millions) as row_num
from layoffs_stagging;

SET sql_safe_updates = 0;

DELETE 
from layoffs_stagging3
WHERE row_num > 1 ; 

select * from layoffs_stagging3;

select company,(trim(company))
from layoffs_stagging3;

update layoffs_stagging3
set company=trim(company);


-- Standardization

select distinct *
from layoffs_stagging3
where industry like 'Crypto%'
;

update layoffs_stagging3
set industry='Crypto'
where industry like 'Crypto%';

select distinct industry
from layoffs_stagging3;

select distinct country
from layoffs_stagging3
order by 1;

-- There is a double value of United States

update layoffs_stagging3
set country='United States'
where country like 'United States%';


-- Formatting `date` column from 'text' to 'Time Series'

select `date`,
str_to_date(`date`,'%m/%d/%Y');

update layoffs_stagging3
set `date`=str_to_date(`date`,'%m/%d/%Y');

select * 
from layoffs_stagging3;

alter table layoffs_stagging3
modify column `date` date;


-- Handling NUll Values

select *
from layoffs_stagging3
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoffs_stagging3
where industry is null or industry='';

select t1.industry ,t2.industry
from layoffs_stagging3 t1
join layoffs_stagging3 t2
on t1.company=t2.company
where (t1.industry is null or t1.industry='')
and t2.industry is not null;

update layoffs_stagging3
set industry=null
where industry='';

update layoffs_stagging3 t1
join layoffs_stagging3 t2
on t1.company=t2.company
set t1.industry=t2.industry
where t1.industry is null 
and t2.industry is not null;

select *
from layoffs_stagging3
where company='Airbnb';

delete
from layoffs_stagging3
where total_laid_off is null
and percentage_laid_off is null;

-- Deleting Column

select *
from layoffs_stagging3;

alter table layoffs_stagging3
drop column row_num;



