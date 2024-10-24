-- Exploratory Data Analysis

select *
from layoffs_stagging3
where percentage_laid_off=1
order by total_laid_off desc;


select company,sum(total_laid_off)
from layoffs_stagging3
group by company
order by 2 desc;

select industry,sum(total_laid_off)
from layoffs_stagging3
group by industry
order by 2 desc;

select country,sum(total_laid_off)
from layoffs_stagging3
group by country
order by 2 desc;


select year(`date`),sum(total_laid_off)
from layoffs_stagging3
group by  year(`date`)
order by 2 desc;

select substring(`date`,1,7) as `Month`,sum(total_laid_off)
from layoffs_stagging3
where substring(`date`,1,7) is not null
group by `Month`
order by 1 asc;

with Rolling_total as(
select substring(`date`,1,7) as `Month`,sum(total_laid_off) as total_off
from layoffs_stagging3
where substring(`date`,1,7) is not null
group by `Month`
order by 1 asc)
select `Month`,total_off,
sum(total_off) over(order by `Month`) as rolling_total
from Rolling_total;


select company,year(`date`),sum(total_laid_off)
from layoffs_stagging3
group by company,year(`date`)
order by 3 desc;

with company_year (company,years,total_laid_off) as
(select company,year(`date`),sum(total_laid_off)
from layoffs_stagging3
group by company,year(`date`)
), company_year_rank as
(select *, dense_rank() over(partition by years order by total_laid_off desc) as Ranking 
from company_year
where years is not null
order by Ranking asc)
select * 
from company_year_rank
where Ranking<=5
;

 