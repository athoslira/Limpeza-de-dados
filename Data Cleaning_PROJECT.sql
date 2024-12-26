SELECT *
FROM layoffs;

-- 1. Remover duplicatas
-- 2. padronizar
-- 3. valores vazios ou em brancos
-- 4. remover colunas

CREATE TABLE layoffs_copy
LIKE layoffs;

SELECT * FROM layoffs_copy;
INSERT layoffs_copy
SELECT * from layoffs;


SELECT *,
ROW_NUMBER() OVER( PARTITION BY company, industry, total_laid_off, 
percentage_laid_off, `date`) AS row_num
FROM layoffs_copy;

WITH duplicate_cte as 
(
SELECT*,
ROW_NUMBER () OVER( PARTITION BY company, location, industry, total_laid_off, 
percentage_laid_off, `date`, stage, 
country, funds_raised_millions) AS row_num
FROM layoffs_copy
)
SELECT * 
FROM duplicate_cte
WHERE row_num > 1;

SELECT * FROM layoffs_copy
WHERE company = "Included Health";

CREATE TABLE `layoffs_copy3` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * from layoffs_copy2;

INSERT INTO layoffs_copy2
SELECT*,
ROW_NUMBER () OVER( PARTITION BY company, location, industry, total_laid_off, 
percentage_laid_off, `date`, stage, 
country, funds_raised_millions) AS row_num
FROM layoffs_copy;

SELECT *
FROM layoffs_copy2
WHERE row_num > 1;

DELETE
FROM layoffs_copy2
WHERE row_num > 1;

-- PADRONIZAR
 SELECT company, TRIM(company)
 FROM layoffs_copy2;
 
 UPDATE layoffs_copy2
 SET company = TRIM(company);

SELECT * 
FROM layoffs_copy2
WHERE industry LIKE "Crypto%";

UPDATE layoffs_copy2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

select distinct country
FROM layoffs_copy2;
 
select distinct country, TRIM( TRAILING '.' FROM country)
FROM layoffs_copy2
ORDER BY 1;

update layoffs_copy2
SET country = TRIM( TRAILING '.' FROM country)
where country LIKE 'United States%';

select `date`date
FROM layoffs_copy2;

UPDATE layoffs_copy2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

 alter table layoffs_copy2 
 MODIFY COLUMN `date` DATE;
 
 -- ESPAÇOS EM BRANCO
 update layoffs_copy2
 set industry = null
 where industry = '';
 
 SELECT *
 FROM layoffs_copy2
 WHERE total_laid_off IS NULL
 AND percentage_laid_off IS NULL;
 
 select *
 from layoffs_copy2
 WHERE industry is null
 OR industry = '';
 
 SELECT *
 FROM layoffs_copy2
 WHERE company = 'Airbnb';
 
select *
from layoffs_copy2 c1
join layoffs_copy2 c2
   on c1.company = c2.company
where (c1.industry is null or c1.industry = '')
and c2.industry is not null;

update layoffs_copy2 c1
join layoffs_copy2 c2
   on c1.company = c2.company
SET c1.industry = c2.industry
where (c1.industry is null or c1.industry = '')
and c2.industry is not null;

delete
from layoffs_copy2
where total_laid_off is null
and percentage_laid_off is null;

alter table layoffs_copy2
drop column row_num;

select *
from layoffs_copy2;
 
 
 