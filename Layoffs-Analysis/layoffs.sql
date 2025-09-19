SELECT * 
FROM layoffs;

-- 1. Remove any duplicates 
-- 2. Standardize the data
-- 3. Null Values
-- 4. Remove any unecessary columns/rows

SELECT * 
FROM layoffs;


-- STEP 1: Remove any duplicates 


-- Create staging table
CREATE TABLE world_layoffs_staging LIKE layoffs;

INSERT INTO world_layoffs_staging
SELECT * 
FROM layoffs;

SELECT * 
FROM world_layoffs_staging;

-- Add row numbers to check for duplicates
WITH world_layoffs_staging_CTE AS (
    SELECT *,
           ROW_NUMBER() OVER(
               PARTITION BY company, location, industry, total_laid_off, 
                            percentage_laid_off, `date`, stage, country, funds_raised
           ) AS row_num
    FROM world_layoffs_staging
)
SELECT *
FROM world_layoffs_staging_CTE
WHERE row_num > 1;

-- Duplicates found: 
-- Casper, Cazoo, Hibob, Wildlife Studios, Yahoo 

-- Create new staging table with row_num column
CREATE TABLE `layoffs`.`world_layoffs_staging_2` (
`company` text,
`location`text,
`industry`text,
`total_laid_off` INT,
`percentage_laid_off` text,
`date` text,
`stage`text,
`country` text,
`funds_raised` int,
row_num INT
);

-- Insert data with row_num
INSERT INTO `layoffs`.`world_layoffs_staging_2`
(
  `company`,
  `location`,
  `industry`,
  `total_laid_off`,
  `percentage_laid_off`,
  `date`,
  `stage`,
  `country`,
  `funds_raised`,
  `row_num`
)
SELECT 
  company,
  location,
  industry,
  NULLIF(total_laid_off, '') AS total_laid_off,
  percentage_laid_off,
  `date`,
  stage,
  country,
  CAST(NULLIF(REPLACE(REPLACE(funds_raised, '$', ''), ',', ''), '') AS UNSIGNED) AS funds_raised,
  ROW_NUMBER() OVER (
    PARTITION BY company, location, industry, NULLIF(total_laid_off, ''), 
                 percentage_laid_off, `date`, stage, country, 
                 CAST(NULLIF(REPLACE(REPLACE(funds_raised, '$', ''), ',', ''), '') AS UNSIGNED)
  ) AS row_num
FROM layoffs.world_layoffs_staging;



-- Delete duplicate rows (keep row_num = 1)
DELETE
FROM world_layoffs_staging_2
WHERE row_num > 1;

SELECT * 
FROM world_layoffs_staging_2
WHERE row_num > 1; -- No duplicates returned


-- STEP 2: Standardize the data


-- Trim whitespace in company names
UPDATE world_layoffs_staging_2
SET company = TRIM(company);

-- Standardize industries
SELECT DISTINCT industry
FROM world_layoffs_staging_2
ORDER BY 1;

UPDATE world_layoffs_staging_2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Standardize country names
UPDATE world_layoffs_staging_2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- Fix date format
UPDATE world_layoffs_staging_2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE world_layoffs_staging_2
MODIFY COLUMN `date` DATE;


-- STEP 3: Null Values


-- Identify rows with null critical fields
SELECT *
FROM world_layoffs_staging_2
WHERE total_laid_off IS NULL
  AND percentage_laid_off IS NULL
  AND funds_raised IS NULL;

-- Delete rows with no useful data
DELETE FROM world_layoffs_staging_2
WHERE (total_laid_off IS NULL OR total_laid_off = '')
  AND (percentage_laid_off IS NULL OR percentage_laid_off = '')
  AND (funds_raised IS NULL OR funds_raised = '');

-- Handle null industries
UPDATE world_layoffs_staging_2
SET industry = NULL
WHERE industry = '';

-- Fill missing industry where possible
UPDATE world_layoffs_staging_2 t1
JOIN world_layoffs_staging_2 t2
  ON t1.company = t2.company
 AND t1.location = t2.location
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
  AND t2.industry IS NOT NULL;


-- STEP 4: Remove unnecessary columns


ALTER TABLE world_layoffs_staging_2
DROP COLUMN row_num;

-- Final check
SELECT *
FROM world_layoffs_staging_2
Limit 10;
