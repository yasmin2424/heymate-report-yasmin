# 🍽️ Heymate Menu Recommendation Project

## Project Overview
This project aims to build a menu recommendation pipeline for Heymate's restaurant partners. By analyzing internal product, store, and category data along with external datasets we seek to provide insights into menu item popularity, pricing, and restaurant-specific recommendations. Our recommendations help partners optimize their offerings through data driven insights.

## Summary
We conduct comprehensive data validation, enrichment, and exploratory analysis on restaurant menu data. Key steps include:

- Cleaning inconsistent menu item names using LLMs  
- Merging external datasets (e.g. Kaggle)  
- Scoring menu item popularity  
- Building a pipeline to deliver actionable insights to restaurant clients  

## How to Run the Analysis
To render the Heymate recommendation proposal report on your local machine, follow these steps:

### 1. Check if Quarto is Installed

Open your terminal or RStudio terminal and run:

```bash
quarto check
```
If you see a version number, Quarto is installed.  
If not, download and install [Quarto](https://quarto.org/)

### 2. **Clone the Repository**:
```bash
git clone https://github.com/your-org/heymate-report.git
cd heymate-report
```

### 3. **Open in RStudio or VS Code**:
Open the project in RStudio or VS Code and navigate to the report folder:
```r
YOUR-PATH/heymate-report/report/heymate-report.qmd
```

### 4. **Install R Dependencies**:
In your R console, install the required packages:
```r
install.packages(c("readr", "dplyr", "tibble", "knitr"))
```

### 5. **In the terminal, run:
```bash
quarto render report/heymate-report.qmd --to pdf
```