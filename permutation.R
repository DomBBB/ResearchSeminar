remove(list=ls())
#dev.off()
#------------------------------------------------------------------------------

###### Prepare Data ######
#Libraries
library(readr)
library(readxl)
library(dplyr)
library(tidyr)
library(tidyverse)
library(patchwork)
library(purrr)
library(scales)
library(lubridate)
library(foreach)
library(moments)
library(openxlsx)
# Read files
setwd("C:/Users/domin/OneDrive/Desktop/MBF/RS/Data/SPD")
csv_files <- list.files(pattern = "\\.csv$")
data_list <- setNames(lapply(csv_files, read_csv),
                      sapply(strsplit(csv_files, "[.]"), `[`, 1))
data_list <- lapply(data_list, function(df) df[,-1 ])
setwd("C:/Users/domin/OneDrive/Desktop/MBF/RS/Data")
description <- read_xlsx("Overview Companies.xlsx")
indices_names <- read_xlsx("List of Market Indices.xlsx")
indices <- read_xlsx("Market indices data.xlsx")
output_dir_graphics <- "C:/Users/domin/OneDrive/Desktop/MBF/RS/Data/Output/Graphics"
output_dir_tables <- "C:/Users/domin/OneDrive/Desktop/MBF/RS/Data/Output/Tables"
event_dates <- read_xlsx("Event Dates.xlsx")
rf_rate <- read.csv("Daily yield of current 30 year federal bond Germany.csv")

# Calculate daily returns and cumulative returns for all stocks
data_list <- lapply(data_list, function(df) {
  df <- df %>%
    mutate(
      Daily_Return = (PX_LAST - lag(PX_LAST)) / lag(PX_LAST),
      Cumulative_Return = cumprod(1 + replace_na(Daily_Return,0)) - 1
    )
  return(df)
})

#Delete _EUR in names
names(data_list) <- gsub("_EUR", "", names(data_list))
names(data_list) <- gsub("_", " ", names(data_list))

# Adjust name of AV / LN stock
names(data_list)[names(data_list) == "AV+ LN Equity"] <- "AV LN Equity"
description$Ticker <- gsub("/", "", description$Ticker) # Adjusting in description data frame as well

# Create list of countries
unique_countries <- unique(description$Country)
country_tickers_list <- list()
for (country in unique_countries) {
  tickers <- description %>%
    filter(Country == country) %>%
    pull(Ticker)

  # Store the tickers in the list with the country name as the key
  country_tickers_list[[country]] <- tickers
}

# Prepare list of market indices
indices_list <- lapply(names(indices)[-c(1:2)], function(col_name) { # Skipping the first two columns
  new_df <- indices %>%
    select(Date, all_of(col_name)) %>%
    rename(Index_Value = col_name)
  new_df <- new_df %>% arrange(Date)
  return(new_df)
})
names(indices_list) <- names(indices)[-c(1:2)] # Assigning the names of indices as list names

# Calculate daily returns and cumulative returns
indices_list <- lapply(indices_list, function(df) {
  df <- df %>%
    mutate(
      Daily_Return = (Index_Value - lag(Index_Value)) / lag(Index_Value),
      Cumulative_Return = cumprod(1 + replace_na(Daily_Return,0)) - 1
    )
  return(df)
})

# Rename Date into date to stay uniformly
indices_list <- lapply(indices_list, function(df) {
  df %>%
    rename(date = Date)
})

# Prepare risk-free data
rf_rate <- rf_rate[-(1:8),]
names(rf_rate)[1] <- "date"
names(rf_rate)[2] <- "yield"
rf_rate <- rf_rate[, -3]
rf_rate <- rf_rate %>% filter(!if_any(everything(), ~. == "."))
rf_rate$yield <- as.numeric(rf_rate$yield)
rf_rate <- rf_rate %>% mutate(yield = (1+yield/100)^(1/250)-1)
rf_rate$date <- as.Date(rf_rate$date)

#------------------------------------------------------------------------------

###### Return Overview Events for Stocks ######
# Create Output Tables
event_dataframes <- list()
num_rows <- nrow(description)
column_names <- c("Company", "Country", "[0]", "[-2,2]", "[-5,5]","[0,10]", "[-10,10]", "[-5,20]", "[0,30]", "[-10,30]", "[-30,30]")
event_names <- colnames(event_dates)[-1]
for (event_name in event_names){
  current_event_df <- tibble(matrix(nrow = num_rows, ncol = length(column_names)))
  names(current_event_df) <- column_names
  # Fill in the Company and Country columns from the description dataframe
  current_event_df$Company <- description$Ticker
  current_event_df$Country <- description$Country

  # Remaining columns are filled with NA, indicating placeholder values for actual calculations
  current_event_df[, c("[0]", "[-2,2]", "[-5,5]", "[0,10]", "[-10,10]","[-5,20]", "[0,30]", "[-10,30]", "[-30,30]")] <- NA

  # Store the dataframe in the list with the event name as the key
  event_dataframes[[event_name]] <- current_event_df
}
event_dataframes$GFC


fill_daily_returns_on_event_stocks <- function(event_names, event_dates, country_tickers_list, data_list, event_dataframes) {
  for (event_name in event_names) {
    event_df <- event_dataframes[[event_name]]

    for (country in unique(event_dates$Country)) {
      event_date <- event_dates %>% filter(Country == country) %>% pull(!!sym(event_name))

      country_tickers <- country_tickers_list[[country]]

      for (ticker in country_tickers) {
        if (!ticker %in% names(data_list)) next

        company_data <- data_list[[ticker]]
        available_dates <- company_data$date
        closest_date <- min(available_dates[available_dates >= event_date], na.rm = TRUE)

        row_to_update <- which(event_df$Company == ticker)
        if (length(row_to_update) == 0 || is.na(row_to_update)) next

        if (is.infinite(closest_date) || abs(as.Date(closest_date) - as.Date(event_date)) > 5) {
          event_df[row_to_update, c("[0]", "[-2,2]", "[-5,5]","[0,10]", "[-10,10]", "[-5,20]","[0,30]", "[-10,30]","[-30,30]")] <- NA
        } else {
        periods <- list(
          "[0]" = 0,
          "[-2,2]" = -2:2,
          "[-5,5]" = -5:5,
          "[0,10]" = 0:10,
          "[-10,10]" = -10:10,
          "[-5,20]" = c(-5:20),
          "[0,30]" = c(0:30),
          "[-10,30]" = c(-10:30),
          "[-30,30]" = c(-30:30)
        )

       #Check if closest date is within 4 days of event date, if not set all returns to NA

        for (period_name in names(periods)) {
          dates_to_fetch <- as.Date(closest_date) + periods[[period_name]]
          # Filter the data for the specific period
          filtered_data <- company_data %>% filter(date %in% dates_to_fetch)
          # Calculate the total return for the period
          total_return <- sum(filtered_data$Daily_Return, na.rm = TRUE)
          # Fill the calculated return into the respective column of event_df
          event_df[row_to_update, period_name] <- total_return
        }
        }
      }
      event_dataframes[[event_name]] <- event_df
    }
  }
    return(event_dataframes)
}

# Run your function and assign the updated list back to event_dataframes
event_dataframes <- fill_daily_returns_on_event_stocks(event_names, event_dates, country_tickers_list, data_list, event_dataframes)


# -----------------------------------------------------------------------------
###### Return Overview Events for Indices
# Create output dataframe
event_dataframes_indices <- list()
num_rows <- nrow(indices_names)
column_names <- c("Index", "Country", "[0]", "[-2,2]", "[-5,5]","[0,10]", "[-10,10]", "[-5,20]", "[0,30]", "[-10,30]", "[-30,30]")
for (event_name in event_names){
  current_event_df <- tibble(matrix(nrow = num_rows, ncol = length(column_names)))
  names(current_event_df) <- column_names
  # Fill in the Indiex and Country columns from the description dataframe
  current_event_df$Country <- indices_names$Country
  current_event_df$Index <- indices_names$Index

  # Remaining columns are filled with NA, indicating placeholder values for actual calculations
  current_event_df[, c("[0]", "[-2,2]", "[-5,5]", "[0,10]", "[-10,10]","[-5,20]", "[0,30]", "[-10,30]", "[-30,30]")] <- NA

  # Store the dataframe in the list with the event name as the key
  event_dataframes_indices[[event_name]] <- current_event_df
}

# Fill in returns from indices
fill_daily_returns_on_event_indices <- function(event_names, event_dates, event_dataframes_indices) {
  for (event_name in event_names) {
    event_df <- event_dataframes_indices[[event_name]]

    for (country in unique(event_dates$Country)) {
      event_date <- event_dates %>% filter(Country == country) %>% pull(!!sym(event_name))
      index <- indices_names %>% filter(Country == country) %>% pull(Index)
        index_data <- indices_list[[index]]
        available_dates <- index_data$date
        closest_date <- min(available_dates[available_dates >= event_date], na.rm = TRUE)

        row_to_update <- which(event_df$Index == index)
        if (length(row_to_update) == 0 || is.na(row_to_update)) next

        if (is.infinite(closest_date) || abs(as.Date(closest_date) - as.Date(event_date)) > 5) {
          event_df[row_to_update, c("[0]", "[-2,2]", "[-5,5]","[0,10]", "[-10,10]", "[-5,20]","[0,30]", "[-10,30]","[-30,30]")] <- NA
        } else {
          periods <- list(
            "[0]" = 0,
            "[-2,2]" = -2:2,
            "[-5,5]" = -5:5,
            "[0,10]" = 0:10,
            "[-10,10]" = -10:10,
            "[-5,20]" = c(-5:20),
            "[0,30]" = c(0:30),
            "[-10,30]" = c(-10:30),
            "[-30,30]" = c(-30:30)
          )

          #Check if closest date is within 4 days of event date, if not set all returns to NA

          for (period_name in names(periods)) {
            dates_to_fetch <- as.Date(closest_date) + periods[[period_name]]
            # Filter the data for the specific period
            filtered_data <- index_data %>% filter(date %in% dates_to_fetch)
            # Calculate the total return for the period
            total_return <- sum(filtered_data$Daily_Return, na.rm = TRUE)
            # Fill the calculated return into the respective column of event_df
            event_df[row_to_update, period_name] <- total_return
        }
      }
      event_dataframes_indices[[event_name]] <- event_df
    }
  }
  return(event_dataframes_indices)
}

# Run your function and assign the updated list back to event_dataframes
event_dataframes_indices <- fill_daily_returns_on_event_indices(event_names, event_dates, event_dataframes_indices)


#------------------------------------------------------------------------------
###### Setting lengths of windows to estimate data for each method as well as maximum event period length one-way
length_days_estimation_window_capm <- 700
length_days_estimation_window_constant_mean <- 180
length_maximum_event_period <- 30
#-------------------------------------------------------------------------------

#################################################################################
event_windows <- list(
  list(name = "[0]", start=0, end=0),
  list(name = "[-2,2]", start=2, end=2),
  list(name = "[-5,5]", start=5, end=5),
  list(name = "[0,10]", start=0, end=10),
  list(name = "[-10,10]", start=10, end=10),
  list(name = "[-5,20]", start=5, end=20),
  list(name = "[0,30]", start=0, end=30),
  list(name = "[-10,30]", start=10, end=30),
  list(name = "[-30,30]", start=30, end=30)
)

###### Calculate Abnormal Returns with constant-mean method ######
forecast_and_calculate_ar_mean <- function(days_estimation_window, days_test_window) {
  # Loop through all events
  for (event_name in event_names) {
    print(event_name)
    # Loop through all countries
    for (country in names(country_tickers_list)) {
      event_date <- event_dates %>% filter(Country == country) %>% select(!!sym(event_name)) %>% pull()

      # Check if event date is NA
      if(is.na(event_date) || length(event_date) == 0) next

      # Convert event date to Date class
      event_date <- as.Date(event_date)

      # Loop through each ticker in the country
      for (ticker in country_tickers_list[[country]]) {
        if(!ticker %in% names(data_list)) next

        ## DATA
        company_data <- data_list[[ticker]]
        company_data$date <- as.Date(company_data$date)
        abnormal_return_col_name <- paste0("AR_", event_name)  # Dynamic column name for abnormal returns
        start_date_est <- event_date - (days_estimation_window + days_test_window)
        end_date_est <- event_date - (days_test_window + 1)
        max_end_date_test <- event_date + days_test_window

        ## GENERATE AR
        estimation_data <- company_data %>%
          filter(date >= start_date_est & date <= end_date_est)
        avg_return <- mean(estimation_data$Daily_Return, na.rm = TRUE)

        combined_data <- company_data %>%
          filter(date >= start_date_est & date <= max_end_date_test) %>%
          mutate(!!abnormal_return_col_name := Daily_Return - avg_return)
        company_data <- left_join(company_data, combined_data %>% select(date, !!abnormal_return_col_name), by = "date")

        abnormal_returns_estimation <- company_data %>%
          filter(date >= start_date_est & date <= end_date_est) %>%
          pull(!!abnormal_return_col_name)  # Extract abnormal returns
        sd_estimation <- sd(abnormal_returns_estimation, na.rm = TRUE)

        ## PERMUTATIONS (already have "abnormal_returns_estimation")
        for (i in seq_along(event_windows)) {
          start_date_test <- event_date - event_windows[[i]]$start
          end_date_test <- event_date + event_windows[[i]]$end
          abnormal_returns_test <- company_data %>%
            filter(date >= start_date_test & date <= end_date_test) %>%
            pull(!!abnormal_return_col_name)  # Extract abnormal returns
          # Initialize a vector to store test statistics from each permutation
          permuted_test_statistics <- numeric(10000)  # Adjust the number of permutations as needed
          # Calculate the original test statistic (e.g., sum of abnormal returns)
          n <- length(abnormal_returns_test[!is.na(abnormal_returns_test)])  # Number of observations
          mean_ar <- mean(abnormal_returns_test, na.rm = TRUE)
          original_test_statistic <- abs(mean_ar / (sd_estimation / sqrt(n)))
          permuted_test_statistics[1] <- original_test_statistic
          # Get full abnormal returns
          abnormal_returns_all <- c(abnormal_returns_estimation, abnormal_returns_test)
          if(length(abnormal_returns_all) > 0) {
            set.seed(123)  # Ensure reproducibility
            for(i in 2:10000) {  # Adjust the number of permutations as needed
              # Shuffle the abnormal returns
              shuffled_returns <- sample(abnormal_returns_all, n)
              # Recalculate the test statistic (e.g., sum of shuffled abnormal returns)
              mean_ar <- mean(shuffled_returns, na.rm = TRUE)
              permuted_test_statistics[i] <- abs(mean_ar / (sd_estimation / sqrt(n)))
            }
            # Calculate the p-value as the proportion of permuted t-statistics that are as extreme or more extreme than the original t-statistic
            p_value_col_name <- paste0("P_Value_", event_name, event_windows[[i]]$start, event_windows[[i]]$end)
            p_value <- sum(permuted_test_statistics >= original_test_statistic)/10000
            company_data[[p_value_col_name]] <- p_value
          }
        }
        data_list[[ticker]] <- company_data
      }
    }
  }
  return(data_list)
}
# Call the function
data_list <- forecast_and_calculate_ar_mean(length_days_estimation_window_constant_mean, length_maximum_event_period)


####################################################################################################
