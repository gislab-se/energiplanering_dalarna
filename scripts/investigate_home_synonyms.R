#!/usr/bin/env Rscript

suppressWarnings(suppressMessages({
  library(dplyr)
  library(stringr)
  library(readr)
}))

in_file <- file.path("data", "interim", "hem_kommun_network", "response_tokens.csv")
out_dir <- file.path("data", "interim", "hem_kommun_network")

if (!file.exists(in_file)) {
  stop("Missing input file: ", in_file, call. = FALSE)
}

tokens <- readr::read_csv(in_file, show_col_types = FALSE) %>%
  mutate(
    token = str_to_lower(token),
    response_id = as.character(response_id)
  ) %>%
  filter(!is.na(token), token != "")

token_freq <- tokens %>%
  count(token, sort = TRUE, name = "token_n")

hem_forms <- token_freq %>%
  filter(str_detect(token, "^hem")) %>%
  arrange(desc(token_n), token)

responses <- tokens %>%
  distinct(response_id)

hem_response_ids <- tokens %>%
  filter(str_detect(token, "^hem")) %>%
  distinct(response_id)

n_responses <- nrow(responses)
n_hem_responses <- nrow(hem_response_ids)

lexicon <- tibble(
  concept = c(
    "hem_prefix",
    "stuga_prefix",
    "fritidshus_prefix",
    "bostad_prefix",
    "hus_prefix",
    "boende_prefix",
    "villa_prefix",
    "lagenhet_prefix"
  ),
  pattern = c(
    "^hem",
    "^stug",
    "^fritidshus",
    "^bostad",
    "^hus",
    "^boend",
    "^villa",
    "^lägenhet|^lagenhet"
  )
)

token_sets <- tokens %>%
  distinct(response_id, token)

concept_stats <- lapply(seq_len(nrow(lexicon)), function(i) {
  concept_name <- lexicon$concept[[i]]
  pattern <- lexicon$pattern[[i]]

  concept_ids <- token_sets %>%
    filter(str_detect(token, pattern)) %>%
    distinct(response_id)

  n_concept <- nrow(concept_ids)
  n_joint <- concept_ids %>%
    inner_join(hem_response_ids, by = "response_id") %>%
    nrow()

  p_concept <- if (n_responses > 0) n_concept / n_responses else 0
  p_concept_given_hem <- if (n_hem_responses > 0) n_joint / n_hem_responses else 0
  lift <- if (p_concept > 0) p_concept_given_hem / p_concept else NA_real_

  tibble(
    concept = concept_name,
    pattern = pattern,
    responses_with_concept = n_concept,
    response_share = p_concept,
    responses_with_both_hem_and_concept = n_joint,
    share_given_hem = p_concept_given_hem,
    lift_vs_baseline = lift
  )
}) %>%
  bind_rows() %>%
  arrange(desc(share_given_hem))

readr::write_csv(hem_forms, file.path(out_dir, "hem_forms_frequency.csv"))
readr::write_csv(concept_stats, file.path(out_dir, "home_synonym_concepts.csv"))

cat("Responses total:", n_responses, "\n")
cat("Responses with hem*:", n_hem_responses, sprintf("(%.1f%%)\n", 100 * n_hem_responses / max(1, n_responses)))
cat("\nTop hem* forms:\n")
print(head(hem_forms, 20))
cat("\nCandidate concepts (co-occurrence with hem*):\n")
print(concept_stats)
cat("\nSaved:\n")
cat(file.path(out_dir, "hem_forms_frequency.csv"), "\n")
cat(file.path(out_dir, "home_synonym_concepts.csv"), "\n")
