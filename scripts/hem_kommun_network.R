#!/usr/bin/env Rscript

suppressWarnings(suppressMessages({
  library(dplyr)
  library(tidyr)
  library(stringr)
  library(readr)
  library(purrr)
}))

`%||%` <- function(x, y) if (!is.null(x) && length(x) > 0) x else y

args <- commandArgs(trailingOnly = TRUE)

find_default_csv <- function() {
  candidates <- list.files("data", pattern = "LabLab\\.csv$", recursive = TRUE, full.names = TRUE)
  if (length(candidates) == 0) stop("No LabLab.csv file found under data/", call. = FALSE)
  candidates[[1]]
}

csv_path <- if (length(args) >= 1) args[[1]] else find_default_csv()
out_dir <- if (length(args) >= 2) args[[2]] else file.path("data", "interim", "hem_kommun_network")

dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

read_csv_robust <- function(path) {
  # Try UTF-8 first, then fallback to latin1/cp1252.
  out <- tryCatch(
    readr::read_csv(path, show_col_types = FALSE),
    error = function(e) NULL
  )
  if (!is.null(out)) return(out)

  out <- tryCatch(
    readr::read_csv(path, show_col_types = FALSE, locale = locale(encoding = "Latin1")),
    error = function(e) NULL
  )
  if (!is.null(out)) return(out)

  read.csv(path, stringsAsFactors = FALSE, check.names = FALSE, fileEncoding = "latin1")
}

df <- read_csv_robust(csv_path)

df <- as.data.frame(df, stringsAsFactors = FALSE)
df[] <- lapply(df, function(x) if (is.factor(x)) as.character(x) else x)

cat("\n=== Step 1: CSV loaded ===\n")
cat("File:", csv_path, "\n")
cat("Rows:", nrow(df), "Columns:", ncol(df), "\n")
cat("Column names:\n")
cat(paste(colnames(df), collapse = ", "), "\n\n")

print(utils::head(df, 3))

non_empty_count <- function(x) sum(!is.na(x) & trimws(as.character(x)) != "")

pick_text_columns <- function(dat) {
  cols <- names(dat)
  chars <- cols[vapply(dat, function(x) is.character(x), logical(1))]
  if (length(chars) == 0) stop("No character columns found.", call. = FALSE)

  info <- tibble(col = chars) %>%
    mutate(
      nonempty = map_int(col, ~ non_empty_count(dat[[.x]])),
      mean_nchar = map_dbl(col, ~ {
        v <- as.character(dat[[.x]])
        v <- v[!is.na(v) & trimws(v) != ""]
        if (length(v) == 0) 0 else mean(nchar(v), na.rm = TRUE)
      }),
      looks_text_name = str_detect(col, regex("txt|text|svar|response|open|free", ignore_case = TRUE)),
      score = nonempty + ifelse(looks_text_name, 10000, 0) + mean_nchar
    ) %>%
    arrange(desc(score), desc(nonempty))

  preferred <- c("Q7_1_TXT", "Q9_1_TXT")
  present_pref <- preferred[preferred %in% info$col]
  if (length(present_pref) >= 1) return(present_pref)

  chosen <- info %>% filter(nonempty >= 50, mean_nchar >= 5) %>% slice_head(n = 2) %>% pull(col)
  if (length(chosen) == 0) chosen <- info %>% slice_head(n = 1) %>% pull(col)
  chosen
}

pick_kommun_column <- function(dat) {
  cols <- names(dat)
  cand <- cols[str_detect(cols, regex("^Q1$|kommun|hemvist|residence|kommunkod", ignore_case = TRUE))]
  if ("Q1" %in% cand) return("Q1")

  if (length(cand) == 0) {
    cand <- cols[vapply(dat, function(x) {
      if (!is.character(x) && !is.numeric(x)) return(FALSE)
      v <- as.character(x)
      v <- v[!is.na(v) & trimws(v) != ""]
      u <- length(unique(v))
      u >= 5 && u <= 60
    }, logical(1))]
  }

  if (length(cand) == 0) stop("No plausible municipality/home column found.", call. = FALSE)

  info <- tibble(col = cand) %>%
    mutate(
      nonempty = map_int(col, ~ non_empty_count(dat[[.x]])),
      uniq = map_int(col, ~ {
        v <- as.character(dat[[.x]])
        length(unique(v[!is.na(v) & trimws(v) != ""]))
      }),
      mean_len = map_dbl(col, ~ {
        v <- as.character(dat[[.x]])
        v <- v[!is.na(v) & trimws(v) != ""]
        if (length(v) == 0) 0 else mean(nchar(v), na.rm = TRUE)
      }),
      name_bonus = str_detect(col, regex("kommun|hemvist|residence|q1", ignore_case = TRUE)),
      score = nonempty + ifelse(name_bonus, 10000, 0) - abs(uniq - 15) * 100 - mean_len
    ) %>%
    arrange(desc(score))

  info$col[[1]]
}

text_cols <- pick_text_columns(df)
kommun_col <- pick_kommun_column(df)

cat("\n=== Step 2: Selected columns ===\n")
cat("Municipality/home column:", kommun_col, "\n")
cat("Open text column(s):", paste(text_cols, collapse = ", "), "\n\n")

print(
  df %>%
    select(any_of(c(kommun_col, text_cols))) %>%
    head(5)
)

kommun_code_map <- c(
  "2021" = "Vansbro",
  "2023" = "Malung-Salen",
  "2026" = "Gagnef",
  "2029" = "Leksand",
  "2031" = "Rattvik",
  "2034" = "Orsa",
  "2039" = "Alvdalen",
  "2061" = "Smedjebacken",
  "2062" = "Mora",
  "2080" = "Falun",
  "2081" = "Borlange",
  "2082" = "Sater",
  "2083" = "Hedemora",
  "2084" = "Avesta",
  "2085" = "Ludvika"
)

normalize_kommun <- function(x) {
  x <- trimws(as.character(x))
  x[x == ""] <- NA_character_
  mapped <- ifelse(!is.na(x) & x %in% names(kommun_code_map), kommun_code_map[x], x)
  ifelse(is.na(mapped) | mapped == "", "Unknown", mapped)
}

normalize_text <- function(x) {
  x <- iconv(as.character(x), from = "", to = "UTF-8", sub = "")
  x <- str_to_lower(x)
  x <- str_replace_all(x, "[^[:alpha:][:digit:]\\s]", " ")
  x <- str_replace_all(x, "\\s+", " ")
  trimws(x)
}

make_long_responses <- function(dat, text_columns, kommun_column) {
  base <- tibble(row_id = seq_len(nrow(dat)), kommun_raw = dat[[kommun_column]])

  bind_rows(lapply(text_columns, function(tc) {
    tibble(
      row_id = seq_len(nrow(dat)),
      source_col = tc,
      text_raw = as.character(dat[[tc]])
    )
  })) %>%
    left_join(base, by = "row_id") %>%
    mutate(
      kommun = normalize_kommun(kommun_raw),
      text_clean = normalize_text(text_raw),
      response_id = paste0(row_id, "_", source_col)
    ) %>%
    filter(!is.na(text_clean), text_clean != "") %>%
    select(response_id, row_id, source_col, kommun, text_raw, text_clean)
}

responses <- make_long_responses(df, text_cols, kommun_col)

cat("\n=== Step 3: Response base ===\n")
cat("Non-empty open responses:", nrow(responses), "\n")
cat("Unique respondents with >=1 open response:", length(unique(responses$row_id)), "\n\n")

sw_fallback <- c(
  "och", "det", "att", "i", "en", "jag", "hon", "som", "han", "pa", "den", "med", "var", "sig",
  "for", "sa", "till", "ar", "men", "ett", "om", "hade", "de", "av", "icke", "mig", "du", "henne",
  "da", "sin", "nu", "har", "inte", "hans", "honom", "skulle", "hennes", "dar", "min", "man", "ej",
  "vid", "kunde", "nagot", "fran", "ut", "nar", "efter", "upp", "vi", "dem", "vara", "vad", "over",
  "an", "dig", "kan", "sina", "har", "ha", "honom", "oss", "alla", "under", "nagon", "eller", "allt",
  "mycket", "sedan", "ju", "denna", "sjalv", "detta", "at", "utan", "varit", "hur", "ingen", "mitt",
  "ni", "bli", "blev", "oss", "din", "dessa", "nagra", "deras", "blir", "mina", "samma", "vilken",
  "er", "saadan", "var", "vart", "era", "vilka", "ditt", "vem", "vilket", "sitta", "sadan", "vart"
)

if (requireNamespace("stopwords", quietly = TRUE)) {
  sw <- unique(c(sw_fallback, stopwords::stopwords("sv")))
} else {
  sw <- sw_fallback
}
sw <- setdiff(sw, "hem")

# Tokenize each response while preserving row metadata.
token_list <- strsplit(responses$text_clean, " ", fixed = TRUE)

tokens <- tibble(
  response_id = rep(responses$response_id, lengths(token_list)),
  row_id = rep(responses$row_id, lengths(token_list)),
  kommun = rep(responses$kommun, lengths(token_list)),
  source_col = rep(responses$source_col, lengths(token_list)),
  token = unlist(token_list, use.names = FALSE)
) %>%
  filter(!is.na(token), token != "")

tokens <- tokens %>%
  mutate(
    keep = token == "hem" | (!(token %in% sw) & nchar(token) >= 2),
    hem_strict = token == "hem",
    hem_lemma = str_detect(token, "^hem")
  )

tokens_kept <- tokens %>% filter(keep)

word_freq <- tokens_kept %>%
  count(token, sort = TRUE, name = "n") %>%
  mutate(hem_prefix = str_detect(token, "^hem"))

word_freq_excl_hem <- word_freq %>%
  filter(!hem_prefix)

response_tokens <- tokens_kept %>%
  select(response_id, row_id, source_col, kommun, token)

response_flags <- tokens %>%
  group_by(response_id, row_id, kommun, source_col) %>%
  summarise(
    has_hem_strict = any(hem_strict),
    has_hem_lemma = any(hem_lemma),
    .groups = "drop"
  )

n_responses <- nrow(response_flags)
strict_count <- sum(response_flags$has_hem_strict)
lemma_count <- sum(response_flags$has_hem_lemma)

cat("\n=== Core metrics ===\n")
cat(sprintf("Strict 'hem': %d of %d responses (%.1f%%)\n", strict_count, n_responses, 100 * strict_count / n_responses))
cat(sprintf("Lemma 'hem*': %d of %d responses (%.1f%%)\n", lemma_count, n_responses, 100 * lemma_count / n_responses))

kommun_stats <- response_flags %>%
  group_by(kommun) %>%
  summarise(
    responses = n(),
    hem_strict_count = sum(has_hem_strict),
    hem_strict_share = hem_strict_count / responses,
    hem_lemma_count = sum(has_hem_lemma),
    hem_lemma_share = hem_lemma_count / responses,
    .groups = "drop"
  ) %>%
  arrange(desc(hem_lemma_count), desc(hem_lemma_share))

cat("\nTop 10 municipalities (hem strict):\n")
print(kommun_stats %>% arrange(desc(hem_strict_count), desc(hem_strict_share)) %>% slice_head(n = 10))

cat("\nTop 10 municipalities (hem* lemma-ish):\n")
print(kommun_stats %>% arrange(desc(hem_lemma_count), desc(hem_lemma_share)) %>% slice_head(n = 10))

make_context <- function(tokens_tbl, match_mode = c("strict", "lemma")) {
  mode <- match.arg(match_mode)

  per_response <- tokens_tbl %>%
    group_by(response_id, kommun) %>%
    summarise(tokens = list(token), .groups = "drop")

  row_context <- lapply(seq_len(nrow(per_response)), function(i) {
    tok <- per_response$tokens[[i]]
    if (mode == "strict") {
      has_match <- any(tok == "hem")
      context_words <- unique(tok[tok != "hem"])
    } else {
      has_match <- any(str_detect(tok, "^hem"))
      context_words <- unique(tok[!str_detect(tok, "^hem")])
    }

    if (!has_match || length(context_words) == 0) return(NULL)

    tibble(
      response_id = per_response$response_id[[i]],
      kommun = per_response$kommun[[i]],
      context_word = context_words
    )
  })

  bind_rows(row_context)
}

ctx_strict <- make_context(tokens_kept, "strict")
ctx_lemma <- make_context(tokens_kept, "lemma")

context_top_strict <- ctx_strict %>% count(context_word, sort = TRUE, name = "n")
context_top_lemma <- ctx_lemma %>% count(context_word, sort = TRUE, name = "n")

cat("\nTop 20 context words with strict 'hem':\n")
print(context_top_strict %>% slice_head(n = 20))

cat("\nTop 20 context words with lemma 'hem*':\n")
print(context_top_lemma %>% slice_head(n = 20))

# Build network edges for both strict and lemma variants.
edge_kommun_hem_strict <- response_flags %>%
  filter(has_hem_strict) %>%
  count(kommun, name = "weight") %>%
  transmute(from = kommun, to = "hem", weight, relation = "kommun_to_hem_strict")

edge_kommun_hem_lemma <- response_flags %>%
  filter(has_hem_lemma) %>%
  count(kommun, name = "weight") %>%
  transmute(from = kommun, to = "hem", weight, relation = "kommun_to_hem_lemma")

edge_hem_word_strict <- ctx_strict %>%
  count(context_word, name = "weight") %>%
  transmute(from = "hem", to = context_word, weight, relation = "hem_to_word_strict")

edge_hem_word_lemma <- ctx_lemma %>%
  count(context_word, name = "weight") %>%
  transmute(from = "hem", to = context_word, weight, relation = "hem_to_word_lemma")

edges <- bind_rows(
  edge_kommun_hem_strict,
  edge_kommun_hem_lemma,
  edge_hem_word_strict,
  edge_hem_word_lemma
) %>%
  arrange(desc(weight), relation)

node_names <- sort(unique(c(edges$from, edges$to)))

nodes <- tibble(id = seq_along(node_names) - 1L, label = node_names) %>%
  mutate(
    type = case_when(
      label == "hem" ~ "focus_word",
      label %in% unique(response_flags$kommun) ~ "municipality",
      TRUE ~ "context_word"
    )
  )

node_weight <- edges %>%
  group_by(node = from) %>%
  summarise(w_out = sum(weight), .groups = "drop") %>%
  full_join(
    edges %>% group_by(node = to) %>% summarise(w_in = sum(weight), .groups = "drop"),
    by = "node"
  ) %>%
  mutate(weight = coalesce(w_out, 0) + coalesce(w_in, 0)) %>%
  select(node, weight)

nodes <- nodes %>%
  left_join(node_weight, by = c("label" = "node")) %>%
  mutate(weight = coalesce(weight, 0)) %>%
  arrange(desc(weight), label)

readr::write_csv(nodes, file.path(out_dir, "nodes.csv"))
readr::write_csv(edges, file.path(out_dir, "edges.csv"))
readr::write_csv(word_freq, file.path(out_dir, "word_frequency.csv"))
readr::write_csv(word_freq_excl_hem, file.path(out_dir, "word_frequency_excl_hem.csv"))
readr::write_csv(kommun_stats, file.path(out_dir, "hem_by_kommun.csv"))
readr::write_csv(context_top_strict, file.path(out_dir, "hem_context_strict.csv"))
readr::write_csv(context_top_lemma, file.path(out_dir, "hem_context_lemma.csv"))
readr::write_csv(response_tokens, file.path(out_dir, "response_tokens.csv"))

# Sankey for lemma mode: top-N municipalities and top-M context words.
top_n_kommun <- 10L
top_m_context <- 20L

sankey_edges <- bind_rows(
  edge_kommun_hem_lemma %>% arrange(desc(weight)) %>% slice_head(n = top_n_kommun),
  edge_hem_word_lemma %>% arrange(desc(weight)) %>% slice_head(n = top_m_context)
)

if (requireNamespace("networkD3", quietly = TRUE) && requireNamespace("htmlwidgets", quietly = TRUE)) {
  sankey_nodes <- tibble(name = unique(c(sankey_edges$from, sankey_edges$to)))
  sankey_links <- sankey_edges %>%
    mutate(
      source = match(from, sankey_nodes$name) - 1L,
      target = match(to, sankey_nodes$name) - 1L,
      value = weight
    ) %>%
    select(source, target, value)

  sankey_obj <- networkD3::sankeyNetwork(
    Links = as.data.frame(sankey_links),
    Nodes = as.data.frame(sankey_nodes),
    Source = "source",
    Target = "target",
    Value = "value",
    NodeID = "name",
    fontSize = 12,
    nodeWidth = 30
  )

  tryCatch(
    {
      htmlwidgets::saveWidget(sankey_obj, file.path(out_dir, "hem_sankey.html"), selfcontained = FALSE)
      cat("\nSankey exported:", file.path(out_dir, "hem_sankey.html"), "\n")
    },
    error = function(e) {
      cat("\nSankey export failed:", conditionMessage(e), "\n")
    }
  )
} else {
  cat("\nnetworkD3/htmlwidgets not available. Sankey export skipped.\n")
}

cat("\n=== Export complete ===\n")
cat("nodes:", file.path(out_dir, "nodes.csv"), "\n")
cat("edges:", file.path(out_dir, "edges.csv"), "\n")
cat("word frequencies:", file.path(out_dir, "word_frequency.csv"), "\n")
cat("word frequencies (excluding hem*):", file.path(out_dir, "word_frequency_excl_hem.csv"), "\n")
cat("hem by municipality:", file.path(out_dir, "hem_by_kommun.csv"), "\n")
cat("context strict:", file.path(out_dir, "hem_context_strict.csv"), "\n")
cat("context lemma:", file.path(out_dir, "hem_context_lemma.csv"), "\n")
cat("response tokens:", file.path(out_dir, "response_tokens.csv"), "\n")
