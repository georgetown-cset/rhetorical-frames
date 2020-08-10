# Overview

This project incorporates data from three different sources: 
Defense One, Foreign Affairs, and Reuters. Our analysis includes all unique articles 
containing specific keywords from these three sources from 2012
through 2019; however, Defense One did not begin publishing until July 15, 2013
so our analysis for that source only extends back to its first date of publication.

Articles were included in the analysis from these sources if and only if they included
 one of the following two keywords in the text, in any form:  
`artificial intelligence` or `machine learning`.

Articles were hand analyzed by annotators, who
employed the [annotation guide](Appendix-B.pdf) to identify
when, where, and how articles incorporated rhetorical frames to discuss
artificial intelligence. This annotation effort produced the metadata which was
used for the final analysis.

Because much of this metadata is sourced from proprietary sources (like Factiva),
we have not made it available directly here. However, we have included:

1.) Code to reproduce our process for extracting articles for annotation.

2.) Our [annotation guide](Appendix-B.pdf).

3.) The [queries](sql/) we performed on the metadata once it was loaded into BQ for analysis purposes.

# Data Selection

As noted, data was pulled from Defense One, Reuters, and Foreign Affairs from 2012-2019.

Defense One was scraped via Google Custom Search.

Reuters and Foreign Affairs were both accessed using the Factiva datasource.

We retrieved the Factiva results manually (clicking "RTF" -> "Article Format") and converted them to plain text with `textutil -convert txt ./*.rtf`.

Once data was selected, it was prepared using `data/process.py`.

The annotation task includes paragraph classification, so that code splits the Reuters and Foreign Affairs text into paragraphs.
This isn't necessary in the Defense One text, since its paragraph structure was preserved by the scraper.
The output is JSONL with keys `id`, `title`, `author`, `date`, and `text`.
The `text` value is an array of strings, one per paragraph.

The `id` identifier for the Foreign Affairs and Reuters articles is the Factiva ID.
For Foreign Affairs, it looks like `FRNA000020130302e9310000z`; for Reuters, `LBA0000020180523ee5n00j2h`.
The Defense One `id` values are URLs.

# Annotation

The results of the scraper were provided to annotators, who used the annotation guide
to identify instances of rhetorical frames in the text. 10% of articles were annotated by
multiple annotators, blindly, and these annotations were checked for intercoder agreement.
These annotators produced metadata about each article, which was incorporated
into the final analysis.

# Analysis

After metadata from annotators was loaded into a BQ dataset, data
was analyzed using the SQL queries found in the [sql](sql/) directory.
Results of this analysis can be seen in the various charts in the paper.
Some of these results were normalized post-query; in particular, some
of the affiliations of the quoted and mentioned names had to be 
resolved into actual entities. As this normalization was done by hand, it is
not included here. Please get in touch with us if you have any questions
about replication.
