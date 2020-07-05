SELECT COUNT(DISTINCT doc.ID) as count, affil, ARRAY_AGG(DISTINCT FramePurpose) FROM
(WITH authortable as (SELECT Quoted, ID FROM `gcp-cset-projects.rhetorical_frames.paragraph_level_latest`, UNNEST(QuotedNames) as Quoted ),
affiltable as (SELECT authortable.Quoted.QuotedAffiliation as affilval, ID FROM authortable)
SELECT ARRAY_TO_STRING(affilval, " ") as affil, ID FROM affiltable, UNNEST(affilval)) as doc INNER JOIN `gcp-cset-projects.rhetorical_frames.paragraph_level_latest` as par ON doc.ID = par.ID WHERE regexp_contains(FramePurpose, r'[A-Z]') GROUP BY affil ORDER BY count desc