SELECT COUNT(DISTINCT doc.ID) as count, affil FROM
(WITH authortable as (SELECT Mentioned, ID FROM `gcp-cset-projects.rhetorical_frames.paragraph_level_latest`, UNNEST(MentionedNames) as Mentioned ),
affiltable as (SELECT authortable.Mentioned.MentionedAffiliation as affilval, ID FROM authortable)
SELECT ARRAY_TO_STRING(affilval, " ") as affil, ID FROM affiltable, UNNEST(affilval)) as doc INNER JOIN `gcp-cset-projects.rhetorical_frames.paragraph_level_latest` as par ON doc.ID = par.ID WHERE FramePurpose="Explanation" GROUP BY affil ORDER BY count desc

SELECT COUNT(DISTINCT doc.ID) as count, affil FROM
(WITH authortable as (SELECT Mentioned, ID FROM `gcp-cset-projects.rhetorical_frames.paragraph_level_latest`, UNNEST(MentionedNames) as Mentioned ),
affiltable as (SELECT authortable.Mentioned.MentionedAffiliation as affilval, ID FROM authortable)
SELECT ARRAY_TO_STRING(affilval, " ") as affil, ID FROM affiltable, UNNEST(affilval)) as doc INNER JOIN `gcp-cset-projects.rhetorical_frames.paragraph_level_latest` as par ON doc.ID = par.ID WHERE FramePurpose="Motivation" GROUP BY affil ORDER BY count desc